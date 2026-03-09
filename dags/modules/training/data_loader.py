# FILE: modules/training/data_loader.py

import torch
import s3fs
import pickle
import math
import copy
from datetime import datetime, timezone
import torch_geometric.transforms as T
from torch_geometric.transforms import RandomLinkSplit
from collections import defaultdict

# -----------------------------------------------------------
# 1. 노드 필터링 함수 (키워드/주식용) 
# -----------------------------------------------------------
def filter_low_frequency_nodes(data, node_type, edge_type, rev_edge_type, min_freq=3):
    print(f"🧹 Filtering {node_type} with frequency < {min_freq}...", flush=True)
    
    edge_index = data[edge_type].edge_index
    num_nodes = data[node_type].num_nodes
    
    # Degree 계산 & 마스크 생성
    degrees = torch.bincount(edge_index[1], minlength=num_nodes)
    mask = degrees >= min_freq
    valid_indices = mask.nonzero(as_tuple=False).view(-1)
    
    print(f"   [{node_type}] Before: {num_nodes} -> After: {valid_indices.numel()} (Removed {num_nodes - valid_indices.numel()})")

    # 매핑 테이블 생성 (Old -> New)
    mapping = torch.full((num_nodes,), -1, dtype=torch.long)
    mapping[valid_indices] = torch.arange(valid_indices.numel())

    if valid_indices.numel() == num_nodes:
        return data, mapping 

    # Feature 필터링
    if data[node_type].x is not None:
        data[node_type].x = data[node_type].x[mask]
        
    # Edge 업데이트 (Forward: News -> Target)
    edge_mask = mask[edge_index[1]]
    new_edge_index = edge_index[:, edge_mask]
    new_edge_index[1] = mapping[new_edge_index[1]]
    data[edge_type].edge_index = new_edge_index
    
    # Edge 업데이트 (Backward: Target -> News)
    if rev_edge_type in data.edge_index_dict:
        rev_edge_index = data[rev_edge_type].edge_index
        rev_mask = mask[rev_edge_index[0]]
        new_rev_edge_index = rev_edge_index[:, rev_mask]
        new_rev_edge_index[0] = mapping[new_rev_edge_index[0]]
        data[rev_edge_type].edge_index = new_rev_edge_index

    # 노드 개수 업데이트
    data[node_type].num_nodes = valid_indices.numel()
    
    return data, mapping

# -----------------------------------------------------------
# 2. 고립된 뉴스 노드 삭제 함수
# -----------------------------------------------------------
def remove_isolated_news(data):
    print("🧹 Removing isolated news nodes...", flush=True)
    
    num_news = data['news'].num_nodes
    connected_news_mask = torch.zeros(num_news, dtype=torch.bool)
    
    for edge_type in [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]:
        if edge_type in data.edge_index_dict:
            src, _ = data[edge_type].edge_index
            connected_news_mask[src] = True
            
    valid_news_indices = connected_news_mask.nonzero(as_tuple=False).view(-1)
    
    print(f"   [News] Before: {num_news} -> After: {valid_news_indices.numel()} (Removed {num_news - valid_news_indices.numel()})")
    
    if valid_news_indices.numel() == num_news:
        return data

    news_mapping = torch.full((num_news,), -1, dtype=torch.long)
    news_mapping[valid_news_indices] = torch.arange(valid_news_indices.numel())
    
    if data['news'].x is not None:
        data['news'].x = data['news'].x[connected_news_mask]
        
    for edge_type in list(data.edge_index_dict.keys()):
        src_type, rel, dst_type = edge_type
        edge_index = data[edge_type].edge_index
        
        if src_type == 'news':
            src, dst = edge_index
            valid_mask = news_mapping[src] != -1
            new_src = news_mapping[src[valid_mask]]
            new_dst = dst[valid_mask]
            data[edge_type].edge_index = torch.stack([new_src, new_dst], dim=0)
            
        elif dst_type == 'news':
            src, dst = edge_index
            valid_mask = news_mapping[dst] != -1
            new_src = src[valid_mask]
            new_dst = news_mapping[dst[valid_mask]]
            data[edge_type].edge_index = torch.stack([new_src, new_dst], dim=0)

    data['news'].num_nodes = valid_news_indices.numel()
    
    return data

# -----------------------------------------------------------
# 3. PMI Edge 생성 함수
# -----------------------------------------------------------
def add_pmi_global_edges(data, min_pmi=0.1, min_cooccur=3):
    print(f"🔗 Building Global PMI Edges (Threshold > {min_pmi})...", flush=True)
    
    news_key_edge = data['news', 'has_keyword', 'keyword'].edge_index
    news_stock_edge = data['news', 'has_stock', 'stock'].edge_index
    num_news = data['news'].num_nodes
    
    news_to_keys = defaultdict(list)
    news_to_stocks = defaultdict(list)
    
    key_freq = defaultdict(int)
    stock_freq = defaultdict(int)
    
    for i in range(news_key_edge.size(1)):
        n_id, k_id = news_key_edge[:, i].tolist()
        news_to_keys[n_id].append(k_id)
    
    for i in range(news_stock_edge.size(1)):
        n_id, s_id = news_stock_edge[:, i].tolist()
        news_to_stocks[n_id].append(s_id)
    
    for k_list in news_to_keys.values():
        for k in set(k_list): key_freq[k] += 1
            
    for s_list in news_to_stocks.values():
        for s in set(s_list): stock_freq[s] += 1
        
    cooccur_counts = defaultdict(int)
    common_news = set(news_to_keys.keys()) & set(news_to_stocks.keys())
    
    for n_id in common_news:
        keys = set(news_to_keys[n_id]) 
        stocks = set(news_to_stocks[n_id])
        for k in keys:
            for s in stocks:
                cooccur_counts[(k, s)] += 1
                
    src_list = []
    dst_list = []
    count = 0
    eps = 1e-8
    
    for (k, s), joint_freq in cooccur_counts.items():
        if joint_freq < min_cooccur:
            continue
            
        freq_k = key_freq[k]
        freq_s = stock_freq[s]
        pmi_score = math.log((joint_freq * num_news) / (freq_k * freq_s + eps))
        
        if pmi_score > min_pmi:
            src_list.append(k)
            dst_list.append(s)
            count += 1
            
    print(f"   - Created {count} PMI-based edges (Min Co-occur: {min_cooccur}, Min PMI: {min_pmi})")
    
    if count > 0:
        edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)
        data['keyword', 'globally_related', 'stock'].edge_index = edge_index
        data['stock', 'rev_globally_related', 'keyword'].edge_index = torch.stack([edge_index[1], edge_index[0]])
        
    return data, count

# -----------------------------------------------------------
# 4. 메인 로드 함수 (수정됨)
# -----------------------------------------------------------
def load_data_from_s3(path, aws_info, config=None): # [변경] config 인자 추가
    print(f"📂 [DataLoader] Fetching from: silver/{path}", flush=True)

    # Config에서 파라미터 추출 (기본값 설정)
    if config is None: config = {}
    data_conf = config.get('data_processing', {})
    
    min_freq_keyword = data_conf.get('min_freq_keyword', 7)
    min_freq_stock = data_conf.get('min_freq_stock', 5)
    min_pmi = data_conf.get('min_pmi', 0.1)
    min_cooccur = data_conf.get('min_cooccur', 3)

    fs = s3fs.S3FileSystem(
        key=aws_info['access_key'],
        secret=aws_info['secret_key'],
        client_kwargs={'endpoint_url': aws_info['endpoint_url']}
    )

    with fs.open(f"silver/{path}", 'rb') as f:
        data = torch.load(f, weights_only=False)
    
    # 1. 키워드 & 주식 필터링 (Config 값 사용)
    data, keyword_mapping = filter_low_frequency_nodes(
        data, 'keyword', 
        ('news', 'has_keyword', 'keyword'), 
        ('keyword', 'rev_has_keyword', 'news'), 
        min_freq=min_freq_keyword
    )
    
    data, stock_mapping = filter_low_frequency_nodes(
        data, 'stock', 
        ('news', 'has_stock', 'stock'), 
        ('stock', 'rev_has_stock', 'news'), 
        min_freq=min_freq_stock
    )
    
    # 2. 고립된 뉴스 삭제
    data = remove_isolated_news(data)
    
    # 3. PMI Edge 추가 (Config 값 사용)
    data, _ = add_pmi_global_edges(data, min_pmi=min_pmi, min_cooccur=min_cooccur)
    
    print("🔄 [DataLoader] Applying ToUndirected (Global)...", flush=True)
    data = T.ToUndirected()(data)

    # 4. 매핑 정보 로드
    mapping_path = path.replace("hetero_graph.pt", "node_mapping.pkl")
    name_to_idx_map = {}
    
    try:
        full_map_path = f"silver/{mapping_path}"
        if fs.exists(full_map_path):
            with fs.open(full_map_path, 'rb') as f:
                full_mappings = pickle.load(f)
            
            for entity_type, mapping_tensor in zip(['keyword', 'stock'], [keyword_mapping, stock_mapping]):
                if entity_type in full_mappings:
                    name_to_idx_map[entity_type] = {}
                    original_map = full_mappings[entity_type].get('id_to_idx', {})
                    meta_map = full_mappings[entity_type].get('meta', {})
                    
                    for real_id, old_idx in original_map.items():
                        if old_idx < len(mapping_tensor):
                            new_idx = mapping_tensor[old_idx].item()
                            if new_idx != -1:
                                display_name = meta_map.get(real_id, str(real_id))
                                name_to_idx_map[entity_type][display_name] = new_idx
                    
                    print(f"   - Updated {entity_type} map size: {len(name_to_idx_map[entity_type])}")

        else:
            name_to_idx_map = None
            
    except Exception as e:
        print(f"⚠️ Mapping load failed: {e}")
        name_to_idx_map = None

    return data, name_to_idx_map, fs


def _parse_date_to_unix_eod(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = dt.replace(hour=23, minute=59, second=59, microsecond=0)
    return int(dt.timestamp())


def _resolve_temporal_boundaries(pub_ts, policy, val_ratio, test_ratio, train_end_date=None, val_end_date=None):
    if policy == "date":
        if not train_end_date or not val_end_date:
            raise ValueError("temporal split(date policy) requires train_end_date and val_end_date")
        train_end_ts = _parse_date_to_unix_eod(train_end_date)
        val_end_ts = _parse_date_to_unix_eod(val_end_date)
        if train_end_ts >= val_end_ts:
            raise ValueError("train_end_date must be earlier than val_end_date")
        return train_end_ts, val_end_ts

    if policy != "ratio":
        raise ValueError(f"Unsupported split_policy: {policy}")

    if val_ratio < 0 or test_ratio < 0 or (val_ratio + test_ratio) >= 1:
        raise ValueError("val_ratio/test_ratio are invalid for temporal ratio split")

    sorted_ts = torch.sort(pub_ts).values
    n = sorted_ts.numel()
    if n == 0:
        raise ValueError("news.pub_ts is empty")

    train_end_idx = max(0, min(n - 1, int((1.0 - val_ratio - test_ratio) * n) - 1))
    val_end_idx = max(train_end_idx, min(n - 1, int((1.0 - test_ratio) * n) - 1))
    train_end_ts = int(sorted_ts[train_end_idx].item())
    val_end_ts = int(sorted_ts[val_end_idx].item())
    return train_end_ts, val_end_ts


def _sample_negative_edges(num_src, num_dst, positive_edge_index, num_neg, generator):
    if num_neg <= 0 or num_src <= 0 or num_dst <= 0:
        return torch.empty((2, 0), dtype=torch.long)

    pos_pairs = set((int(s), int(d)) for s, d in positive_edge_index.t().tolist())
    neg_pairs = set()
    max_trials = max(num_neg * 20, 1000)
    trials = 0

    while len(neg_pairs) < num_neg and trials < max_trials:
        s = int(torch.randint(0, num_src, (1,), generator=generator).item())
        d = int(torch.randint(0, num_dst, (1,), generator=generator).item())
        pair = (s, d)
        if pair not in pos_pairs and pair not in neg_pairs:
            neg_pairs.add(pair)
        trials += 1

    if not neg_pairs:
        return torch.empty((2, 0), dtype=torch.long)

    src = [p[0] for p in neg_pairs]
    dst = [p[1] for p in neg_pairs]
    return torch.tensor([src, dst], dtype=torch.long)


def _build_edge_labels(pos_edge_index, full_positive_edge_index, num_src, num_dst, negative_ratio, generator):
    num_pos = int(pos_edge_index.size(1))
    num_neg = int(round(num_pos * float(negative_ratio)))
    neg_edge_index = _sample_negative_edges(
        num_src=num_src,
        num_dst=num_dst,
        positive_edge_index=full_positive_edge_index,
        num_neg=num_neg,
        generator=generator,
    )

    if num_pos == 0 and neg_edge_index.size(1) == 0:
        return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.float)

    edge_label_index = pos_edge_index
    edge_label = torch.ones(num_pos, dtype=torch.float)

    if neg_edge_index.size(1) > 0:
        edge_label_index = torch.cat([edge_label_index, neg_edge_index], dim=1)
        edge_label = torch.cat([edge_label, torch.zeros(neg_edge_index.size(1), dtype=torch.float)], dim=0)

    return edge_label_index, edge_label


def temporal_link_split(
    data,
    val_ratio=0.1,
    test_ratio=0.1,
    split_policy="date",
    train_end_date=None,
    val_end_date=None,
    negative_ratio=1.0,
    seed=42,
    temporal_message_passing="full_graph",
):
    print("✂️ [DataLoader] Temporal Splitting Dataset...", flush=True)

    target_edge_types = [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]
    rev_edge_types = [('keyword', 'rev_has_keyword', 'news'), ('stock', 'rev_has_stock', 'news')]

    if not hasattr(data['news'], 'pub_ts'):
        raise ValueError("Temporal split requires data['news'].pub_ts. Rebuild trainset with updated graph_builder.")

    pub_ts = data['news'].pub_ts.cpu().long()
    train_end_ts, val_end_ts = _resolve_temporal_boundaries(
        pub_ts=pub_ts,
        policy=split_policy,
        val_ratio=val_ratio,
        test_ratio=test_ratio,
        train_end_date=train_end_date,
        val_end_date=val_end_date,
    )
    print(f"   - temporal boundaries: train<= {train_end_ts}, val<= {val_end_ts}, test> {val_end_ts}", flush=True)

    train_data = copy.deepcopy(data)
    val_data = copy.deepcopy(data)
    test_data = copy.deepcopy(data)

    generator = torch.Generator()
    generator.manual_seed(int(seed))

    for et, rev_et in zip(target_edge_types, rev_edge_types):
        src_type, _, dst_type = et
        full_edge_index = data[et].edge_index.cpu()
        src_news_idx = full_edge_index[0]
        edge_ts = pub_ts[src_news_idx]

        train_mask = edge_ts <= train_end_ts
        val_mask = (edge_ts > train_end_ts) & (edge_ts <= val_end_ts)
        test_mask = edge_ts > val_end_ts

        train_pos = full_edge_index[:, train_mask]
        val_pos = full_edge_index[:, val_mask]
        test_pos = full_edge_index[:, test_mask]

        # Train split은 항상 train edge만으로 message passing
        train_data[et].edge_index = train_pos
        train_data[rev_et].edge_index = torch.stack([train_pos[1], train_pos[0]], dim=0)

        # Val/Test message passing 범위는 config로 선택
        if temporal_message_passing == "train_only":
            val_data[et].edge_index = train_pos
            val_data[rev_et].edge_index = torch.stack([train_pos[1], train_pos[0]], dim=0)
            test_data[et].edge_index = train_pos
            test_data[rev_et].edge_index = torch.stack([train_pos[1], train_pos[0]], dim=0)
        elif temporal_message_passing != "full_graph":
            raise ValueError(f"Unsupported temporal_message_passing: {temporal_message_passing}")

        num_src = int(data[src_type].num_nodes)
        num_dst = int(data[dst_type].num_nodes)

        train_edge_label_index, train_edge_label = _build_edge_labels(
            pos_edge_index=train_pos,
            full_positive_edge_index=full_edge_index,
            num_src=num_src,
            num_dst=num_dst,
            negative_ratio=negative_ratio,
            generator=generator,
        )
        val_edge_label_index, val_edge_label = _build_edge_labels(
            pos_edge_index=val_pos,
            full_positive_edge_index=full_edge_index,
            num_src=num_src,
            num_dst=num_dst,
            negative_ratio=negative_ratio,
            generator=generator,
        )
        test_edge_label_index, test_edge_label = _build_edge_labels(
            pos_edge_index=test_pos,
            full_positive_edge_index=full_edge_index,
            num_src=num_src,
            num_dst=num_dst,
            negative_ratio=negative_ratio,
            generator=generator,
        )

        train_data[et].edge_label_index = train_edge_label_index
        train_data[et].edge_label = train_edge_label
        val_data[et].edge_label_index = val_edge_label_index
        val_data[et].edge_label = val_edge_label
        test_data[et].edge_label_index = test_edge_label_index
        test_data[et].edge_label = test_edge_label

        print(
            f"   [{et}] train/val/test positives = "
            f"{train_pos.size(1)}/{val_pos.size(1)}/{test_pos.size(1)}",
            flush=True
        )

    return (train_data, val_data, test_data), target_edge_types


def preprocess_data(data, val_ratio=0.1, test_ratio=0.1, config=None):
    print("✂️ [DataLoader] Splitting Dataset...", flush=True)

    if config is None:
        config = {}
    training_conf = config.get('training', {})

    split_mode = training_conf.get('split_mode', 'random')
    val_ratio = training_conf.get('val_ratio', val_ratio)
    test_ratio = training_conf.get('test_ratio', test_ratio)

    if split_mode == "temporal":
        return temporal_link_split(
            data=data,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            split_policy=training_conf.get('split_policy', 'date'),
            train_end_date=training_conf.get('train_end_date'),
            val_end_date=training_conf.get('val_end_date'),
            negative_ratio=training_conf.get('negative_ratio', 1.0),
            seed=training_conf.get('seed', 42),
            temporal_message_passing=training_conf.get('temporal_message_passing', 'full_graph'),
        )

    target_edge_types = [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]
    rev_edge_types = [('keyword', 'rev_has_keyword', 'news'), ('stock', 'rev_has_stock', 'news')]

    transform = RandomLinkSplit(
        num_val=val_ratio,
        num_test=test_ratio,
        is_undirected=True,
        add_negative_train_samples=True,
        edge_types=target_edge_types,
        rev_edge_types=rev_edge_types,
    )

    return transform(data), target_edge_types
