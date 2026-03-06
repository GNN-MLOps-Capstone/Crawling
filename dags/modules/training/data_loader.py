# FILE: modules/training/data_loader.py

import torch
import s3fs
import pickle
import math
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


def preprocess_data(data, val_ratio=0.1, test_ratio=0.1):
    print("✂️ [DataLoader] Splitting Dataset...", flush=True)

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