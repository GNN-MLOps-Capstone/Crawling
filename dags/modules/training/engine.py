# FILE: modules/training/engine.py

import torch
import torch.nn.functional as F
import numpy as np
from collections import defaultdict


# ----------------- Helper Functions ----------------- #

def compute_diversity_metric(embedding_matrix, sample_size=1000):
    num_nodes, dim = embedding_matrix.size()
    if num_nodes < 2: return 0.0, 0.0

    k = min(num_nodes, sample_size)
    indices = torch.randperm(num_nodes)[:k]
    samples = F.normalize(embedding_matrix[indices], p=2, dim=1)

    cos_sim = torch.mm(samples, samples.t())
    mask = ~torch.eye(k, dtype=torch.bool, device=embedding_matrix.device)

    avg_sim = cos_sim[mask].mean().item()
    avg_var = samples.var(dim=0).mean().item()
    return avg_sim, avg_var


def compute_ranking_metrics(pos_scores, neg_scores, k=10, batch_size=1000):
    if pos_scores.numel() == 0 or neg_scores.numel() == 0:
        return 0.0, 0.0

    num_pos = pos_scores.size(0)
    ranks_list = []
    neg_view = neg_scores.view(1, -1)

    for i in range(0, num_pos, batch_size):
        pos_batch = pos_scores[i: i + batch_size].view(-1, 1)
        compare = (neg_view > pos_batch).long()
        batch_ranks = compare.sum(dim=1) + 1
        ranks_list.append(batch_ranks)

    all_ranks = torch.cat(ranks_list)
    mrr = (1.0 / all_ranks).mean().item()
    hit_at_k = (all_ranks <= k).float().mean().item()

    return mrr, hit_at_k


# ----------------- New Logic: Implicit Relation Eval ----------------- #

def _build_news_to_entity_map(edge_index):
    news_to_entity = defaultdict(set)
    if edge_index is None or edge_index.numel() == 0:
        return news_to_entity

    src = edge_index[0].detach().cpu().tolist()
    dst = edge_index[1].detach().cpu().tolist()
    for news_id, entity_id in zip(src, dst):
        news_to_entity[int(news_id)].add(int(entity_id))
    return news_to_entity


def build_relation_ground_truth(source_type, target_type, edge_index_dict):
    src_edge_key = ('news', f'has_{source_type}', source_type)
    tgt_edge_key = ('news', f'has_{target_type}', target_type)

    if src_edge_key not in edge_index_dict or tgt_edge_key not in edge_index_dict:
        return {}

    news_to_src = _build_news_to_entity_map(edge_index_dict[src_edge_key])
    news_to_tgt = _build_news_to_entity_map(edge_index_dict[tgt_edge_key])

    ground_truth = {}
    common_news = set(news_to_src.keys()) & set(news_to_tgt.keys())
    for news_id in common_news:
        srcs = news_to_src[news_id]
        tgts = news_to_tgt[news_id]
        for src_id in srcs:
            ground_truth.setdefault(src_id, {})
            for tgt_id in tgts:
                if source_type == target_type and src_id == tgt_id:
                    continue
                ground_truth[src_id][tgt_id] = ground_truth[src_id].get(tgt_id, 0) + 1

    return {src_id: tgt_map for src_id, tgt_map in ground_truth.items() if tgt_map}


def _average_precision_at_k(topk_indices, positive_ids):
    if not positive_ids:
        return 0.0

    hits = 0
    precision_sum = 0.0
    for rank, idx in enumerate(topk_indices, start=1):
        if idx in positive_ids:
            hits += 1
            precision_sum += hits / rank

    denom = min(len(positive_ids), len(topk_indices))
    return (precision_sum / denom) if denom > 0 else 0.0


def evaluate_relation_retrieval(
        source_type,
        target_type,
        out_emb,
        relation_edge_index_dict,
        k=10,
        sample_limit=500,
        seed=None,
):
    ground_truth = build_relation_ground_truth(source_type, target_type, relation_edge_index_dict)
    if not ground_truth:
        return None

    src_emb_norm = F.normalize(out_emb[source_type], p=2, dim=1)
    tgt_emb_norm = F.normalize(out_emb[target_type], p=2, dim=1)

    eval_src_ids = list(ground_truth.keys())
    if len(eval_src_ids) > sample_limit:
        if seed is None:
            np.random.shuffle(eval_src_ids)
        else:
            rng = np.random.default_rng(seed)
            rng.shuffle(eval_src_ids)
        eval_src_ids = eval_src_ids[:sample_limit]

    query_mrr = []
    query_hit = []
    query_recall = []
    query_map = []
    positive_counts = []

    for src_id in eval_src_ids:
        positive_ids = set(ground_truth[src_id].keys())
        if not positive_ids:
            continue

        all_scores = torch.mv(tgt_emb_norm, src_emb_norm[src_id])
        if source_type == target_type and src_id < all_scores.numel():
            all_scores[src_id] = float('-inf')

        positive_tensor = torch.tensor(sorted(positive_ids), device=all_scores.device)
        best_positive_score = all_scores[positive_tensor].max()
        best_rank = int((all_scores > best_positive_score).sum().item()) + 1

        topk = min(k, int(all_scores.numel()) - (1 if source_type == target_type else 0))
        if topk <= 0:
            continue
        topk_indices = torch.topk(all_scores, k=topk).indices.detach().cpu().tolist()
        hits_in_topk = sum(1 for idx in topk_indices if idx in positive_ids)

        query_mrr.append(1.0 / best_rank)
        query_hit.append(1.0 if hits_in_topk > 0 else 0.0)
        query_recall.append(hits_in_topk / len(positive_ids))
        query_map.append(_average_precision_at_k(topk_indices, positive_ids))
        positive_counts.append(len(positive_ids))

    if not query_mrr:
        return None

    candidate_count = int(tgt_emb_norm.size(0) - (1 if source_type == target_type else 0))
    return {
        'mrr': float(np.mean(query_mrr)),
        f'hit_at_{k}': float(np.mean(query_hit)),
        f'recall_at_{k}': float(np.mean(query_recall)),
        f'map_at_{k}': float(np.mean(query_map)),
        'query_count': float(len(query_mrr)),
        'candidate_count': float(candidate_count),
        'avg_positive_count': float(np.mean(positive_counts)) if positive_counts else 0.0,
    }


# ----------------- Core Functions ----------------- #

def compute_infonce_loss_batch(anchor, positive, temperature=0.07, reduction="mean"):
    # 1. Normalize
    anchor = F.normalize(anchor, p=2, dim=1)
    positive = F.normalize(positive, p=2, dim=1)

    # 2. Similarity Matrix
    logits = torch.mm(anchor, positive.t()) / temperature

    # 3. Labels
    labels = torch.arange(logits.size(0), device=anchor.device)

    # 4. Loss (sample-wise or reduced)
    losses = F.cross_entropy(logits, labels, reduction='none')
    if reduction == "none":
        return losses
    if reduction == "sum":
        return losses.sum()
    return losses.mean()


def _compute_sample_weights(strength, mode="log1p"):
    """Map per-edge connectivity strength to positive weights."""
    strength = strength.float().clamp(min=0.0)
    if mode == "sqrt":
        return torch.sqrt(strength)
    if mode == "linear":
        return strength
    if mode == "binary":
        return (strength > 1).float()
    # default: log1p
    return torch.log1p(strength)


RELATION_TASK_SPECS = (
    ('keyword', 'stock', 'key2stock'),
    ('stock', 'stock', 'stock2stock'),
    ('keyword', 'keyword', 'key2key'),
)


def _compute_pair_loss(src_emb, dst_emb, strength, temperature, loss_mode, weighting):
    if src_emb.size(0) < 2:
        return None, 0.0

    if loss_mode == "weighted_infonce":
        per_sample_loss = compute_infonce_loss_batch(
            src_emb,
            dst_emb,
            temperature=temperature,
            reduction="none"
        )
        weights = _compute_sample_weights(strength, mode=weighting).to(per_sample_loss.device)
        denom = weights.sum()
        if denom.item() <= 0:
            return None, 0.0
        return (per_sample_loss * weights).sum() / denom, float(denom.item())

    return compute_infonce_loss_batch(src_emb, dst_emb, temperature=temperature), float(src_emb.size(0))


def _accumulate_relation_loss(
        out,
        data,
        relation_batch_size,
        temperature,
        loss_mode,
        weighting,
):
    total_loss = None
    num_batches = 0
    total_pos_seen = 0
    total_pos_used = 0
    sum_weights = 0.0

    for source_type, target_type, relation_name in RELATION_TASK_SPECS:
        relation_edges = getattr(data[source_type], f'{relation_name}_index', None)
        relation_strength = getattr(data[source_type], f'{relation_name}_strength', None)
        if relation_edges is None or relation_strength is None or relation_edges.numel() == 0:
            continue

        relation_edges = relation_edges.to(out[source_type].device)
        relation_strength = relation_strength.to(out[source_type].device)
        num_edges = relation_edges.size(1)
        if num_edges == 0:
            continue

        perm = torch.randperm(num_edges, device=relation_edges.device)
        for i in range(0, num_edges, relation_batch_size):
            idx = perm[i: i + relation_batch_size]
            batch_edges = relation_edges[:, idx]
            batch_strength = relation_strength[idx]
            total_pos_seen += int(batch_edges.size(1))
            if batch_edges.size(1) < 2:
                continue

            src_emb = out[source_type][batch_edges[0]]
            dst_emb = out[target_type][batch_edges[1]]
            loss, weight_total = _compute_pair_loss(
                src_emb,
                dst_emb,
                batch_strength,
                temperature,
                loss_mode,
                weighting,
            )
            if loss is None:
                continue

            total_pos_used += int(batch_edges.size(1))
            total_loss = loss if total_loss is None else total_loss + loss
            num_batches += 1
            sum_weights += weight_total

    return total_loss, num_batches, total_pos_seen, total_pos_used, sum_weights


def train_one_epoch(model, optimizer, data, criterion, target_edge_types,
                    batch_size=4096, temperature=0.1,
                    loss_mode="infonce", weighting="log1p",
                    min_train_cooccur=1, enable_low_cooccur_filter=False,
                    use_relation_loss=False, relation_batch_size=None,
                    lambda_news_loss=1.0, lambda_relation_loss=1.0,
                    relation_data_store=None, relation_loss_types=None):
    model.train()
    optimizer.zero_grad()

    out = model.encoder(data.x_dict, data.edge_index_dict)

    news_total_loss = None
    news_num_batches = 0
    total_pos_seen = 0
    total_pos_used = 0
    sum_weights = 0.0

    for et in target_edge_types:
        src_type, _, dst_type = et

        edge_index = data[et].edge_label_index
        labels = data[et].edge_label

        pos_mask = labels == 1
        pos_edges = edge_index[:, pos_mask]

        dst_connectivity = torch.bincount(
            pos_edges[1],
            minlength=out[dst_type].size(0)
        ).float()

        num_edges = pos_edges.size(1)
        if num_edges == 0:
            continue

        perm = torch.randperm(num_edges, device=pos_edges.device)

        for i in range(0, num_edges, batch_size):
            idx = perm[i: i + batch_size]
            batch_edges = pos_edges[:, idx]
            batch_strength = dst_connectivity[batch_edges[1]]
            total_pos_seen += int(batch_edges.size(1))

            if enable_low_cooccur_filter:
                keep_mask = batch_strength >= float(min_train_cooccur)
                batch_edges = batch_edges[:, keep_mask]
                batch_strength = batch_strength[keep_mask]

            if batch_edges.size(1) < 2:
                continue

            src_emb = out[src_type][batch_edges[0]]
            dst_emb = out[dst_type][batch_edges[1]]
            loss, weight_total = _compute_pair_loss(
                src_emb,
                dst_emb,
                batch_strength,
                temperature,
                loss_mode,
                weighting,
            )
            if loss is None:
                continue

            total_pos_used += int(batch_edges.size(1))
            news_total_loss = loss if news_total_loss is None else news_total_loss + loss
            news_num_batches += 1
            sum_weights += weight_total

    relation_batch_size = relation_batch_size or batch_size
    relation_total_loss = None
    relation_num_batches = 0
    relation_pos_seen = 0
    relation_pos_used = 0
    relation_weight_sum = 0.0

    if use_relation_loss and relation_data_store:
        if relation_loss_types:
            allowed_relation_names = set(relation_loss_types)
        else:
            allowed_relation_names = {relation_name for _, _, relation_name in RELATION_TASK_SPECS}

        class _RelationStoreAdapter:
            def __init__(self, relation_store):
                self.relation_store = relation_store

            def __getitem__(self, node_type):
                class _NodeView:
                    def __init__(self, relation_store, node_type):
                        self.relation_store = relation_store
                        self.node_type = node_type

                    def __getattr__(self, attr_name):
                        for source_type, _, relation_name in RELATION_TASK_SPECS:
                            if source_type != self.node_type or relation_name not in self.relation_store:
                                continue
                            if attr_name == f'{relation_name}_index':
                                return self.relation_store[relation_name]['edge_index']
                            if attr_name == f'{relation_name}_strength':
                                return self.relation_store[relation_name]['strength']
                        raise AttributeError(attr_name)

                return _NodeView(self.relation_store, node_type)

        filtered_relation_store = {
            relation_name: relation_data_store[relation_name]
            for relation_name in allowed_relation_names
            if relation_name in relation_data_store
        }
        relation_total_loss, relation_num_batches, relation_pos_seen, relation_pos_used, relation_weight_sum = _accumulate_relation_loss(
            out,
            _RelationStoreAdapter(filtered_relation_store),
            relation_batch_size,
            temperature,
            loss_mode,
            weighting,
        )

    loss_terms = []
    news_loss_value = 0.0
    relation_loss_value = 0.0

    if news_total_loss is not None and news_num_batches > 0:
        news_loss = news_total_loss / news_num_batches
        news_loss_value = float(news_loss.item())
        loss_terms.append(float(lambda_news_loss) * news_loss)

    if relation_total_loss is not None and relation_num_batches > 0:
        relation_loss = relation_total_loss / relation_num_batches
        relation_loss_value = float(relation_loss.item())
        loss_terms.append(float(lambda_relation_loss) * relation_loss)

    if loss_terms:
        final_loss = loss_terms[0]
        for term in loss_terms[1:]:
            final_loss = final_loss + term
        final_loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        train_stats = {
            "train_avg_weight": (sum_weights / total_pos_used) if total_pos_used > 0 else 0.0,
            "train_positive_seen": float(total_pos_seen),
            "train_positive_used": float(total_pos_used),
            "train_positive_used_ratio": (total_pos_used / total_pos_seen) if total_pos_seen > 0 else 0.0,
            "train_news_loss": news_loss_value,
            "train_relation_loss": relation_loss_value,
            "train_total_loss": float(final_loss.item()),
            "train_relation_avg_weight": (relation_weight_sum / relation_pos_used) if relation_pos_used > 0 else 0.0,
            "train_relation_positive_seen": float(relation_pos_seen),
            "train_relation_positive_used": float(relation_pos_used),
            "train_relation_positive_used_ratio": (
                        relation_pos_used / relation_pos_seen) if relation_pos_seen > 0 else 0.0,
        }
        return float(final_loss.item()), train_stats

    return 0.0, {
        "train_avg_weight": 0.0,
        "train_positive_seen": 0.0,
        "train_positive_used": 0.0,
        "train_positive_used_ratio": 0.0,
        "train_news_loss": 0.0,
        "train_relation_loss": 0.0,
        "train_total_loss": 0.0,
        "train_relation_avg_weight": 0.0,
        "train_relation_positive_seen": 0.0,
        "train_relation_positive_used": 0.0,
        "train_relation_positive_used_ratio": 0.0,
    }


@torch.no_grad()
def evaluate(
        model,
        data,
        target_edge_types,
        k=10,
        eval_implicit=False,
        split_prefix="val",
        implicit_seed=None,
        implicit_sample_limit=500,
        compute_final_score=False,
        relation_ground_truth_edge_index_dict=None,
        message_passing_edge_index_dict=None,
):
    model.eval()
    edge_index_dict = message_passing_edge_index_dict or data.edge_index_dict
    out = model.encoder(data.x_dict, edge_index_dict)

    metrics = {}

    # Explicit Relation
    edge_label_index_dict = {et: data[et].edge_label_index for et in target_edge_types}
    preds = model.decode(out, edge_label_index_dict)
    labels = {et: data[et].edge_label for et in target_edge_types}

    total_mrr = []
    total_hit = []

    for et in target_edge_types:
        if et in preds:
            pred_score = preds[et]
            label = labels[et]

            pos_mask = label == 1
            neg_mask = label == 0
            pos_scores = pred_score[pos_mask].sigmoid()
            neg_scores = pred_score[neg_mask].sigmoid()

            mrr, hit = compute_ranking_metrics(pos_scores, neg_scores, k=k)

            edge_name = f"{et[0]}_{et[2]}"
            metrics[f"{split_prefix}_{edge_name}_mrr"] = mrr
            metrics[f"{split_prefix}_{edge_name}_hit_at_{k}"] = hit

            if pos_scores.numel() > 0:
                total_mrr.append(mrr)
                total_hit.append(hit)

    metrics[f"{split_prefix}_mrr"] = np.mean(total_mrr) if total_mrr else 0.0
    metrics[f"{split_prefix}_hit_at_{k}"] = np.mean(total_hit) if total_hit else 0.0

    # Implicit Relation
    if eval_implicit:
        print("⏳ Calculating Implicit Relations...", flush=True)
        relation_edges = relation_ground_truth_edge_index_dict or {
            et: getattr(data[et], 'relation_edge_index', data[et].edge_index)
            for et in target_edge_types
        }

        seed_base = implicit_seed if implicit_seed is not None else None
        ks_seed = None if seed_base is None else seed_base + 1
        ss_seed = None if seed_base is None else seed_base + 2
        kk_seed = None if seed_base is None else seed_base + 3

        ks_metrics = evaluate_relation_retrieval(
            'keyword',
            'stock',
            out,
            relation_edges,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=ks_seed,
        )
        if ks_metrics is not None:
            for metric_name, metric_value in ks_metrics.items():
                metrics[f"{split_prefix}_key2stock_{metric_name}"] = metric_value

        ss_metrics = evaluate_relation_retrieval(
            'stock',
            'stock',
            out,
            relation_edges,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=ss_seed,
        )
        if ss_metrics is not None:
            for metric_name, metric_value in ss_metrics.items():
                metrics[f"{split_prefix}_stock2stock_{metric_name}"] = metric_value

        kk_metrics = evaluate_relation_retrieval(
            'keyword',
            'keyword',
            out,
            relation_edges,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=kk_seed,
        )
        if kk_metrics is not None:
            for metric_name, metric_value in kk_metrics.items():
                metrics[f"{split_prefix}_key2key_{metric_name}"] = metric_value

    # Diversity
    sim_list = []
    var_list = []
    for nt in ['stock', 'keyword', 'news']:
        if nt in out:
            sim, var = compute_diversity_metric(out[nt])
            metrics[f"{split_prefix}_diversity_{nt}_sim"] = sim
            metrics[f"{split_prefix}_diversity_{nt}_var"] = var
            sim_list.append(sim)
            var_list.append(var)

    if sim_list:
        metrics[f"{split_prefix}_diversity_score"] = 1.0 - np.mean(sim_list)
        metrics[f"{split_prefix}_avg_variance"] = np.mean(var_list)

    # Final score (stock-stock, keyword-stock, keyword-keyword)
    if compute_final_score:
        rel_mrr = [
            metrics.get(f"{split_prefix}_key2stock_mrr"),
            metrics.get(f"{split_prefix}_key2key_mrr"),
            metrics.get(f"{split_prefix}_stock2stock_mrr"),
        ]
        rel_hit = [
            metrics.get(f"{split_prefix}_key2stock_hit_at_{k}"),
            metrics.get(f"{split_prefix}_key2key_hit_at_{k}"),
            metrics.get(f"{split_prefix}_stock2stock_hit_at_{k}"),
        ]
        if all(v is not None for v in rel_mrr) and all(v is not None for v in rel_hit):
            metrics[f"{split_prefix}_final_score"] = float(
                0.5 * (float(np.mean(rel_mrr)) + float(np.mean(rel_hit)))
            )
            metrics[f"{split_prefix}_final_score_missing"] = 0.0
        else:
            metrics[f"{split_prefix}_final_score"] = 0.0
            metrics[f"{split_prefix}_final_score_missing"] = 1.0

    return metrics


@torch.no_grad()
def evaluate_golden_set(model, full_data, golden_cases, name_to_idx_map, device, k=10):
    """Golden Set 정성 평가 + Diversity 통합 지표"""
    if not name_to_idx_map or not golden_cases: return {}

    model.eval()
    full_data = full_data.to(device)

    try:
        out = model.encoder(full_data.x_dict, full_data.edge_index_dict)
        results = {}

        # -------------------------------------------------------
        # 1. Diversity Check (개선됨)
        # -------------------------------------------------------
        sim_list = []
        var_list = []

        # 뉴스(news)도 포함하여 전반적인 임베딩 품질 확인
        for nt in ['stock', 'keyword', 'news']:
            if nt in out:
                sim, var = compute_diversity_metric(out[nt])

                # 개별 지표 유지
                results[f"gold_diversity_{nt}_sim"] = sim
                results[f"gold_diversity_{nt}_var"] = var

                sim_list.append(sim)
                var_list.append(var)

        # [NEW] 통합 Diversity Score (높을수록 좋음: 0 ~ 1)
        # Similarity가 낮을수록(0에 가까울수록) 다양하므로 (1 - Sim)
        if sim_list:
            results['gold_diversity_score'] = 1.0 - np.mean(sim_list)
            results['gold_avg_variance'] = np.mean(var_list)
        else:
            results['gold_diversity_score'] = 0.0

        # -------------------------------------------------------
        # 2. Golden Case Evaluation (기존 유지)
        # -------------------------------------------------------
        score_storage = {
            'all': {'mrr': [], 'hit': []},
            'key2stock': {'mrr': [], 'hit': []},
            'key2key': {'mrr': [], 'hit': []},
            'stock2stock': {'mrr': [], 'hit': []}
        }

        edge_indices = {}
        for et in [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]:
            if et in full_data.edge_index_dict:
                edge_indices[et[2]] = full_data.edge_index_dict[et]

        for case in golden_cases:
            src, tgt = case.get('source_name'), case.get('target_name')
            stype, ttype = case.get('source_type'), case.get('target_type')

            sid = name_to_idx_map.get(stype, {}).get(src)
            tid = name_to_idx_map.get(ttype, {}).get(tgt)

            if sid is None or tid is None or stype not in edge_indices: continue

            e_idx = edge_indices[stype]
            news_indices = e_idx[0][e_idx[1] == sid].unique()

            current_mrr = 0.0
            current_hit = 0.0

            if len(news_indices) > 0:
                tgt_embs = out[ttype]

                decoder_key = f'has_{ttype}'
                if decoder_key in model.decoders:
                    decoder = model.decoders[decoder_key]

                    news_embs = out['news'][news_indices]
                    ranks = []

                    for i in range(len(news_embs)):
                        n_emb_expanded = news_embs[i].unsqueeze(0).expand(tgt_embs.size(0), -1)
                        pred_scores = decoder(n_emb_expanded, tgt_embs).view(-1)

                        my_score = pred_scores[tid]
                        rank = (pred_scores > my_score).sum().item() + 1
                        ranks.append(rank)

                    ranks = np.array(ranks)
                    current_mrr = (1.0 / ranks).mean()
                    current_hit = (ranks <= k).mean()

            # 점수 집계
            score_storage['all']['mrr'].append(current_mrr)
            score_storage['all']['hit'].append(current_hit)

            cat = 'key2stock' if stype == 'keyword' and ttype == 'stock' else \
                'key2key' if stype == 'keyword' and ttype == 'keyword' else \
                    'stock2stock' if stype == 'stock' and ttype == 'stock' else None

            if cat:
                score_storage[cat]['mrr'].append(current_mrr)
                score_storage[cat]['hit'].append(current_hit)

        # 3. Final Golden Metrics
        for cat, scores in score_storage.items():
            if scores['mrr']:
                results[f"gold_{cat}_mrr"] = np.mean(scores['mrr'])
                results[f"gold_{cat}_hit_at_{k}"] = np.mean(scores['hit'])
                results[f"gold_{cat}_count"] = float(len(scores['mrr']))
            else:
                results[f"gold_{cat}_count"] = 0.0

        # Final golden score (use available categories only)
        gold_mrr = [
            results.get('gold_key2stock_mrr'),
            results.get('gold_key2key_mrr'),
            results.get('gold_stock2stock_mrr')
        ]
        gold_hit = [
            results.get(f'gold_key2stock_hit_at_{k}'),
            results.get(f'gold_key2key_hit_at_{k}'),
            results.get(f'gold_stock2stock_hit_at_{k}')
        ]
        gold_mrr = [v for v in gold_mrr if v is not None]
        gold_hit = [v for v in gold_hit if v is not None]
        if gold_mrr and gold_hit:
            results['gold_final_score'] = float(
                0.5 * (float(np.mean(gold_mrr)) + float(np.mean(gold_hit)))
            )
            results['gold_final_score_missing'] = 0.0
        else:
            results['gold_final_score'] = 0.0
            results['gold_final_score_missing'] = 1.0

        return results

    finally:
        del full_data
        torch.cuda.empty_cache()
