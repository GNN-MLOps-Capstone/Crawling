# FILE: modules/training/engine.py

import torch
import torch.nn.functional as F
import numpy as np

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
        pos_batch = pos_scores[i : i + batch_size].view(-1, 1)
        compare = (neg_view > pos_batch).long()
        batch_ranks = compare.sum(dim=1) + 1
        ranks_list.append(batch_ranks)
        
    all_ranks = torch.cat(ranks_list)
    mrr = (1.0 / all_ranks).mean().item()
    hit_at_k = (all_ranks <= k).float().mean().item()
    
    return mrr, hit_at_k

# ----------------- New Logic: Implicit Relation Eval ----------------- #

def evaluate_derived_relation(
    source_type,
    target_type,
    out_emb,
    edge_index_dict,
    k=10,
    sample_limit=500,
    seed=None,
):
    src_edge_key = ('news', f'has_{source_type}', source_type)
    tgt_edge_key = ('news', f'has_{target_type}', target_type)
    
    if src_edge_key not in edge_index_dict or tgt_edge_key not in edge_index_dict:
        return None, None

    src_edges = edge_index_dict[src_edge_key] 
    tgt_edges = edge_index_dict[tgt_edge_key]
    
    news_to_src = {}
    for i in range(src_edges.shape[1]):
        n_id = src_edges[0, i].item()
        e_id = src_edges[1, i].item()
        if n_id not in news_to_src: news_to_src[n_id] = []
        news_to_src[n_id].append(e_id)
        
    news_to_tgt = {}
    for i in range(tgt_edges.shape[1]):
        n_id = tgt_edges[0, i].item()
        e_id = tgt_edges[1, i].item()
        if n_id not in news_to_tgt: news_to_tgt[n_id] = []
        news_to_tgt[n_id].append(e_id)
        
    ground_truth = {}
    common_news = set(news_to_src.keys()) & set(news_to_tgt.keys())
    
    for n_id in common_news:
        srcs = news_to_src[n_id]
        tgts = news_to_tgt[n_id]
        
        for s in srcs:
            if s not in ground_truth: ground_truth[s] = {}
            for t in tgts:
                if source_type == target_type and s == t: continue
                ground_truth[s][t] = ground_truth[s].get(t, 0) + 1
                
    if not ground_truth: return 0.0, 0.0

    src_emb_matrix = out_emb[source_type]
    tgt_emb_matrix = out_emb[target_type]
    
    src_emb_norm = F.normalize(src_emb_matrix, p=2, dim=1)
    tgt_emb_norm = F.normalize(tgt_emb_matrix, p=2, dim=1)
    
    total_weight = 0.0
    weighted_mrr_sum = 0.0
    weighted_hit_sum = 0.0
    
    eval_src_ids = list(ground_truth.keys())
    if len(eval_src_ids) > sample_limit:
        if seed is None:
            np.random.shuffle(eval_src_ids)
        else:
            rng = np.random.default_rng(seed)
            rng.shuffle(eval_src_ids)
        eval_src_ids = eval_src_ids[:sample_limit]
        
    for s_id in eval_src_ids:
        tgt_weight_map = ground_truth[s_id]
        if not tgt_weight_map: continue
        
        query = src_emb_norm[s_id].view(1, -1)
        all_scores = torch.mm(query, tgt_emb_norm.t()).view(-1)
        for t_id, w in tgt_weight_map.items():
            p_score = all_scores[t_id]
            rank = (all_scores > p_score).sum().item() + 1
            total_weight += w
            weighted_mrr_sum += w * (1.0 / rank)
            weighted_hit_sum += w * (1.0 if rank <= k else 0.0)

    if total_weight == 0:
        return 0.0, 0.0

    return weighted_mrr_sum / total_weight, weighted_hit_sum / total_weight

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

def train_one_epoch(model, optimizer, data, criterion, target_edge_types, 
                    batch_size=4096, temperature=0.1,
                    loss_mode="infonce", weighting="log1p",
                    min_train_cooccur=1, enable_low_cooccur_filter=False):
    model.train()
    optimizer.zero_grad()
    
    out = model.encoder(data.x_dict, data.edge_index_dict)
    
    total_loss = 0
    num_batches = 0
    total_pos_seen = 0
    total_pos_used = 0
    sum_weights = 0.0
    
    for et in target_edge_types:
        src_type, _, dst_type = et
        
        edge_index = data[et].edge_label_index
        labels = data[et].edge_label
        
        pos_mask = labels == 1
        pos_edges = edge_index[:, pos_mask]

        # Train split positive만으로 dst 노드 연결강도 계산
        dst_connectivity = torch.bincount(
            pos_edges[1],
            minlength=out[dst_type].size(0)
        ).float()
        
        num_edges = pos_edges.size(1)
        if num_edges == 0: continue
            
        perm = torch.randperm(num_edges, device=pos_edges.device)
        
        for i in range(0, num_edges, batch_size):
            idx = perm[i : i + batch_size]
            batch_edges = pos_edges[:, idx]
            batch_strength = dst_connectivity[batch_edges[1]]
            total_pos_seen += int(batch_edges.size(1))

            if enable_low_cooccur_filter:
                keep_mask = batch_strength >= float(min_train_cooccur)
                batch_edges = batch_edges[:, keep_mask]
                batch_strength = batch_strength[keep_mask]
            
            if batch_edges.size(1) < 2: continue 
            total_pos_used += int(batch_edges.size(1))
            
            src_emb = out[src_type][batch_edges[0]]
            dst_emb = out[dst_type][batch_edges[1]]
            
            if loss_mode == "weighted_infonce":
                per_sample_loss = compute_infonce_loss_batch(
                    src_emb,
                    dst_emb,
                    temperature=temperature,
                    reduction="none"
                )
                weights = _compute_sample_weights(batch_strength, mode=weighting).to(per_sample_loss.device)
                denom = weights.sum()
                if denom.item() <= 0:
                    continue
                loss = (per_sample_loss * weights).sum() / denom
                sum_weights += float(denom.item())
            else:
                loss = compute_infonce_loss_batch(src_emb, dst_emb, temperature=temperature)
                sum_weights += float(batch_edges.size(1))
            
            total_loss += loss
            num_batches += 1

    if num_batches > 0:
        final_loss = total_loss / num_batches
        final_loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        train_stats = {
            "train_avg_weight": (sum_weights / total_pos_used) if total_pos_used > 0 else 0.0,
            "train_positive_seen": float(total_pos_seen),
            "train_positive_used": float(total_pos_used),
            "train_positive_used_ratio": (total_pos_used / total_pos_seen) if total_pos_seen > 0 else 0.0,
        }
        return final_loss.item(), train_stats
    else:
        return 0.0, {
            "train_avg_weight": 0.0,
            "train_positive_seen": 0.0,
            "train_positive_used": 0.0,
            "train_positive_used_ratio": 0.0,
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
):
    model.eval()
    out = model.encoder(data.x_dict, data.edge_index_dict)
    
    metrics = {}
    
    # Explicit Relation
    edge_label_index_dict = {et: data[et].edge_label_index for et in target_edge_types}
    preds = model(data.x_dict, data.edge_index_dict, edge_label_index_dict)
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

        seed_base = implicit_seed if implicit_seed is not None else None
        ks_seed = None if seed_base is None else seed_base + 1
        ss_seed = None if seed_base is None else seed_base + 2
        kk_seed = None if seed_base is None else seed_base + 3

        # [변경] k 값을 인자로 전달
        ks_mrr, ks_hit = evaluate_derived_relation(
            'keyword',
            'stock',
            out,
            data.edge_index_dict,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=ks_seed,
        )
        if ks_mrr is not None:
            metrics[f"{split_prefix}_key2stock_mrr"] = ks_mrr
            metrics[f"{split_prefix}_key2stock_hit_at_{k}"] = ks_hit
            
        ss_mrr, ss_hit = evaluate_derived_relation(
            'stock',
            'stock',
            out,
            data.edge_index_dict,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=ss_seed,
        )
        if ss_mrr is not None:
            metrics[f"{split_prefix}_stock2stock_mrr"] = ss_mrr
            metrics[f"{split_prefix}_stock2stock_hit_at_{k}"] = ss_hit
            
        kk_mrr, kk_hit = evaluate_derived_relation(
            'keyword',
            'keyword',
            out,
            data.edge_index_dict,
            k=k,
            sample_limit=implicit_sample_limit,
            seed=kk_seed,
        )
        if kk_mrr is not None:
            metrics[f"{split_prefix}_key2key_mrr"] = kk_mrr
            metrics[f"{split_prefix}_key2key_hit_at_{k}"] = kk_hit

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

            cat = 'key2stock' if stype=='keyword' and ttype=='stock' else \
                  'key2key' if stype=='keyword' and ttype=='keyword' else \
                  'stock2stock' if stype=='stock' and ttype=='stock' else None
            
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
