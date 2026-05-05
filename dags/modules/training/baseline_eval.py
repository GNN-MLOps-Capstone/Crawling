
import json
import os
import pickle
from collections import defaultdict

import numpy as np
import torch
import torch.nn.functional as F

from modules.training.data_loader import load_data_from_s3, preprocess_data
from modules.training.utils import set_seed


RELATION_SPECS = (
    ('keyword', 'stock', 'key2stock'),
    ('stock', 'stock', 'stock2stock'),
    ('keyword', 'keyword', 'key2key'),
)


def _normalize_embeddings(value):
    if isinstance(value, np.ndarray):
        tensor = torch.tensor(value, dtype=torch.float)
    elif isinstance(value, torch.Tensor):
        tensor = value.detach().float().cpu()
    else:
        tensor = torch.tensor(np.asarray(value), dtype=torch.float)
    return F.normalize(tensor, p=2, dim=1)


def _build_ground_truth(edge_index):
    ground_truth = defaultdict(set)
    if edge_index is None or edge_index.numel() == 0:
        return ground_truth
    for src_idx, dst_idx in edge_index.detach().cpu().t().tolist():
        ground_truth[int(src_idx)].add(int(dst_idx))
    return ground_truth


def _build_implicit_relation_edge(source_edge, target_edge, same_type=False):
    news_to_sources = defaultdict(set)
    news_to_targets = defaultdict(set)
    for news_idx, src_idx in source_edge.detach().cpu().t().tolist():
        news_to_sources[int(news_idx)].add(int(src_idx))
    for news_idx, dst_idx in target_edge.detach().cpu().t().tolist():
        news_to_targets[int(news_idx)].add(int(dst_idx))

    pairs = set()
    for news_idx in set(news_to_sources.keys()) & set(news_to_targets.keys()):
        for src_idx in news_to_sources[news_idx]:
            for dst_idx in news_to_targets[news_idx]:
                if same_type and src_idx == dst_idx:
                    continue
                pairs.add((src_idx, dst_idx))

    if not pairs:
        return torch.empty((2, 0), dtype=torch.long)
    ordered = sorted(pairs)
    return torch.tensor([[src for src, _ in ordered], [dst for _, dst in ordered]], dtype=torch.long)


def _sample_negative_targets(ground_truth, num_candidates, *, seed=42, same_type=False):
    rng = np.random.default_rng(seed)
    negatives = []
    for src_idx, positives in ground_truth.items():
        blocked = set(positives)
        if same_type:
            blocked.add(src_idx)
        candidates = [idx for idx in range(num_candidates) if idx not in blocked]
        if not candidates:
            continue
        for _ in positives:
            negatives.append((src_idx, int(rng.choice(candidates))))
    return negatives


def _average_precision_at_k(topk_indices, positive_ids):
    hits = 0
    precision_sum = 0.0
    for rank, idx in enumerate(topk_indices, start=1):
        if idx in positive_ids:
            hits += 1
            precision_sum += hits / rank
    denom = min(len(positive_ids), len(topk_indices))
    return (precision_sum / denom) if denom else 0.0


def _evaluate_embedding_relation(src_emb, dst_emb, relation_edge_index, *, k=10, seed=42, same_type=False):
    src_norm = _normalize_embeddings(src_emb)
    dst_norm = _normalize_embeddings(dst_emb)
    ground_truth = _build_ground_truth(relation_edge_index)
    if not ground_truth:
        return None

    positive_scores = []
    for src_idx, positives in ground_truth.items():
        if src_idx >= src_norm.size(0):
            continue
        for dst_idx in positives:
            if dst_idx < dst_norm.size(0):
                positive_scores.append(float(torch.dot(src_norm[src_idx], dst_norm[dst_idx]).item()))

    negative_pairs = _sample_negative_targets(
        ground_truth,
        int(dst_norm.size(0)),
        seed=seed,
        same_type=same_type,
    )
    negative_scores = [
        float(torch.dot(src_norm[src_idx], dst_norm[dst_idx]).item())
        for src_idx, dst_idx in negative_pairs
        if src_idx < src_norm.size(0) and dst_idx < dst_norm.size(0)
    ]

    query_mrr = []
    query_hit = []
    query_recall = []
    query_map = []
    for src_idx, positives in ground_truth.items():
        if src_idx >= src_norm.size(0):
            continue
        valid_positives = {idx for idx in positives if idx < dst_norm.size(0)}
        if not valid_positives:
            continue

        scores = torch.mv(dst_norm, src_norm[src_idx]).cpu().numpy()
        if same_type and src_idx < scores.shape[0]:
            scores[src_idx] = -np.inf

        positive_values = scores[list(valid_positives)]
        best_rank = int(np.sum(scores > float(np.max(positive_values)))) + 1
        candidate_count = int(dst_norm.size(0) - (1 if same_type else 0))
        topk_size = min(int(k), candidate_count)
        if topk_size <= 0:
            continue
        topk_indices = np.argpartition(-scores, topk_size - 1)[:topk_size]
        topk_indices = topk_indices[np.argsort(-scores[topk_indices])].tolist()
        hits_in_topk = sum(1 for idx in topk_indices if idx in valid_positives)

        query_mrr.append(1.0 / best_rank)
        query_hit.append(1.0 if hits_in_topk > 0 else 0.0)
        query_recall.append(hits_in_topk / len(valid_positives))
        query_map.append(_average_precision_at_k(topk_indices, valid_positives))

    if not positive_scores or not negative_scores or not query_mrr:
        return None

    positive_mean = float(np.mean(positive_scores))
    random_mean = float(np.mean(negative_scores))
    return {
        'positive_sim': positive_mean,
        'random_sim': random_mean,
        'separation': float(positive_mean - random_mean),
        'mrr': float(np.mean(query_mrr)),
        f'hit_at_{k}': float(np.mean(query_hit)),
        f'recall_at_{k}': float(np.mean(query_recall)),
        f'map_at_{k}': float(np.mean(query_map)),
        'query_count': float(len(query_mrr)),
        'positive_pair_count': float(len(positive_scores)),
        'random_pair_count': float(len(negative_scores)),
    }


def _evaluate_embedding_methods(eval_data, embedding_methods, *, k=10, seed=42):
    results = {}
    for method_name, embeddings in embedding_methods.items():
        method_metrics = {}
        for source_type, target_type, relation_name in RELATION_SPECS:
            if source_type not in embeddings or target_type not in embeddings:
                continue
            source_edge = eval_data[('news', f'has_{source_type}', source_type)].relation_edge_index
            target_edge = eval_data[('news', f'has_{target_type}', target_type)].relation_edge_index
            relation_edge_index = _build_implicit_relation_edge(
                source_edge,
                target_edge,
                same_type=(source_type == target_type),
            )
            metrics = _evaluate_embedding_relation(
                embeddings[source_type],
                embeddings[target_type],
                relation_edge_index,
                k=k,
                seed=seed,
                same_type=(source_type == target_type),
            )
            if metrics is None:
                continue
            for metric_name, metric_value in metrics.items():
                method_metrics[f'{relation_name}_{metric_name}'] = metric_value

        separations = [method_metrics.get(f'{rel}_separation') for _, _, rel in RELATION_SPECS]
        hits = [method_metrics.get(f'{rel}_hit_at_{k}') for _, _, rel in RELATION_SPECS]
        mrrs = [method_metrics.get(f'{rel}_mrr') for _, _, rel in RELATION_SPECS]
        method_metrics['avg_separation'] = float(np.mean([v for v in separations if v is not None])) if any(v is not None for v in separations) else 0.0
        method_metrics['avg_hit_at_k'] = float(np.mean([v for v in hits if v is not None])) if any(v is not None for v in hits) else 0.0
        method_metrics['avg_mrr'] = float(np.mean([v for v in mrrs if v is not None])) if any(v is not None for v in mrrs) else 0.0
        results[method_name] = method_metrics
    return results


def _flatten_metrics(results, prefix='holdout'):
    flat = {}
    for method_name, metrics in results.items():
        for metric_name, metric_value in metrics.items():
            flat[f'{method_name}_{prefix}_{metric_name}'] = float(metric_value)
    return flat


def _download_gnn_embeddings(mlflow_client, experiment_name, model_status='product', candidate_version=''):
    exp = mlflow_client.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f'MLflow experiment not found: {experiment_name}')

    filter_parts = ["attributes.status = 'FINISHED'", f"tags.status = '{model_status}'"]
    if candidate_version:
        filter_parts.append(f"tags.candidate_version = '{candidate_version}'")
    runs = mlflow_client.search_runs(
        [exp.experiment_id],
        filter_string=' and '.join(filter_parts),
        order_by=['attributes.start_time DESC'],
        max_results=20,
    )
    if not runs:
        raise ValueError(f'No MLflow run found for status={model_status}, candidate_version={candidate_version or "ANY"}')

    import mlflow
    for run in runs:
        try:
            emb_path = mlflow.artifacts.download_artifacts(run_id=run.info.run_id, artifact_path='node_embeddings.pkl')
            with open(emb_path, 'rb') as handle:
                embeddings = pickle.load(handle)
            return run, embeddings
        except Exception as exc:
            print(f'Skip run {run.info.run_id}: {exc}', flush=True)
    raise ValueError('No selected run has node_embeddings.pkl')


def run_baseline_eval(trainset_path, aws_info, config, output_path=None, model_status='product', candidate_version=''):
    import mlflow
    from mlflow.tracking import MlflowClient

    os.environ['AWS_ACCESS_KEY_ID'] = aws_info['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_info['secret_key']
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = aws_info['endpoint_url']
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    seed = config.get('training', {}).get('seed', 42)
    set_seed(seed)

    raw_data, _, _, _ = load_data_from_s3(trainset_path, aws_info, config=config)
    (_, _, test_data), _ = preprocess_data(
        raw_data,
        val_ratio=config.get('training', {}).get('val_ratio', 0.1),
        test_ratio=config.get('training', {}).get('test_ratio', 0.1),
        config=config,
    )

    mlflow_conf = config.get('mlflow', {})
    experiment_name = mlflow_conf.get('experiment_name', 'News_GNN_v1')
    tracking_uri = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)
    selected_run, gnn_embeddings = _download_gnn_embeddings(
        client,
        experiment_name,
        model_status=model_status,
        candidate_version=candidate_version,
    )

    embedding_methods = {
        'bge': {
            'keyword': raw_data['keyword'].x,
            'stock': raw_data['stock'].x,
        },
        'gnn': {
            'keyword': gnn_embeddings['keyword'],
            'stock': gnn_embeddings['stock'],
        },
    }

    eval_k = config.get('training', {}).get('eval_k', 10)
    results = _evaluate_embedding_methods(test_data, embedding_methods, k=eval_k, seed=seed)
    flat_metrics = _flatten_metrics(results)

    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name='gnn_embedding_baseline_eval'):
        mlflow.log_param('trainset_path', str(trainset_path))
        mlflow.log_param('baseline_eval_k', eval_k)
        mlflow.log_param('baseline_split_mode', config.get('training', {}).get('split_mode'))
        mlflow.log_param('baseline_split_policy', config.get('training', {}).get('split_policy'))
        mlflow.log_param('baseline_gnn_run_id', selected_run.info.run_id)
        mlflow.log_param('baseline_gnn_status', model_status)
        if candidate_version:
            mlflow.log_param('baseline_candidate_version', candidate_version)
        mlflow.set_tag('status', 'baseline')
        mlflow.set_tag('model_type', 'baseline_bge_vs_gnn_embedding')
        mlflow.set_tag('artifact_store_mode', 'mlflow_only')
        if output_path:
            mlflow.set_tag('baseline_output_path', str(output_path))
        mlflow.log_metrics(flat_metrics)

        summary_path = '/tmp/gnn_embedding_baseline_metrics.json'
        with open(summary_path, 'w', encoding='utf-8') as handle:
            json.dump(results, handle, ensure_ascii=False, indent=2, sort_keys=True)
        mlflow.log_artifact(summary_path)
        os.remove(summary_path)

    print(json.dumps(results, ensure_ascii=False, indent=2, sort_keys=True), flush=True)
    return results
