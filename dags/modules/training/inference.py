import pickle
from collections import defaultdict

import torch
import torch.nn.functional as F

from modules.training.models import Model


def load_model_checkpoint(checkpoint_path, raw_data, config, device=None):
    if device is None:
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    model = Model(
        hidden_dim=config['model']['hidden_dim'],
        out_dim=config['model']['out_dim'],
        news_feat_dim=raw_data['news'].x.shape[1],
        keyword_feat_dim=raw_data['keyword'].x.shape[1],
        stock_feat_dim=raw_data['stock'].x.shape[1],
        config=config,
    ).to(device)
    state_dict = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(state_dict)
    model.eval()
    return model


@torch.no_grad()
def encode_entities(model, data, edge_index_dict=None, device=None):
    if device is None:
        device = next(model.parameters()).device
    graph = data.to(device)
    out = model.encoder(graph.x_dict, edge_index_dict or graph.edge_index_dict)
    return {key: value.detach().cpu() for key, value in out.items()}


def load_pickle(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def build_news_evidence(news_keywords, news_stocks):
    evidence = defaultdict(lambda: defaultdict(float))
    unique_keywords = sorted(set(news_keywords))
    unique_stocks = sorted(set(news_stocks))

    for keyword_idx in unique_keywords:
        for stock_idx in unique_stocks:
            evidence[('keyword', keyword_idx)][('stock', stock_idx)] += 1.0
            evidence[('stock', stock_idx)][('keyword', keyword_idx)] += 1.0

    for src_idx in unique_keywords:
        for dst_idx in unique_keywords:
            if src_idx == dst_idx:
                continue
            evidence[('keyword', src_idx)][('keyword', dst_idx)] += 1.0

    for src_idx in unique_stocks:
        for dst_idx in unique_stocks:
            if src_idx == dst_idx:
                continue
            evidence[('stock', src_idx)][('stock', dst_idx)] += 1.0

    return evidence


def rerank_with_evidence(source_type, source_idx, target_type, embeddings, evidence=None, alpha=0.2, top_k=10):
    src_emb = F.normalize(torch.as_tensor(embeddings[source_type]), p=2, dim=1)
    tgt_emb = F.normalize(torch.as_tensor(embeddings[target_type]), p=2, dim=1)

    scores = torch.mv(tgt_emb, src_emb[int(source_idx)])
    if source_type == target_type and int(source_idx) < scores.numel():
        scores[int(source_idx)] = float('-inf')

    if evidence:
        for (ev_src_type, ev_src_idx), target_map in evidence.items():
            if ev_src_type != source_type or int(ev_src_idx) != int(source_idx):
                continue
            for (ev_tgt_type, ev_tgt_idx), strength in target_map.items():
                if ev_tgt_type == target_type and int(ev_tgt_idx) < scores.numel():
                    scores[int(ev_tgt_idx)] += alpha * float(strength)

    top_k = min(top_k, int(scores.numel()))
    values, indices = torch.topk(scores, k=top_k)
    return list(zip(indices.tolist(), values.tolist()))
