import os
import json
import mlflow
import random
import numpy as np
import torch
import torch.nn.functional as F

def set_seed(seed=42):
    # 1. 기본 시드 고정
    random.seed(seed)
    np.random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    
    # 2. [중요] CUDNN 결정론적 모드 (속도 저하 없음)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

    # 3. [핵심] PyTorch 결정론적 알고리즘 강제 사용
    # GPU 연산 순서를 강제하여 100% 재현 가능하게 만듦
    # 주의: 일부 연산(Sparse Matrix 등)에서 에러가 날 수 있음 -> 에러 나면 이 줄 주석 처리
    torch.use_deterministic_algorithms(True)

    # 4. [핵심] CUBLAS 설정 (이게 없으면 위 3번에서 에러 남)
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"
    
    print(f"🔒 Random Seed & Deterministic Mode set to: {seed}", flush=True)

def load_config(config_path):
    """Config JSON 파일 로드"""
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def get_module_path(filename):
    """modules/training/ 내의 파일 절대 경로 반환"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, filename)

def load_json_file(filename):
    """모듈 폴더 내 JSON 파일 로드 (예: golden_cases.json)"""
    path = get_module_path(filename)
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    except Exception as e:
        print(f"⚠️ Error loading {filename}: {e}")
        return []

def log_file_artifact(filename):
    """모듈 폴더 내 파일을 MLflow Artifact로 업로드"""
    path = get_module_path(filename)
    if os.path.exists(path):
        print(f"📎 Logging Artifact: {filename}")
        mlflow.log_artifact(path)
    else:
        print(f"⚠️ Artifact not found: {path}")

def build_gold_similarity_report(emb, golden_cases, name_to_idx_map, edge_index_dict=None, top_k=10, min_cooccur=3):
    """
    골든 키워드/종목에 대해 코사인 유사도 기반으로
    상위 K개 결과를 텍스트로 생성.
    - keyword -> stock
    - stock -> keyword
    - keyword -> keyword
    - stock -> stock
    """
    if emb is None or name_to_idx_map is None or not golden_cases:
        return ""

    if 'keyword' not in emb or 'stock' not in emb:
        return ""

    keyword_map = name_to_idx_map.get('keyword', {})
    stock_map = name_to_idx_map.get('stock', {})
    if not keyword_map or not stock_map:
        return ""

    idx_to_keyword = {idx: name for name, idx in keyword_map.items()}
    idx_to_stock = {idx: name for name, idx in stock_map.items()}

    gold_keywords = set()
    gold_stocks = set()
    for case in golden_cases:
        s_type = case.get('source_type')
        t_type = case.get('target_type')
        s_name = case.get('source_name')
        t_name = case.get('target_name')
        if s_type == 'keyword' and s_name: gold_keywords.add(s_name)
        if t_type == 'keyword' and t_name: gold_keywords.add(t_name)
        if s_type == 'stock' and s_name: gold_stocks.add(s_name)
        if t_type == 'stock' and t_name: gold_stocks.add(t_name)

    if not gold_keywords and not gold_stocks:
        return ""

    keyword_emb = F.normalize(emb['keyword'], dim=1)
    stock_emb = F.normalize(emb['stock'], dim=1)

    keyword_to_news = {}
    stock_to_news = {}
    if edge_index_dict is not None:
        key_edge = edge_index_dict.get(('news', 'has_keyword', 'keyword'))
        stock_edge = edge_index_dict.get(('news', 'has_stock', 'stock'))
        if key_edge is not None and stock_edge is not None:
            key_src = key_edge[0].cpu().tolist()
            key_dst = key_edge[1].cpu().tolist()
            for n_id, k_id in zip(key_src, key_dst):
                keyword_to_news.setdefault(k_id, set()).add(n_id)

            stock_src = stock_edge[0].cpu().tolist()
            stock_dst = stock_edge[1].cpu().tolist()
            for n_id, s_id in zip(stock_src, stock_dst):
                stock_to_news.setdefault(s_id, set()).add(n_id)

    def cooccur_count(k_idx, s_idx):
        if not keyword_to_news or not stock_to_news:
            return None
        k_set = keyword_to_news.get(k_idx, set())
        s_set = stock_to_news.get(s_idx, set())
        if not k_set or not s_set:
            return 0
        return len(k_set & s_set)

    lines = []
    lines.append("=== Gold Similarity Report ===")
    lines.append(f"Top-K = {top_k}")
    lines.append(f"Min Co-occur (keyword-stock) = {min_cooccur}")
    lines.append("")

    if gold_keywords:
        lines.append("[Gold Keywords -> Top Stocks]")
        for name in sorted(gold_keywords):
            if name not in keyword_map:
                lines.append(f"- {name}: NOT_FOUND")
                continue
            k_idx = keyword_map[name]
            sims = torch.matmul(keyword_emb[k_idx], stock_emb.T)
            k = min(max(top_k * 5, top_k), sims.numel())
            top_vals, top_idx = torch.topk(sims, k=k)
            items = []
            for v, i in zip(top_vals.tolist(), top_idx.tolist()):
                if keyword_to_news and stock_to_news:
                    cc = cooccur_count(k_idx, i)
                    if cc is None or cc < min_cooccur:
                        continue
                stock_name = idx_to_stock.get(i, str(i))
                items.append(f"{stock_name} ({v:.4f})")
                if len(items) >= top_k:
                    break
            lines.append(f"- {name}: " + ", ".join(items))
        lines.append("")

        lines.append("[Gold Keywords -> Top Keywords]")
        for name in sorted(gold_keywords):
            if name not in keyword_map:
                lines.append(f"- {name}: NOT_FOUND")
                continue
            k_idx = keyword_map[name]
            sims = torch.matmul(keyword_emb[k_idx], keyword_emb.T)
            k = min(top_k + 1, sims.numel())
            top_vals, top_idx = torch.topk(sims, k=k)
            items = []
            for v, i in zip(top_vals.tolist(), top_idx.tolist()):
                if i == k_idx:
                    continue
                keyword_name = idx_to_keyword.get(i, str(i))
                items.append(f"{keyword_name} ({v:.4f})")
                if len(items) >= top_k:
                    break
            lines.append(f"- {name}: " + ", ".join(items))
        lines.append("")

    if gold_stocks:
        lines.append("[Gold Stocks -> Top Keywords]")
        for name in sorted(gold_stocks):
            if name not in stock_map:
                lines.append(f"- {name}: NOT_FOUND")
                continue
            s_idx = stock_map[name]
            sims = torch.matmul(stock_emb[s_idx], keyword_emb.T)
            k = min(max(top_k * 5, top_k), sims.numel())
            top_vals, top_idx = torch.topk(sims, k=k)
            items = []
            for v, i in zip(top_vals.tolist(), top_idx.tolist()):
                if keyword_to_news and stock_to_news:
                    cc = cooccur_count(i, s_idx)
                    if cc is None or cc < min_cooccur:
                        continue
                keyword_name = idx_to_keyword.get(i, str(i))
                items.append(f"{keyword_name} ({v:.4f})")
                if len(items) >= top_k:
                    break
            lines.append(f"- {name}: " + ", ".join(items))
        lines.append("")

        lines.append("[Gold Stocks -> Top Stocks]")
        for name in sorted(gold_stocks):
            if name not in stock_map:
                lines.append(f"- {name}: NOT_FOUND")
                continue
            s_idx = stock_map[name]
            sims = torch.matmul(stock_emb[s_idx], stock_emb.T)
            k = min(top_k + 1, sims.numel())
            top_vals, top_idx = torch.topk(sims, k=k)
            items = []
            for v, i in zip(top_vals.tolist(), top_idx.tolist()):
                if i == s_idx:
                    continue
                stock_name = idx_to_stock.get(i, str(i))
                items.append(f"{stock_name} ({v:.4f})")
                if len(items) >= top_k:
                    break
            lines.append(f"- {name}: " + ", ".join(items))

    return "\n".join(lines)
