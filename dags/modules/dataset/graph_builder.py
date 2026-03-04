import os
import torch
import pandas as pd
import numpy as np
import s3fs
import pickle
import psycopg2
from torch_geometric.data import HeteroData


def run_graph_building(target_date, paths, aws_info, db_info):  # 🟢 db_info 추가됨
    # 1. S3 & DB 연결 설정
    fs = s3fs.S3FileSystem(
        key=aws_info['access_key'],
        secret=aws_info['secret_key'],
        client_kwargs={'endpoint_url': aws_info['endpoint_url']}
    )
    bucket = 'silver'

    print(f"🔌 Connecting to Database ({db_info['host']})...")
    conn_db = psycopg2.connect(
        host=db_info['host'], port=db_info['port'],
        user=db_info['user'], password=db_info['password'], dbname=db_info['dbname']
    )

    print(f"🚀 [Dataset Builder] Start creating graph. (Cutoff Date: {target_date})")

    # --- [1] Node Data Loading & DB Joining ---

    # 1-1. Keyword Node
    print(f"📥 Loading Keywords from S3: {paths['keyword_snapshot']}")
    with fs.open(f"{bucket}/{paths['keyword_snapshot']}", 'rb') as f:
        df_kw = pd.read_parquet(f, engine='pyarrow')  # [keyword, vector]

    # 🟢 DB에서 정확한 keyword_id 가져오기
    print("📥 Fetching Keyword IDs from DB...")
    df_kw_db = pd.read_sql("SELECT keyword_id, word FROM public.keywords", conn_db)

    # S3 키워드와 DB 키워드 병합 (word 기준)
    # DB에 없는 키워드는 keyword_id가 NaN이 되며, 이는 나중에 필터링됨
    df_kw = pd.merge(df_kw, df_kw_db, left_on='keyword', right_on='word', how='inner')  # 교집합만 사용

    print(f"✅ Matched Keywords: {len(df_kw)} (Filtered from S3 snapshot)")

    # 1-2. Stock Node
    print(f"📥 Loading Stocks from S3: {paths['stock_snapshot']}")
    with fs.open(f"{bucket}/{paths['stock_snapshot']}", 'rb') as f:
        df_st = pd.read_parquet(f, engine='pyarrow')  # [stock_id, summary_embedding]

    # 🟢 DB에서 정확한 stock_name 가져오기
    print("📥 Fetching Stock Names from DB...")
    df_st_db = pd.read_sql("SELECT stock_id, stock_name FROM public.stocks", conn_db)

    # S3 종목과 DB 종목 병합
    df_st = pd.merge(df_st, df_st_db, on='stock_id', how='left')
    # 혹시 DB에 이름이 없으면 ID로 대체
    df_st['stock_name'] = df_st['stock_name'].fillna(df_st['stock_id'])

    # 1-3. News Node
    print(f"📥 Loading Refined News History...")
    df_news = pd.read_parquet(f"s3://{bucket}/refined_news", storage_options=fs.storage_options, engine='pyarrow')

    if 'pub_date' in df_news.columns:
        df_news['pub_date'] = pd.to_datetime(df_news['pub_date'])
        df_news = df_news[df_news['pub_date'] <= pd.Timestamp(target_date)]

    valid_news_ids = set(df_news['news_id'])

    conn_db.close()  # DB 연결 종료

    # --- [2] Edge Data Loading ---
    print(f"📥 Loading Edges...")

    df_edge_kw = pd.read_parquet(f"s3://{bucket}/extracted_keywords", storage_options=fs.storage_options,
                                 engine='pyarrow')
    df_edge_kw = df_edge_kw[df_edge_kw['news_id'].isin(valid_news_ids)]

    df_edge_st = pd.read_parquet(f"s3://{bucket}/extracted_stocks", storage_options=fs.storage_options,
                                 engine='pyarrow')
    df_edge_st = df_edge_st[df_edge_st['news_id'].isin(valid_news_ids)]

    # --- [3] ID Mapping & Metadata Construction ---
    print("🔄 Mapping IDs & Metadata...")

    # (1) News
    df_news = df_news.sort_values('pub_date').reset_index(drop=True)
    df_news['news_idx'] = df_news.index
    news_map = dict(zip(df_news['news_id'], df_news['news_idx']))
    news_meta = {}  # Title 등 필요 시 추가

    # (2) Keyword (이제 keyword_id는 DB의 PK입니다)
    df_kw = df_kw.reset_index(drop=True)
    df_kw['kw_idx'] = df_kw.index

    # Map: keyword_id(DB PK) -> Graph Index
    kw_map = dict(zip(df_kw['keyword_id'], df_kw['kw_idx']))
    # Meta: keyword_id(DB PK) -> Word(Display Name)
    kw_meta = dict(zip(df_kw['keyword_id'], df_kw['word']))

    # Edge 연결용: Text -> ID 매핑
    kw_text_to_id = dict(zip(df_kw['word'], df_kw['keyword_id']))

    # (3) Stock
    df_st = df_st.reset_index(drop=True)
    df_st['stock_idx'] = df_st.index

    # Map: stock_id -> Graph Index
    stock_map = dict(zip(df_st['stock_id'], df_st['stock_idx']))
    # Meta: stock_id -> stock_name (Display Name)
    stock_meta = dict(zip(df_st['stock_id'], df_st['stock_name']))

    # --- [4] Construct HeteroData ---
    print("🏗️ Building HeteroData Object...")
    data = HeteroData()

    # Features
    data['news'].x = torch.tensor(np.vstack(df_news['news_embedding'].values), dtype=torch.float)
    data['keyword'].x = torch.tensor(np.vstack(df_kw['vector'].values), dtype=torch.float)
    data['stock'].x = torch.tensor(np.vstack(df_st['summary_embedding'].values), dtype=torch.float)

    data['news'].num_nodes = len(df_news)
    data['keyword'].num_nodes = len(df_kw)
    data['stock'].num_nodes = len(df_st)

    # Edges Update
    # Keyword Edge
    df_edge_kw['news_idx'] = df_edge_kw['news_id'].map(news_map)
    # text -> db_id -> graph_idx
    df_edge_kw['kw_id'] = df_edge_kw['keyword'].map(kw_text_to_id)
    df_edge_kw['kw_idx'] = df_edge_kw['kw_id'].map(kw_map)

    valid_nk = df_edge_kw.dropna(subset=['news_idx', 'kw_idx'])
    data['news', 'has_keyword', 'keyword'].edge_index = torch.tensor([
        valid_nk['news_idx'].values, valid_nk['kw_idx'].values
    ], dtype=torch.long)

    # Stock Edge
    df_edge_st['news_idx'] = df_edge_st['news_id'].map(news_map)
    df_edge_st['stock_idx'] = df_edge_st['stock_id'].map(stock_map)

    valid_ns = df_edge_st.dropna(subset=['news_idx', 'stock_idx'])
    data['news', 'has_stock', 'stock'].edge_index = torch.tensor([
        valid_ns['news_idx'].values, valid_ns['stock_idx'].values
    ], dtype=torch.long)

    # --- [5] Save & Upload ---
    print(f"💾 Saving Graph & Mappings to {paths['output']}...")

    local_graph_tmp = f"/tmp/hetero_graph_{target_date}.pt"
    torch.save(data, local_graph_tmp)
    remote_graph_path = f"{bucket}/{paths['output']}"
    fs.put(local_graph_tmp, remote_graph_path)

    # 🟢 메타데이터가 포함된 매핑 저장
    full_mapping_data = {
        'news': {'id_to_idx': news_map, 'meta': news_meta},
        'keyword': {'id_to_idx': kw_map, 'meta': kw_meta},  # keyword_id -> word
        'stock': {'id_to_idx': stock_map, 'meta': stock_meta}  # stock_id -> stock_name
    }

    local_map_tmp = f"/tmp/node_mapping_{target_date}.pkl"
    with open(local_map_tmp, 'wb') as f:
        pickle.dump(full_mapping_data, f)

    map_output_path = paths['output'].replace("hetero_graph.pt", "node_mapping.pkl")
    fs.put(local_map_tmp, f"{bucket}/{map_output_path}")

    if os.path.exists(local_graph_tmp): os.remove(local_graph_tmp)
    if os.path.exists(local_map_tmp): os.remove(local_map_tmp)

    print("✅ Trainset created successfully.")
    return remote_graph_path