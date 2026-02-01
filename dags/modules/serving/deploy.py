import os
import json
import pickle
import s3fs
import numpy as np
import psycopg2
from psycopg2.extras import execute_values


def run_deploy(artifact_info, aws_info, db_info):
    print(f"🚀 [Serving] Deploying Version: {artifact_info['version']}")

    fs = s3fs.S3FileSystem(
        key=aws_info['access_key'],
        secret=aws_info['secret_key'],
        client_kwargs={'endpoint_url': aws_info['endpoint_url']}
    )

    # 1. 데이터 로드
    print(f"📥 Loading Artifacts...")

    emb_path = f"{artifact_info['model_bucket']}/{artifact_info['model_emb_key']}"
    with fs.open(emb_path, 'rb') as f:
        embeddings = pickle.load(f)

    map_path = f"{artifact_info['map_bucket']}/{artifact_info['map_key']}"
    with fs.open(map_path, 'rb') as f:
        full_mappings = pickle.load(f)

    # 2. DB 연결
    print("🔌 Connecting to Database...")
    conn_pg = psycopg2.connect(
        host=db_info['host'], port=db_info['port'],
        user=db_info['user'], password=db_info['password'], dbname=db_info['dbname']
    )
    cursor = conn_pg.cursor()

    insert_query = """
                   INSERT INTO public.test_service_embeddings
                       (entity_id, entity_type, display_name, gnn_embedding, model_version)
                   VALUES %s ON CONFLICT (entity_id, entity_type)
        DO \
                   UPDATE SET
                       model_version = EXCLUDED.model_version, \
                       created_at = CURRENT_TIMESTAMP; \
                   """

    data_to_insert = []
    version = artifact_info['version']

    # (1) News
    if 'news' in embeddings and 'news' in full_mappings:
        vecs = embeddings['news']
        id_map = full_mappings['news']['id_to_idx']
        meta_map = full_mappings['news'].get('meta', {})  # 빈 딕셔너리일 것임

        idx_to_id = {v: k for k, v in id_map.items()}

        for idx, vec in enumerate(vecs):
            if idx in idx_to_id:
                real_id = idx_to_id[idx]
                # 🟢 Title이 없으므로 None (DB에 NULL로 들어감)
                display_name = meta_map.get(real_id, None)

                data_to_insert.append((str(real_id), 'news', display_name, vec.tolist(), version))

    # (2) Keyword
    if 'keyword' in embeddings and 'keyword' in full_mappings:
        vecs = embeddings['keyword']
        id_map = full_mappings['keyword']['id_to_idx']
        meta_map = full_mappings['keyword']['meta']

        idx_to_id = {v: k for k, v in id_map.items()}

        for idx, vec in enumerate(vecs):
            if idx in idx_to_id:
                real_id = idx_to_id[idx]
                display_name = meta_map.get(real_id, str(real_id))

                data_to_insert.append((str(real_id), 'keyword', display_name, vec.tolist(), version))

    # (3) Stock
    if 'stock' in embeddings and 'stock' in full_mappings:
        vecs = embeddings['stock']
        id_map = full_mappings['stock']['id_to_idx']
        meta_map = full_mappings['stock']['meta']

        idx_to_id = {v: k for k, v in id_map.items()}

        for idx, vec in enumerate(vecs):
            if idx in idx_to_id:
                real_id = idx_to_id[idx]
                display_name = meta_map.get(real_id, str(real_id))

                data_to_insert.append((str(real_id), 'stock', display_name, vec.tolist(), version))

    print(f"📦 Inserting {len(data_to_insert)} rows...")

    try:
        execute_values(cursor, insert_query, data_to_insert, page_size=1000)
        conn_pg.commit()
        print("✅ Deployment Successful.")
    except Exception as e:
        conn_pg.rollback()
        print(f"❌ DB Insert Failed: {e}")
        raise e
    finally:
        cursor.close()
        conn_pg.close()