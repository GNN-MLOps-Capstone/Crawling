import os
import pickle
import psycopg2
import mlflow
from datetime import datetime, timezone
from mlflow.tracking import MlflowClient
from psycopg2.extras import execute_values


def run_deploy(artifact_info, aws_info, db_info):
    # MLflow artifact store(MinIO) 접근에 필요한 환경변수 설정
    os.environ['AWS_ACCESS_KEY_ID'] = aws_info['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_info['secret_key']
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = aws_info['endpoint_url']
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    experiment_name = artifact_info["experiment_name"]
    target_status = artifact_info["status"]
    candidate_version = artifact_info.get("candidate_version", "")
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    print(
        f"🔎 [Serving] Resolving model from MLflow experiment={experiment_name}, "
        f"status={target_status}, candidate_version={candidate_version or 'ANY'}"
    )
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"Experiment not found: {experiment_name}")

    filter_parts = [
        "attributes.status = 'FINISHED'",
        f"tags.status = '{target_status}'",
    ]
    if candidate_version:
        filter_parts.append(f"tags.candidate_version = '{candidate_version}'")
    filter_string = " and ".join(filter_parts)

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=filter_string,
        order_by=["attributes.start_time DESC"],
        max_results=20,
    )
    if not runs:
        raise ValueError(f"No FINISHED run found with tags.status={target_status}")

    selected_run = None
    emb_local_path = None
    map_local_path = None
    for run in runs:
        run_id = run.info.run_id
        try:
            emb_local_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="node_embeddings.pkl")
            map_local_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="node_mapping.pkl")
            selected_run = run
            break
        except Exception as exc:
            print(f"⚠️ Skip run {run_id}: missing required artifact(s): {exc}")

    if selected_run is None:
        raise ValueError("No run has both node_embeddings.pkl and node_mapping.pkl artifacts")

    version = selected_run.data.tags.get("candidate_version") or selected_run.info.run_id
    print(f"🚀 [Serving] Deploying run_id={selected_run.info.run_id}, version={version}")

    # 1. 데이터 로드
    print("📥 Loading Artifacts from MLflow...")
    with open(emb_local_path, 'rb') as f:
        embeddings = pickle.load(f)
    with open(map_local_path, 'rb') as f:
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
                       display_name = EXCLUDED.display_name, \
                       gnn_embedding = EXCLUDED.gnn_embedding, \
                       model_version = EXCLUDED.model_version, \
                       created_at = CURRENT_TIMESTAMP; \
                   """

    data_to_insert = []

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
        # Promote deployed run to product after DB commit succeeds.
        deployed_at = datetime.now(timezone.utc).isoformat()
        current_run_id = selected_run.info.run_id
        current_version = version

        current_product_runs = client.search_runs(
            experiment_ids=[exp.experiment_id],
            filter_string="attributes.status = 'FINISHED' and tags.status = 'product'",
            order_by=["attributes.start_time DESC"],
            max_results=100,
        )
        for product_run in current_product_runs:
            if product_run.info.run_id == current_run_id:
                continue
            client.set_tag(product_run.info.run_id, "status", "legacy")
            client.set_tag(product_run.info.run_id, "replaced_by_run_id", current_run_id)
            client.set_tag(product_run.info.run_id, "replaced_by_version", current_version)
            client.set_tag(product_run.info.run_id, "replaced_at_utc", deployed_at)

        client.set_tag(selected_run.info.run_id, "status", "product")
        client.set_tag(selected_run.info.run_id, "deployed_from_status", target_status)
        client.set_tag(selected_run.info.run_id, "deployed_by", "graph_to_db")
        client.set_tag(selected_run.info.run_id, "deployed_at_utc", deployed_at)
        print(
            f"✅ Deployment Successful. run_id={selected_run.info.run_id} promoted to status=product",
            flush=True
        )
    except Exception as e:
        conn_pg.rollback()
        print(f"❌ DB Insert Failed: {e}")
        raise e
    finally:
        cursor.close()
        conn_pg.close()
