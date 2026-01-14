import io
import boto3
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from ollama import Client


class KeywordEmbeddingManager:
    """
    [역할]
    신규 키워드를 식별하고, LLM을 통해 Embedding Vector를 생성하여
    Master Registry(Embedding Dictionary)를 갱신합니다.
    """

    def __init__(self, aws_info: dict, bucket_name: str = 'silver'):
        self.s3 = boto3.client('s3',
                               aws_access_key_id=aws_info['access_key'],
                               aws_secret_access_key=aws_info['secret_key'],
                               endpoint_url=aws_info['endpoint_url'])
        self.bucket = bucket_name
        self.ollama_host = "http://ollama:11434"
        self.model = "bge-m3"

    def _find_latest_registry(self, target_date_str: str) -> str:
        """가장 최근에 생성된 임베딩 마스터 파일(Registry) 경로 탐색"""
        prefix = "keyword_embeddings/date="
        target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")

        paginator = self.s3.get_paginator('list_objects_v2')
        valid_dates = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' not in page: continue
            for p in page['CommonPrefixes']:
                try:
                    d_str = p['Prefix'].split("date=")[1].replace('/', '')
                    d_obj = datetime.strptime(d_str, "%Y%m%d")
                    if d_obj < target_dt:
                        valid_dates.append(d_obj)
                except:
                    continue

        if not valid_dates:
            print("✨ No previous embedding registry found (Cold Start).")
            return None

        latest_date = max(valid_dates).strftime("%Y%m%d")
        return f"keyword_embeddings/date={latest_date}/keyword_embeddings.parquet"

    def update_embedding_registry(self, target_date_str: str) -> str:
        """
        [Core Logic]
        1. New Keywords 식별 (Daily - Master)
        2. Embedding Generation (Heavy Compute)
        3. Update & Save Registry
        """
        # 1. Path Setup
        y, m, d = target_date_str.split('-')
        daily_input_path = f"extracted_keywords/year={y}/month={m}/day={d}/data.parquet"
        prev_registry_path = self._find_latest_registry(target_date_str)

        # 2. Load Daily Keywords (Target for Embedding)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=daily_input_path)
            df_daily = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            daily_keywords = set(df_daily['keyword'].unique())
            print(f"📥 Loaded Daily Keywords: {len(daily_keywords)}")
        except self.s3.exceptions.NoSuchKey:
            print(f"⚠️ No keywords found for date: {target_date_str}")
            return None

        # 3. Load Existing Registry (To avoid re-calculation)
        df_registry = pd.DataFrame(columns=['keyword', 'vector'])
        existing_keywords = set()

        if prev_registry_path:
            try:
                print(f"📂 Loading Registry: {prev_registry_path}")
                obj_prev = self.s3.get_object(Bucket=self.bucket, Key=prev_registry_path)
                df_registry = pd.read_parquet(io.BytesIO(obj_prev['Body'].read()))
                existing_keywords = set(df_registry['keyword'].unique())
            except Exception as e:
                print(f"⚠️ Registry Load Failed: {e}")

        # 4. Identify "Un-embedded" Keywords
        new_keywords = list(daily_keywords - existing_keywords)
        print(f"🚀 Keywords requiring Embedding: {len(new_keywords)}")

        # 5. Generate Embeddings (The Main Task)
        new_embeddings = []
        if new_keywords:
            client = Client(host=self.ollama_host, timeout=300)
            print(f"🧠 Starting LLM Embedding Generation ({self.model})...")

            for kw in tqdm(new_keywords):
                try:
                    res = client.embeddings(model=self.model, prompt=kw)
                    new_embeddings.append({'keyword': kw, 'vector': res['embedding']})
                except Exception:
                    pass  # Log error

            # Clean up memory
            try:
                client.embeddings(model=self.model, prompt="", keep_alive=0)
            except:
                pass
        else:
            print("💤 No new keywords to embed.")

        # 6. Update Registry & Save
        if new_embeddings:
            df_new = pd.DataFrame(new_embeddings)
            final_df = pd.concat([df_registry, df_new], ignore_index=True)
        else:
            final_df = df_registry

        final_df = final_df.drop_duplicates(subset=['keyword'], keep='last')

        # Save as a new version (Snapshot)
        save_date = target_date_str.replace('-', '')
        output_key = f"keyword_embeddings/date={save_date}/keyword_embeddings.parquet"

        out_buf = io.BytesIO()
        final_df[['keyword', 'vector']].to_parquet(out_buf, index=False)
        self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=out_buf.getvalue())

        print(f"✅ Registry Updated: {output_key} (Total: {len(final_df)})")
        return output_key


# Service Wrapper
def run_embedding_update_service(updated_dates: list, aws_info: dict):
    manager = KeywordEmbeddingManager(aws_info)
    results = []
    for date_str in updated_dates:
        res = manager.update_embedding_registry(date_str)
        if res: results.append(res)
    return results