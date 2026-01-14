import io
import pandas as pd
import numpy as np
import boto3
from modules.loading.base_loader import BaseLoader


class NewsLoader(BaseLoader):
    def load_filtered_news(self, s3_client, bucket: str, key: str):
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        except Exception:
            print(f"⚠️ File not found: {key}")
            return

        upsert_sql = """
                     INSERT INTO public.filtered_news
                         (news_id, refined_text, news_embedding, embedding_model_version)
                     VALUES %s ON CONFLICT (news_id) 
            DO \
                     UPDATE SET
                         refined_text = EXCLUDED.refined_text, \
                         news_embedding = EXCLUDED.news_embedding, \
                         embedding_model_version = EXCLUDED.embedding_model_version, \
                         updated_at = CURRENT_TIMESTAMP; \
                     """

        values = []
        for _, row in df.iterrows():
            vec = row['news_embedding']
            if isinstance(vec, np.ndarray): vec = vec.tolist()
            values.append((row['news_id'], row['refined_text'], vec, 'bge-m3'))

        self.bulk_insert(upsert_sql, values, page_size=500)
        print(f"✅ Upserted {len(values)} news items.")