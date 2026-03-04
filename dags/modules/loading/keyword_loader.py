import io
import pandas as pd
import numpy as np
from modules.loading.base_loader import BaseLoader


class KeywordLoader(BaseLoader):
    def sync_master_keywords(self, s3_client, bucket: str, key: str):
        if not key: return

        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        except:
            return

        upsert_sql = """
                     INSERT INTO public.keywords (word, embedding_vector)
                     VALUES %s ON CONFLICT (word) 
            DO \
                     UPDATE SET embedding_vector = EXCLUDED.embedding_vector; \
                     """

        values = [(row['keyword'], row['vector'].tolist() if isinstance(row['vector'], np.ndarray) else row['vector'])
                  for _, row in df.iterrows()]

        self.bulk_insert(upsert_sql, values, page_size=1000)
        print(f"✅ Synced {len(values)} master keywords.")

    def load_mappings(self, s3_client, bucket: str, key: str):
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            if df.empty: return
        except:
            return

        # 1. Keyword ID Lookup
        keywords = df['keyword'].unique().tolist()
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT word, keyword_id FROM public.keywords WHERE word = ANY(%s)", (keywords,))
                db_map = {row[0]: row[1] for row in cur.fetchall()}

                # 2. Delete existing mappings for these news_ids
                news_ids = tuple(df['news_id'].unique().tolist())
                cur.execute("DELETE FROM public.news_keyword_mapping WHERE news_id IN %s", (news_ids,))
            conn.commit()
        finally:
            conn.close()

        # 3. Prepare Insert
        values = []
        for _, row in df.iterrows():
            if row['keyword'] in db_map:
                values.append((row['news_id'], db_map[row['keyword']], 'local_v1.0', 1.0))

        sql = "INSERT INTO public.news_keyword_mapping (news_id, keyword_id, extractor_version, weight) VALUES %s"
        self.bulk_insert(sql, values)
        print(f"✅ Loaded {len(values)} keyword mappings.")