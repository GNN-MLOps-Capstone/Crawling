import io
import pandas as pd
from modules.loading.base_loader import BaseLoader

class StockLoader(BaseLoader):
    def load_mappings(self, s3_client, bucket: str, key: str):
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            if df.empty: return
        except: return

        news_ids = tuple(df['news_id'].unique().tolist())
        values = [(row['news_id'], row['stock_id'], 'local_v1.0', 1.0) for _, row in df.iterrows()]

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                if news_ids:
                    cur.execute("DELETE FROM public.news_stock_mapping WHERE news_id IN %s", (news_ids,))
            conn.commit()
        finally:
            conn.close()

        sql = "INSERT INTO public.news_stock_mapping (news_id, stock_id, extractor_version, weight) VALUES %s"
        self.bulk_insert(sql, values)
        print(f"✅ Loaded {len(values)} stock mappings.")