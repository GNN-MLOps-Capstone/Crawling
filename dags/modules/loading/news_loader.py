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

    def update_analysis_info(self, s3_client, bucket: str, key: str):
        """
        Gemini 분석 결과(요약, 감성)를 기존 뉴스 테이블에 업데이트합니다.
        """
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        except Exception:
            print(f"⚠️ Analysis file not found: {key}")
            return

        if df.empty:
            return

        # PostgreSQL의 bulk update 문법 (VALUES 절 활용)
        # bulk_insert가 execute_values를 사용한다고 가정할 때 가장 효율적인 방식입니다.
        update_sql = """
                     UPDATE public.filtered_news AS t
                     SET summary    = v.summary,
                         sentiment  = v.sentiment,
                         updated_at = CURRENT_TIMESTAMP FROM (VALUES %s) AS v(news_id, summary, sentiment)
                     WHERE t.news_id = v.news_id \
                     """

        # 데이터 준비 (순서: news_id, summary, sentiment)
        values = []
        for _, row in df.iterrows():
            values.append((
                row['news_id'],
                row.get('summary', ''),
                row.get('sentiment', '중립')
            ))

        # 기존 BaseLoader의 bulk_insert 재사용
        self.bulk_insert(update_sql, values, page_size=500)
        print(f"✅ Updated Analysis (Summary/Sentiment) for {len(values)} items.")