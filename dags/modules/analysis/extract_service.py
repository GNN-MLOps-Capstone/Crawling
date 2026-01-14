import io
import pandas as pd
import boto3
from tqdm import tqdm

# 로직 모듈 Import
from modules.analysis.keyword_extractor import KeywordExtractor
from modules.analysis.stock_extractor import StockExtractor


def run_keyword_extraction(updated_dates: list, aws_info: dict) -> list:
    """[Service] 키워드 추출 파이프라인 실행"""
    s3 = boto3.client('s3', aws_access_key_id=aws_info['access_key'], aws_secret_access_key=aws_info['secret_key'],
                      endpoint_url=aws_info['endpoint_url'])
    processed = []
    extractor = KeywordExtractor()

    for date_str in updated_dates:
        print(f"🔎 [Keyword] Processing: {date_str}")
        y, m, d = date_str.split('-')
        input_key = f'refined_news/year={y}/month={m}/day={d}/data.parquet'
        output_key = f'extracted_keywords/year={y}/month={m}/day={d}/data.parquet'

        try:
            # Load
            obj = s3.get_object(Bucket='silver', Key=input_key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

            results = []
            for _, row in tqdm(df.iterrows(), total=len(df)):
                text = row.get('refined_text', row.get('text', ''))
                keywords = extractor.extract(text)
                if keywords:
                    results.append({'news_id': row['news_id'], 'keyword': keywords})

            if not results: continue

            # Save
            out_df = pd.DataFrame(results).explode('keyword').dropna()
            out_buf = io.BytesIO()
            out_df[['news_id', 'keyword']].to_parquet(out_buf, index=False)

            s3.put_object(Bucket='silver', Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Saved Keywords: {output_key}")
            processed.append(output_key)

        except s3.exceptions.NoSuchKey:
            print(f"⚠️ Data not found: {input_key}")
        except Exception as e:
            print(f"❌ Error: {e}")
            raise e

    return processed


def run_stock_extraction(updated_dates: list, stock_map: dict, aws_info: dict) -> list:
    """[Service] 주식 추출 파이프라인 실행"""
    s3 = boto3.client('s3', aws_access_key_id=aws_info['access_key'], aws_secret_access_key=aws_info['secret_key'],
                      endpoint_url=aws_info['endpoint_url'])
    processed = []
    extractor = StockExtractor(stock_map)  # 매핑 정보 주입

    for date_str in updated_dates:
        print(f"🔎 [Stock] Processing: {date_str}")
        y, m, d = date_str.split('-')
        input_key = f'refined_news/year={y}/month={m}/day={d}/data.parquet'
        output_key = f'extracted_stocks/year={y}/month={m}/day={d}/data.parquet'

        try:
            obj = s3.get_object(Bucket='silver', Key=input_key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

            results = []
            for _, row in tqdm(df.iterrows(), total=len(df)):
                text = row.get('refined_text', row.get('text', ''))

                candidates = extractor.find_candidates(text)
                for info in candidates:
                    if extractor.verify_relevance(info['name'], text):
                        results.append({'news_id': row['news_id'], 'stock_id': info['id'], 'stock_name': info['name']})

            if not results: continue

            out_df = pd.DataFrame(results)
            out_buf = io.BytesIO()
            out_df.to_parquet(out_buf, index=False)

            s3.put_object(Bucket='silver', Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Saved Stocks: {output_key}")
            processed.append(output_key)

        except s3.exceptions.NoSuchKey:
            print(f"⚠️ Data not found: {input_key}")
        except Exception as e:
            print(f"❌ Error: {e}")
            raise e

    return processed