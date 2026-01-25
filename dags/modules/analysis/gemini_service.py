import io
import pandas as pd
import boto3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# 리팩토링된 모듈 Import
from modules.analysis.gemini_extractor import GeminiExtractor
from modules.analysis.gemini_sentiment import SentimentAnalyzer

def run_stage1_extraction(updated_dates: list, stock_map: dict, aws_info: dict, api_key: str) -> list:
    """[Service] 1단계: 뉴스 기본 정보(종목, 키워드, 요약, 전체감성) 추출"""
    s3 = boto3.client('s3', aws_access_key_id=aws_info['access_key'], aws_secret_access_key=aws_info['secret_key'],
                      endpoint_url=aws_info['endpoint_url'])
    processed_keys = []
    extractor = GeminiExtractor(stock_map, api_key)
    MAX_WORKERS = 8 # API 등급에 맞춰 조절

    for date_str in updated_dates:
        print(f"🔎 [Stage 1] Processing: {date_str}")
        y, m, d = date_str.split('-')
        input_key = f'refined_news/year={y}/month={m}/day={d}/data.parquet'
        # 1차 분석 결과 임시 저장 경로
        output_key = f'extracted_stage1/year={y}/month={m}/day={d}/data.parquet'

        try:
            # Load Data
            obj = s3.get_object(Bucket='silver', Key=input_key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

            print(f"🚀 {date_str} 데이터 1단계 분석 중... (총 {len(df)}건)")
            results = {}
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_idx = {
                    executor.submit(extractor.extract, row.get('refined_text', row.get('text', '')), row['news_id']): idx 
                    for idx, row in df.iterrows()
                }
                for future in tqdm(as_completed(future_to_idx), total=len(df), desc="Stage 1 Extraction"):
                    idx = future_to_idx[future]
                    results[idx] = future.result()

            # Merge Results
            for idx, res in results.items():
                df.at[idx, 'related_stocks'] = res['related_stocks']
                df.at[idx, 'keywords'] = res['keywords']
                df.at[idx, 'overall_sentiment'] = res['sentiment']
                df.at[idx, 'summary'] = res['summary']

            # Save to S3
            out_buf = io.BytesIO()
            df.to_parquet(out_buf, index=False)
            s3.put_object(Bucket='silver', Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Stage 1 Saved: {output_key}")
            processed_keys.append(output_key)

        except s3.exceptions.NoSuchKey:
            print(f"⚠️ Data not found: {input_key}")
        except Exception as e:
            print(f"❌ Error in Stage 1: {e}")

    return processed_keys


def run_stage2_sentiment(updated_dates: list, aws_info: dict, api_key: str) -> list:
    """[Service] 2단계: 관련 주식이 있는 기사 대상 상세 감성 분석"""
    s3 = boto3.client('s3', aws_access_key_id=aws_info['access_key'], aws_secret_access_key=aws_info['secret_key'],
                      endpoint_url=aws_info['endpoint_url'])
    processed_keys = []
    analyzer = SentimentAnalyzer(api_key)
    MAX_WORKERS = 3 # 2차 분석은 프롬프트가 길어 부하가 클 수 있으므로 워커 수를 줄임

    for date_str in updated_dates:
        print(f"🔎 [Stage 2] Processing: {date_str}")
        y, m, d = date_str.split('-')
        # 1차 분석이 끝난 데이터를 Input으로 사용
        input_key = f'extracted_stage1/year={y}/month={m}/day={d}/data.parquet'
        # 최종 결과 저장 경로
        output_key = f'extracted_final/year={y}/month={m}/day={d}/data.parquet'

        try:
            # Load Data
            obj = s3.get_object(Bucket='silver', Key=input_key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

            # 분석 대상 필터링: related_stocks 리스트가 비어있지 않은 행만 선택
            target_df = df[df['related_stocks'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
            
            print(f"🔬 {date_str} 데이터 2단계 분석 중... (대상 {len(target_df)}건)")
            results = {}
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_idx = {
                    executor.submit(
                        analyzer.analyze, 
                        row.get('refined_text', row.get('text', '')), 
                        row['related_stocks'], 
                        row['keywords']
                    ): idx 
                    for idx, row in target_df.iterrows()
                }
                for future in tqdm(as_completed(future_to_idx), total=len(target_df), desc="Stage 2 Sentiment"):
                    idx = future_to_idx[future]
                    results[idx] = future.result()

            # Merge Results (전체 df에 병합)
            df['related_stocks_sentiment'] = None
            df['keywords_sentiment'] = None
            for idx, res in results.items():
                df.at[idx, 'related_stocks_sentiment'] = res['stock_sentiments']
                df.at[idx, 'keywords_sentiment'] = res['keyword_sentiments']

            # Save Final Data to S3
            out_buf = io.BytesIO()
            df.to_parquet(out_buf, index=False)
            s3.put_object(Bucket='silver', Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Final Data Saved: {output_key}")
            processed_keys.append(output_key)

        except s3.exceptions.NoSuchKey:
            print(f"⚠️ Stage 1 Data not found: {input_key}")
        except Exception as e:
            print(f"❌ Error in Stage 2: {e}")

    return processed_keys