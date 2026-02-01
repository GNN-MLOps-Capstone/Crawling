import io
import json
import time
import yaml
import boto3
import pandas as pd
import sys  # [추가] 로그 강제 출력을 위해 필요
from google import genai
from google.genai import types


class GeminiNewsAnalyzer:
    def __init__(self, aws_info: dict, config_path: str, stock_map: dict):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_info['access_key'],
            aws_secret_access_key=aws_info['secret_key'],
            endpoint_url=aws_info['endpoint_url']
        )
        self.bucket = self.config['aws']['bucket_name']
        self.stock_map = stock_map
        self.client = genai.Client(api_key=aws_info.get('gemini_api_key'))
        self.model_name = self.config['gemini']['model_name']
        self.gen_config = types.GenerateContentConfig(
            temperature=self.config['gemini']['temperature'],
            system_instruction=self.config['gemini']['system_prompt'],
            response_mime_type="application/json"
        )

    def _call_gemini(self, text: str):
        max_retries = self.config['gemini']['max_retries']
        for attempt in range(max_retries):
            try:
                # [디버깅] 호출 시작
                # print(f"   Sending request to Gemini (len: {len(text)})...", end='', flush=True)

                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=text,
                    config=self.gen_config
                )
                data = json.loads(response.text)

                # ✅ [FIX] 응답이 리스트([])로 오면 딕셔너리({})로 변환
                if isinstance(data, list):
                    data = data[0] if len(data) > 0 else {}

                return data

            except Exception as e:
                print(f"⚠️ API Error (Attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(4 ** attempt)
                else:
                    return None
        return None

    def _validate_stocks(self, raw_stocks: list) -> list:
        validated = []
        seen = set()
        for s_name in raw_stocks:
            if isinstance(s_name, dict): s_name = list(s_name.values())[0]
            clean_name = str(s_name).strip().lower()

            if clean_name in self.stock_map:
                info = self.stock_map[clean_name]
                if info['id'] not in seen:
                    validated.append({'stock_id': info['id'], 'stock_name': info['name']})
                    seen.add(info['id'])
        return validated

    def process_analysis(self, updated_dates: list):
        processed_paths = []

        for date_str in updated_dates:
            print(f"🚀 [Gemini] Analyzing News for: {date_str}", flush=True)
            y, m, d = date_str.split('-')

            input_key = f"{self.config['paths']['input_refined']}/year={y}/month={m}/day={d}/data.parquet"

            try:
                # [디버깅] 파일 로드 시작 알림
                print(f"   Downloading from S3: {input_key} ...", flush=True)
                obj = self.s3.get_object(Bucket=self.bucket, Key=input_key)
                df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                print(f"   ✅ Loaded {len(df)} rows.", flush=True)
            except self.s3.exceptions.NoSuchKey:
                print(f"⚠️ Data not found: {input_key}", flush=True)
                continue
            except Exception as e:
                print(f"❌ Error loading S3 data: {e}", flush=True)
                continue

            stock_data, keyword_data, analysis_data = [], [], []
            total_count = len(df)

            # [수정] tqdm 제거하고 주기적 print 사용
            for idx, row in df.iterrows():
                # 10건마다 로그 출력 (Airflow UI에서 진행상황 확인용)
                if idx % 10 == 0:
                    print(f"   Processing {idx}/{total_count} ({(idx / total_count) * 100:.1f}%) ...", flush=True)

                text = row.get('refined_text', '')
                if not text: continue

                res = self._call_gemini(text)
                if not res: continue

                # 1. Stocks
                valid_stocks = self._validate_stocks(res.get('related_stocks', []))
                for s in valid_stocks:
                    stock_data.append({
                        'news_id': row['news_id'],
                        'stock_id': s['stock_id'],
                        'stock_name': s['stock_name']
                    })

                # 2. Keywords
                for k in res.get('keywords', []):
                    keyword_data.append({'news_id': row['news_id'], 'keyword': k})

                # 3. Analysis (Summary, Sentiment)
                analysis_data.append({
                    'news_id': row['news_id'],
                    'summary': res.get('summary', ''),
                    'sentiment': res.get('sentiment', '중립')
                })

            print(f"✅ Finished Analysis for {date_str}. Saving...", flush=True)

            # S3 저장 (나머지 코드는 동일)
            paths = {}
            if stock_data:
                key = f"{self.config['paths']['output_stocks']}/year={y}/month={m}/day={d}/data.parquet"
                self._save_parquet(stock_data, key)
                paths['stocks'] = key

            if keyword_data:
                key = f"{self.config['paths']['output_keywords']}/year={y}/month={m}/day={d}/data.parquet"
                self._save_parquet(keyword_data, key)
                paths['keywords'] = key

            if analysis_data:
                key = f"{self.config['paths']['output_analysis']}/year={y}/month={m}/day={d}/data.parquet"
                self._save_parquet(analysis_data, key)
                paths['analysis'] = key

            processed_paths.append(paths)

        return processed_paths

    def _save_parquet(self, data: list, key: str):
        df = pd.DataFrame(data)
        out_buf = io.BytesIO()
        df.to_parquet(out_buf, index=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=out_buf.getvalue())
        print(f"   💾 Saved S3: {key}", flush=True)


# Service Wrapper
def run_gemini_service(updated_dates: list, aws_info: dict, config_path: str, stock_map: dict):
    analyzer = GeminiNewsAnalyzer(aws_info, config_path, stock_map)
    return analyzer.process_analysis(updated_dates)