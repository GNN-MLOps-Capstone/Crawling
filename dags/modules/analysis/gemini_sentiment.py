import json
import time
import random
from google import genai
from google.genai import types

class SentimentAnalyzer:
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        
        # 2차 상세 감성 분석 프롬프트
        self.sentiment_config = types.GenerateContentConfig(
            temperature=my_config['temperature'], 
            system_instruction=my_config['system_instruction'],
            response_mime_type="application/json"
        )

    def analyze(self, text: str, target_stocks: list, target_keywords: list) -> dict:
        result = {
            'stock_sentiments': ["에러"] * len(target_stocks),
            'keyword_sentiments': ["에러"] * len(target_keywords)
        }

        if not target_stocks and not target_keywords:
            return {'stock_sentiments': [], 'keyword_sentiments': []}

        all_targets = target_stocks + target_keywords
        input_prompt = f"[뉴스 본문]\n{text}\n\n[분석 대상 목록]\n{', '.join(all_targets)}"

        max_retries = 5
        base_delay = 5
        sentiment_map = {} 

        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash-lite",
                    contents=input_prompt,
                    config=self.sentiment_config
                )
                data = json.loads(response.text)
                
                for item in data.get('items', []):
                    t_name = item.get('target', '').strip()
                    sentiment_map[t_name] = item.get('sentiment', '중립')
                
                # 순서 보장 매핑
                result['stock_sentiments'] = [sentiment_map.get(s, "중립") for s in target_stocks]
                result['keyword_sentiments'] = [sentiment_map.get(k, "중립") for k in target_keywords]
                return result

            except Exception as e:
                error_msg = str(e)
                if any(code in error_msg for code in ["429", "RESOURCE_EXHAUSTED", "500", "503"]):
                    if attempt < max_retries - 1:
                        sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                        time.sleep(sleep_time)
                        continue
                print(f"❌ [Stage 2] 분석 실패: {e}")
                break

        return result
    
my_config = {
    "temperature": 0.1,
    "system_instruction": """
    너는 금융 뉴스 감성 분석기야. 
    내가 제공하는 **[뉴스 본문]**과 **[분석 대상 목록]**을 보고, 
    각 대상이 이 뉴스 문맥상 긍정인지, 부정인지, 중립인지 판단해.

    [작업 절차]
    1. 각 대상(종목, 키워드)이 뉴스에서 어떻게 서술되는지 확인한다.
    2. **판단 근거(Reason)**를 먼저 생각한 뒤, 최종 **감성(Sentiment)**을 결정한다.
            
    [감성 기준]
    - **긍정(Positive)**: 주가 상승 요인, 긍정적 묘사, 호재, 성장.
    - **부정(Negative)**: 주가 하락 요인, 부정적 묘사, 악재, 리스크.
    - **중립(Neutral)**: 단순 언급, 정보 전달, 판단 불가.

    [출력 형식 - JSON]
    입력받은 대상들을 **반드시 하나도 빠짐없이** 포함해야 함.
    {
        "items": [
            {"target": "삼성전자", "sentiment": "긍정", "reason": "공급 계약 체결"},
            {"target": "SK하이닉스", "sentiment": "중립", "reason": "단순 비교 대상"},
            {"target": "AI", "sentiment": "긍정", "reason": "산업 성장 기대"}
        ]
    }
    """
}
