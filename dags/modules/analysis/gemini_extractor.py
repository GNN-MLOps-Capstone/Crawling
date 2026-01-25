import json
import time
import random
import re
from google import genai
from google.genai import types

class GeminiExtractor:
    def __init__(self, stock_map: dict, api_key: str):
        self.stock_map = stock_map
        self.client = genai.Client(api_key=api_key)
        
        # 1차 통합 분석 프롬프트 (노트북 코드 적용)
        self.analyze_config = types.GenerateContentConfig(
            temperature=my_config['temperature'],
            system_instruction=my_config['system_instruction'],
            response_mime_type="application/json"
        )

    def extract(self, text: str, news_id: str) -> dict:
        result = {
            'news_id': news_id,
            'related_stocks': [],
            'keywords': [],
            'sentiment': "에러",
            'summary': "분석 실패"
        }

        if not text: return result

        max_retries = 5
        base_delay = 2

        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash-lite", 
                    contents=text,
                    config=self.analyze_config
                )
                        
                data = json.loads(response.text)
                if isinstance(data, list):
                    data = data[0] if len(data) > 0 else {}

                raw_stocks = data.get('related_stocks', [])
                keywords = data.get('keywords', [])
                
                # 주식 필터링 로직
                valid_stocks = []
                names_to_exclude = set()
                
                for s_name in raw_stocks:
                    if isinstance(s_name, dict):
                        values = list(s_name.values())
                        s_name = str(values[0]) if values else ""
                    
                    s_name_clean = str(s_name).strip()
                    if s_name_clean in self.stock_map:
                        official_name = self.stock_map[s_name_clean]['name'] # stock_map 구조에 맞게 조정 필요
                        valid_stocks.append(official_name)
                        names_to_exclude.add(official_name)
                        names_to_exclude.add(s_name_clean)
                    elif not re.search(r'[^가-힣a-zA-Z0-9\s]', s_name_clean) and len(s_name_clean) > 1:
                        # 매핑되지 않은 기업명은 키워드에라도 추가
                        keywords.append(s_name_clean)

                valid_stocks = list(set(valid_stocks))
                unique_keywords = list(set(keywords))

                # 키워드에서 주식명 제외
                filtered_keywords = [k for k in unique_keywords if k not in names_to_exclude]
                # 키워드에 정식 종목명 추가
                filtered_keywords.extend(valid_stocks)
                
                result['related_stocks'] = valid_stocks
                result['keywords'] = filtered_keywords
                result['sentiment'] = data.get('sentiment', "중립")
                result['summary'] = data.get('summary', "요약 실패")
                return result

            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    print(f"❌ [Stage 1] ID:{news_id} 최종 실패: {e}")
                    break

        return result
    
my_config = {
    "temperature": 0.3,
    "system_instruction": """
    너는 금융 뉴스 데이터 분석 전문가야. 기사 내용을 분석해서 투자 정보를 추출해.

    [핵심 작업 절차]
    1. **Full Scan**: 기사의 첫 문장부터 마지막 문장까지 **한 글자도 빠뜨리지 말고 정독**해.
    2. **Event Check**: 기사 안에는 서로 다른 여러 기업의 소식이 나열되어 있을 수 있다. (예: 특징주 모음, 섹터 결산 등)
    3. **Selection**: 각 기업별로 **'구체적인 사건(신제품, 실적, 급등락, 공시 등)'이 서술된 경우**에만 추출해.

    [상세 규칙]
    1. related_stocks: 
       기업이 아래 **3가지 카테고리 중 하나 이상**에 해당하면 무조건 추출해.
       
       **(A) 비즈니스/재무/영업 (Business & Sales)**
         - 실적, 계약, M&A, 공시.
         - **신규 서비스/제품 출시, 대규모 마케팅.**
         - **전시회 참가(CES, TGS, 지스타 등), 신작 공개/시연, 베타테스트(CBT/OBT) 진행.**
           (이유: 게임/IT 기업의 경우, 신작에 대한 '기대감'이나 '공개 행사' 자체가 중요한 투자 재료임. 기자가 '체험해봤다'는 형식의 기사라도 신작 공개가 핵심이면 추출할 것.)
       
       **(B) ESG/사회공헌/협력 (Cooperation & ESG)**
         - 업무협약(MOU), 제휴, 정부 지원사업 참여, 기부, 상생 활동.
       
       **(C) 리스크/사건사고 (Risk & Issue)**
         - 수사, 규제, 소송, 해킹, 화재, 횡령.
         - 기업 인프라 악용, 보안 사고, 서비스 장애 등 관리 책임 이슈.
         - 기업의 대응(해명, 사과 등)이 포함된 경우.

       **[제외 기준]**
       - 단순히 비교 대상으로 언급된 경쟁사.
       - 기사의 핵심 사건과 직접적인 관련이 없는 단순 배경 기업.
    
    2. keywords: 
       - 기사의 길이와 정보량에 따라 **핵심 명사(Noun)를 3개에서 6개 사이**로 유동적으로 추출해.
       - 내용이 짧거나 단순하면 3개만, 복잡하고 중요하면 최대 6개까지 추출.
       - (중요) 개수를 맞추기 위해 불필요한 단어를 억지로 포함하지 말 것.

    3. summary: 기사를 한 줄로 요약.
    4. sentiment: 주가에 미칠 영향 (긍정/부정/중립).

    반드시 JSON 형식으로만 응답해. 잡담하지 마.
    """
}
