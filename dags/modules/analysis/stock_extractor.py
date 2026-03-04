import re
from ollama import Client


class StockExtractor:
    def __init__(self, stock_map: dict, model_name="hf.co/MLP-KTLim/llama-3-Korean-Bllossom-8B-gguf-Q4_K_M:Q4_K_M",
                 host="http://ollama:11434"):
        self.stock_map = stock_map
        self.client = Client(host=host, timeout=300)
        self.model = model_name
        self.regex = self._build_regex(stock_map)

    def _build_regex(self, mapping: dict):
        unique_names = sorted(list(mapping.keys()), key=len, reverse=True)
        josa = r'(?:은|는|이|가|을|를|의|와|과|로|으로|에|에서|에게|까지|부터|만|도)?'
        suffix = r'(?:그룹|팀|연구소|노조|본부|직원)?'

        pattern = "|".join(
            rf"\b(?:{re.escape(name)})(?:{suffix})?(?={josa}\b)"
            for name in unique_names
        )
        return re.compile(pattern, re.IGNORECASE)

    def find_candidates(self, text: str) -> list:
        if not text: return []
        matches = self.regex.findall(str(text))
        found_stocks = {}
        for m in matches:
            key = m.lower()
            if key in self.stock_map:
                s_id = self.stock_map[key]['id']
                found_stocks[s_id] = self.stock_map[key]
        return list(found_stocks.values())

    def verify_relevance(self, stock_name: str, text: str) -> bool:
        prompt = (
            f"주식 종목 '{stock_name}'이(가) 다음 뉴스 본문에서 다뤄졌습니까? "
            f"본문: {text[:2000]}\n반드시 '예' 또는 '아니오'로만 대답."
        )
        try:
            res = self.client.generate(model=self.model, prompt=prompt, options={'temperature': 0.1})
            return "예" in res['response']
        except:
            return False