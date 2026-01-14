import json
from ollama import Client


class KeywordExtractor:
    def __init__(self, model_name="hf.co/MLP-KTLim/llama-3-Korean-Bllossom-8B-gguf-Q4_K_M:Q4_K_M",
                 host="http://ollama:11434"):
        self.client = Client(host=host, timeout=180)
        self.model = model_name

    def _get_target_count(self, text_len: int) -> int:
        if text_len < 700:
            return 3
        elif text_len < 2300:
            return 4
        elif text_len < 4500:
            return 5
        else:
            return 6

    def _chunk_text(self, text: str, max_chars=3000) -> list:
        if len(text) <= max_chars: return [text]
        chunks = []
        while text:
            if len(text) <= max_chars:
                chunks.append(text)
                break
            split_idx = text.rfind('.', 0, max_chars)
            if split_idx == -1: split_idx = max_chars
            chunks.append(text[:split_idx + 1])
            text = text[split_idx + 1:].strip()
        return chunks

    def _verify_keywords(self, keywords: list, original_text: str) -> list:
        clean_text = "".join(original_text.split())
        return [k for k in keywords if "".join(k.split()) in clean_text]

    def extract(self, text: str) -> list:
        if not text: return []

        target_n = self._get_target_count(len(text))
        chunks = self._chunk_text(text)
        all_keywords = set()

        for chunk in chunks:
            try:
                res = self.client.chat(
                    model=self.model,
                    format='json',
                    messages=[{
                        'role': 'system',
                        'content': f'핵심 명사 키워드를 최대 {target_n}개 뽑아 JSON으로 응답. 예: {{"keywords": ["A", "B"]}}'
                    }, {
                        'role': 'user',
                        'content': f"본문: {chunk}"
                    }],
                    options={'temperature': 0.1, 'num_ctx': 8192}
                )
                data = json.loads(res['message']['content'])
                all_keywords.update(data.get('keywords', []))
            except Exception:
                continue

        verified = self._verify_keywords(list(all_keywords), text)
        # 중복 제거 및 개수 제한
        return list(dict.fromkeys(verified))[:target_n]