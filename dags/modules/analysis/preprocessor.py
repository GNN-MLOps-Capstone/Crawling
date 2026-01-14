import re
import html

# Hanja는 설치 안 된 환경 대비용
try:
    import hanja
except ImportError:
    hanja = None


class NewsPreProcessor:
    def __init__(self):
        self.remove_patterns = [
            r'\(.*?\)|\[.*?\]|\{.*?\}|<.*?>', r'\S*@\S*', r'http\S+|www\S+',
            r'\S*=\S*', r'[가-힣]{2,4}\s?(기자|특파원)',
            r'(연합뉴스|뉴스1|뉴시스|조선일보|중앙일보|동아일보|한겨레|한국일보|서울경제|매일경제|머니투데이|한국경제|경향신문|헤럴드경제|아시아경제|이데일리|데일리안|세계일보|국민일보|뉴스핌|파이낸셜뉴스)',
            r'(무단전재\s*및\s*재배포\s*금지|저작권자[^.,\n]+|Copyright\s*ⓒ[^.,\n]+|끝\)|끝$)',
            r'(사진\s*=\s*[^.,\n]+|관련기사|주요뉴스|이 시각 뉴스)[^.\n]*',
            r'※.*|▶.*|★.*'
        ]

    def clean_text_basic(self, text: str) -> str:
        if not isinstance(text, str): return ""
        text = html.unescape(text)
        if hanja: text = hanja.translate(text, 'substitution')
        for pat in self.remove_patterns: text = re.sub(pat, '', text)
        text = re.sub(r'[^A-Za-z0-9가-힇.,\"\'\:\·\!\?\-\%\~\&]', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def is_english_only(self, text: str) -> str:
        """한글이 포함된 문장만 남김"""
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        cleaned = [s for s in sentences if re.search(r'[가-힣]', s)]
        return '. '.join(cleaned) + ('.' if cleaned else '')

    def is_sports_news(self, text: str) -> bool:
        content = str(text).lower()
        sports_kwd = ["야구", "농구", "축구", "골프", "e스포츠", "KBO"]
        context_kwd = ["경기", "시즌", "우승", "패배", "리그", "순위", "스코어"]

        has_sports = any(k in content for k in sports_kwd)
        kwd_count = sum(1 for k in context_kwd if k in content)
        return has_sports and kwd_count >= 3