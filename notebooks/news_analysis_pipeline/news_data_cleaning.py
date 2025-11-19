import os
import pandas as pd
import re
import html
import hanja
from sqlalchemy import create_engine
from kiwipiepy import Kiwi

def fetch_data_from_db():
    """
    Fetches news data from the PostgreSQL database.
    Requires DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME environment variables.
    """
    print("Fetching data from database...")
    DB_HOST = os.environ.get("DB_HOST", "localhost")
    DB_PORT = os.environ.get("DB_PORT", "5432")
    DB_USER = os.environ.get("DB_USER", "airflow")
    DB_PASS = os.environ.get("DB_PASS", "airflow")
    DB_NAME = os.environ.get("DB_NAME", "airflow")

    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    
    query = """
    SELECT 
        nn.news_id,
        nn.title,
        nn.pub_date,
        nn.url,
        cn.crawled_news_id,
        cn.text
    FROM naver_news nn
    JOIN crawled_news cn ON nn.news_id = cn.news_id
    WHERE cn.text IS NOT NULL
    """
    
    df = pd.read_sql(query, engine)
    print(f"Fetched {len(df)} rows.")
    return df

def clean_text_data(df):
    """
    Cleans the news text data:
    - Deduplication
    - Removing NaNs
    - Removing specific patterns
    - HTML unescaping & Hanja translation
    - Filtering short text
    - Filtering sports news
    - Checking title-body consistency
    """
    print("Cleaning data...")
    original_len = len(df)
    
    # 1. Deduplication
    df = df.drop_duplicates(subset=['text'], keep='first').reset_index(drop=True)
    
    # 2. Remove NaNs
    df = df.dropna(subset=['title', 'text'])
    
    # 3. Remove specific patterns
    patterns_to_remove = [
        r'\([^)]*\)|\[[^\]]*\]|\{[^}]*\}|<[^>]*>',
        r'\S*@\S*',
        r'http\S+|www\S+',
        r'\S*=\S*',
        r'[가-힣]{2,4}\s?(기자|특파원)',
        r'(연합뉴스|뉴스1|뉴시스|조선일보|중앙일보|동아일보|한겨레|한국일보|서울경제|매일경제|머니투데이|한국경제|경향신문|헤럴드경제|아시아경제|이데일리|데일리안|세계일보|국민일보|뉴스핌|파이낸셜뉴스)',
        r'(무단전재\s*및\s*재배포\s*금지|저작권자[^.,\n]+|Copyright\s*ⓒ[^.,\n]+|끝\)|끝$)',
        r'(사진\s*=\s*[^.,\n]+|관련기사|주요뉴스|이 시각 뉴스)[^.\n]*',
        r'SNS\s*기사보내기',
        r'카카오톡로\s*기사보내기',
        r'URL복사로\s*기사보내기',
        r'다른\s*공유\s*찾기',
        r'기사와\s*상관\s*없는\s*자료사진',
        r'※.*|▶.*|★.*'
    ]
    
    for pattern in patterns_to_remove:
        df['text'] = df['text'].apply(lambda x: re.sub(pattern, '', str(x)))
        
    # HTML unescape & Hanja translation
    df['title'] = df['title'].apply(lambda x: html.unescape(str(x)))
    df['text'] = df['text'].apply(lambda x: html.unescape(str(x)))
    df['text'] = df['text'].apply(lambda x: hanja.translate(x, 'substitution'))
    
    # Clean whitespace and special chars
    df['title'] = df['title'].apply(lambda x: re.sub(r'[^A-Za-z0-9가-힇.,\"\':·!?\-%~&]', ' ', x))
    df['title'] = df['title'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
    
    df['text'] = df['text'].apply(lambda x: re.sub(r'[^A-Za-z0-9가-힇.,\"\':·!?\-%~&]', ' ', x))
    df['text'] = df['text'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
    
    # 4. Filter short text
    df = df[df['title'].str.len() > 0]
    df = df[df['text'].str.len() > 20]
    
    # 5. Sports News Filtering
    SPORT_KEYWORDS = ["야구", "농구", "축구", "골프", "e스포츠", "LCK", "롤드컵", "KBO", "KBL", "KPGA", "프로야구", "프로농구"]
    SPORT_CONTEXT_KEYWORDS = ["경기", "시즌", "우승", "패배", "완패", "승리", "라운드", "감독", "리그", "순위", "스코어", "1위", "2위", "3위", "시드", "예선", "결승", "챔피언", "대회", "출전", "득점", "홈런", "발로란트", "공동", "GEN.G", "코치", "포스트시즌", "정규시즌", "완파", "역전", "프로", "진출", "연봉"]
    
    def is_sports_news(row):
        content = (str(row['title']) + " " + str(row['text'])).lower()
        found_sports = [k for k in SPORT_KEYWORDS if k.lower() in content]
        found_context = [k for k in SPORT_CONTEXT_KEYWORDS if k.lower() in content]
        has_business = any(k in content for k in ["매출", "실적", "계약", "공시", "출시", "사업"])
        
        if found_sports and len(found_context) >= 3 and not has_business:
            return True
        return False
        
    df['is_sports'] = df.apply(is_sports_news, axis=1)
    df = df[~df['is_sports']].drop(columns=['is_sports'])
    
    # 6. Title-Body Consistency (Kiwi)
    try:
        kiwi = Kiwi()
        def check_noun_title(row):
            title = str(row['title'])
            text = str(row['text'])
            result = kiwi.analyze(title)
            if not result: return True
            
            tagged_words = result[0][0]
            title_nouns = [word for word, tag, _, _ in tagged_words if tag in ['NNG', 'NNP', 'SL']]
            
            if not title_nouns: return True
            
            match_count = sum(1 for noun in title_nouns if noun in text)
            return (match_count / len(title_nouns)) >= 0.3
            
        df['is_consistent'] = df.apply(check_noun_title, axis=1)
        df = df[df['is_consistent']].drop(columns=['is_consistent'])
    except Exception as e:
        print(f"Warning: Kiwi analysis failed, skipping consistency check. Error: {e}")

    print(f"Data cleaning complete. Rows: {original_len} -> {len(df)}")
    return df.reset_index(drop=True)
