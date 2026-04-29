import pandas as pd


def add_embeddings_to_df(df: pd.DataFrame,
                         model_name: str = "bge-m3",
                         host: str = "http://ollama:11434") -> pd.DataFrame:
    """
    [Ollama] DataFrame의 'refined_text'를 임베딩하여 'news_embedding' 컬럼 추가
    """
    if df.empty or 'refined_text' not in df.columns:
        return df

    from ollama import Client
    from tqdm import tqdm

    client = Client(host=host)
    embeddings = []
    MAX_CHAR_LIMIT = 3000

    print(f"🧠 [NewsEmbedding] Generating for {len(df)} rows...")

    for text in tqdm(df['refined_text']):
        try:
            if len(text) < 10:
                embeddings.append(None)
                continue

            safe_text = text[:MAX_CHAR_LIMIT]
            res = client.embeddings(model=model_name, prompt=safe_text)
            embeddings.append(res['embedding'])
        except Exception:
            embeddings.append(None)

    df['news_embedding'] = embeddings
    return df.dropna(subset=['news_embedding']).copy()
