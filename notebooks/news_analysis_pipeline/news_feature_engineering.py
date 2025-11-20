import pandas as pd
import numpy as np
import re
import urllib.request
from sklearn.feature_extraction.text import TfidfVectorizer

def extract_features(df):
    """
    Extracts features from the news data:
    - Stock names (using KRX list)
    - Keywords (using TF-IDF)
    """
    print("Extracting features...")
    
    # 1. Stock Extraction
    try:
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download'
        response = urllib.request.urlopen(url)
        df_krx = pd.read_html(response.read(), header=0, flavor='lxml', encoding='cp949')[0]
        stock_name_list = df_krx['회사명'].tolist()
        
        escaped_names = sorted([re.escape(name) for name in stock_name_list], key=len, reverse=True)
        regex_pattern = '|'.join(escaped_names)
        
        df['stock'] = df['text'].str.findall(regex_pattern).apply(
            lambda x: ','.join(list(dict.fromkeys(x))) if isinstance(x, list) else ''
        )
    except Exception as e:
        print(f"Warning: Failed to fetch KRX stock list. Error: {e}")
        df['stock'] = ''

    # 2. Keyword Extraction (TF-IDF)
    # Since we don't have pre-computed keywords, we extract top keywords from text
    tfidf = TfidfVectorizer(max_features=1000, stop_words='english') # Adjust max_features as needed
    try:
        tfidf_matrix = tfidf.fit_transform(df['text'])
        feature_names = np.array(tfidf.get_feature_names_out())
        
        def get_top_keywords(row_idx, top_n=5):
            row_data = tfidf_matrix[row_idx]
            sorted_indices = np.argsort(row_data.data)[::-1][:top_n]
            top_indices = row_data.indices[sorted_indices]
            return ','.join(feature_names[top_indices])

        df['keywords'] = [get_top_keywords(i) for i in range(len(df))]
    except Exception as e:
        print(f"Warning: TF-IDF extraction failed. Error: {e}")
        df['keywords'] = ''
        
    return df
