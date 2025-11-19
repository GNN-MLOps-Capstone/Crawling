import pandas as pd
import numpy as np
import torch
from torch_geometric.data import HeteroData
from torch_geometric.transforms import ToUndirected
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import StandardScaler

def build_graph(df):
    """
    Constructs a heterogeneous graph from the dataframe.
    Nodes: News, Keyword, Stock
    Edges: News-links_to-Keyword, News-mentions-Stock
    """
    print("Building graph...")
    
    # Helper to split comma-separated strings
    def split_and_clean(series):
        return series.str.split(',').apply(lambda x: [item.strip() for item in x if item.strip()] if isinstance(x, list) else [])

    # Edges
    df['keywords_list'] = split_and_clean(df['keywords'])
    news_to_keyword = df[['crawled_news_id', 'keywords_list']].explode('keywords_list').rename(columns={'keywords_list': 'keyword'}).dropna()
    
    df['stock_list'] = split_and_clean(df['stock'])
    news_to_stock = df[['crawled_news_id', 'stock_list']].explode('stock_list').rename(columns={'stock_list': 'stock'}).dropna()
    
    # Node Mapping
    news_id_map = {nid: i for i, nid in enumerate(df['crawled_news_id'])}
    
    unique_keywords = news_to_keyword['keyword'].unique()
    keyword_map = {k: i for i, k in enumerate(unique_keywords)}
    
    unique_stocks = news_to_stock['stock'].unique()
    stock_map = {s: i for i, s in enumerate(unique_stocks)}
    
    # Node Features
    # News: SBERT + PubDate
    sbert_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    title_embeddings = sbert_model.encode(df['title'].tolist(), show_progress_bar=True)
    
    pub_date_numeric = pd.to_datetime(df['pub_date']).astype('int64').values.reshape(-1, 1)
    scaler = StandardScaler()
    pub_date_scaled = scaler.fit_transform(pub_date_numeric)
    
    news_features = np.hstack([title_embeddings, pub_date_scaled])
    news_x = torch.tensor(news_features, dtype=torch.float)
    
    # Keyword & Stock: ID features (will be embedded)
    keyword_x = torch.arange(len(unique_keywords))
    stock_x = torch.arange(len(unique_stocks))
    
    # Build HeteroData
    data = HeteroData()
    data['news'].x = news_x
    data['keyword'].x = keyword_x
    data['stock'].x = stock_x
    
    # Add Edges
    src_news_kw = news_to_keyword['crawled_news_id'].map(news_id_map).values
    dst_keyword = news_to_keyword['keyword'].map(keyword_map).values
    data['news', 'links_to', 'keyword'].edge_index = torch.tensor([src_news_kw, dst_keyword], dtype=torch.long)
    
    src_news_st = news_to_stock['crawled_news_id'].map(news_id_map).values
    dst_stock = news_to_stock['stock'].map(stock_map).values
    data['news', 'mentions', 'stock'].edge_index = torch.tensor([src_news_st, dst_stock], dtype=torch.long)
    
    data = ToUndirected()(data)
    
    return data, news_id_map, keyword_map, stock_map
