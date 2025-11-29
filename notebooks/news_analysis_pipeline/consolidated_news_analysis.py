import os
import pandas as pd
import numpy as np
import torch
import torch.nn.functional as F
import mlflow
import boto3
import io
import pickle
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from torch_geometric.data import HeteroData
from torch_geometric.nn import SAGEConv, HeteroConv
from torch_geometric.transforms import ToUndirected, RandomLinkSplit
from torch.optim import Adam
from sentence_transformers import SentenceTransformer
from botocore.client import Config

# Import custom modules
from news_data_cleaning import fetch_data_from_db
from news_feature_engineering import extract_features
from news_graph_construction import build_graph
from preprocessing_experiments import get_preprocessor

# --- Configuration & Environment Variables ---
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_S3_ENDPOINT_URL = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

if mlflow.active_run():
    print("Closing active run...")
    mlflow.end_run()

# Set MLFlow Tracking URI
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("News_GNN_Analysis")

# MinIO Client
s3_client = boto3.client(
    's3',
    endpoint_url=MLFLOW_S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
BUCKET_NAME = "data-lake"

# Ensure bucket exists
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
except:
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    except Exception as e:
        print(f"Warning: Could not create bucket {BUCKET_NAME}. It might already exist or permission denied. Error: {e}")

# --- GNN Model ---
class GNNEncoder(torch.nn.Module):
    def __init__(self, hidden_dim, out_dim, news_feat_dim, num_keywords, num_stocks):
        super().__init__()
        self.keyword_emb = torch.nn.Embedding(num_keywords, hidden_dim)
        self.stock_emb = torch.nn.Embedding(num_stocks, hidden_dim)
        self.news_lin = torch.nn.Linear(news_feat_dim, hidden_dim)
        
        self.conv1 = HeteroConv({
            ('news', 'links_to', 'keyword'): SAGEConv(-1, hidden_dim),
            ('keyword', 'rev_links_to', 'news'): SAGEConv(-1, hidden_dim),
            ('news', 'mentions', 'stock'): SAGEConv(-1, hidden_dim),
            ('stock', 'rev_mentions', 'news'): SAGEConv(-1, hidden_dim),
        }, aggr='sum')

        self.conv2 = HeteroConv({
            ('news', 'links_to', 'keyword'): SAGEConv(-1, out_dim),
            ('keyword', 'rev_links_to', 'news'): SAGEConv(-1, out_dim),
            ('news', 'mentions', 'stock'): SAGEConv(-1, out_dim),
            ('stock', 'rev_mentions', 'news'): SAGEConv(-1, out_dim),
        }, aggr='sum')

    def forward(self, x_dict, edge_index_dict):
        x_dict = {
            'news': self.news_lin(x_dict['news']).relu(),
            'keyword': self.keyword_emb(x_dict['keyword']),
            'stock': self.stock_emb(x_dict['stock'])
        }
        x_dict = self.conv1(x_dict, edge_index_dict)
        x_dict = {key: x.relu() for key, x in x_dict.items()}
        x_dict = self.conv2(x_dict, edge_index_dict)
        return x_dict

class EdgeDecoder(torch.nn.Module):
    def __init__(self, hidden_dim):
        super().__init__()
        self.lin1 = torch.nn.Linear(2 * hidden_dim, hidden_dim)
        self.lin2 = torch.nn.Linear(hidden_dim, 1)

    def forward(self, z_src, z_dst):
        z = torch.cat([z_src, z_dst], dim=-1)
        z = self.lin1(z).relu()
        z = self.lin2(z)
        return z.view(-1)

class Model(torch.nn.Module):
    def __init__(self, hidden_dim, out_dim, news_feat_dim, num_keywords, num_stocks):
        super().__init__()
        self.encoder = GNNEncoder(hidden_dim, out_dim, news_feat_dim, num_keywords, num_stocks)
        self.decoders = torch.nn.ModuleDict({
            'links_to': EdgeDecoder(out_dim),
            'mentions': EdgeDecoder(out_dim),
        })

    def forward(self, x_dict, edge_index_dict, edge_label_index_dict):
        z_dict = self.encoder(x_dict, edge_index_dict)
        preds = {}
        for edge_type, edge_label_index in edge_label_index_dict.items():
            src_type, rel_type, dst_type = edge_type
            z_src = z_dict[src_type][edge_label_index[0]]
            z_dst = z_dict[dst_type][edge_label_index[1]]
            preds[edge_type] = self.decoders[rel_type](z_src, z_dst)
        return preds

# --- Training & Logging ---
def train_and_log(data, news_feat_dim, num_keywords, num_stocks, preprocessor_name="baseline"):
    print("Starting training...")
    
    # Split Data
    transform = RandomLinkSplit(
        num_val=0.1, num_test=0.1, is_undirected=True, add_negative_train_samples=True,
        edge_types=[('news', 'links_to', 'keyword'), ('news', 'mentions', 'stock')],
        rev_edge_types=[('keyword', 'rev_links_to', 'news'), ('stock', 'rev_mentions', 'news')],
    )
    train_data, val_data, test_data = transform(data)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    HIDDEN_DIM = 64
    OUT_DIM = 32
    EPOCHS = 50
    
    model = Model(HIDDEN_DIM, OUT_DIM, news_feat_dim, num_keywords, num_stocks).to(device)
    optimizer = Adam(model.parameters(), lr=0.01)
    criterion = torch.nn.BCEWithLogitsLoss()
    
    train_data = train_data.to(device)
    val_data = val_data.to(device)
    
    with mlflow.start_run():
        mlflow.set_tag("preprocessor", preprocessor_name)
        mlflow.log_param("hidden_dim", HIDDEN_DIM)
        mlflow.log_param("out_dim", OUT_DIM)
        mlflow.log_param("epochs", EPOCHS)
        
        for epoch in range(1, EPOCHS + 1):
            model.train()
            optimizer.zero_grad()
            
            edge_label_index_dict = {
                ('news', 'links_to', 'keyword'): train_data['news', 'links_to', 'keyword'].edge_label_index,
                ('news', 'mentions', 'stock'): train_data['news', 'mentions', 'stock'].edge_label_index
            }
            preds_dict = model(train_data.x_dict, train_data.edge_index_dict, edge_label_index_dict)
            
            labels_dict = {
                ('news', 'links_to', 'keyword'): train_data['news', 'links_to', 'keyword'].edge_label,
                ('news', 'mentions', 'stock'): train_data['news', 'mentions', 'stock'].edge_label
            }
            
            loss = 0
            for edge_type in preds_dict:
                loss += criterion(preds_dict[edge_type], labels_dict[edge_type].float())
            
            loss.backward()
            optimizer.step()
            
            # Validation
            model.eval()
            with torch.no_grad():
                val_edge_label_index_dict = {
                    ('news', 'links_to', 'keyword'): val_data['news', 'links_to', 'keyword'].edge_label_index,
                    ('news', 'mentions', 'stock'): val_data['news', 'mentions', 'stock'].edge_label_index
                }
                val_preds_dict = model(val_data.x_dict, val_data.edge_index_dict, val_edge_label_index_dict)
                val_labels_dict = {
                    ('news', 'links_to', 'keyword'): val_data['news', 'links_to', 'keyword'].edge_label,
                    ('news', 'mentions', 'stock'): val_data['news', 'mentions', 'stock'].edge_label
                }
                
                total_auc = 0
                for edge_type in val_preds_dict:
                    preds = val_preds_dict[edge_type].sigmoid().cpu().numpy()
                    labels = val_labels_dict[edge_type].cpu().numpy()
                    try:
                        total_auc += roc_auc_score(labels, preds)
                    except:
                        pass
                avg_auc = total_auc / len(val_preds_dict)
            
            print(f"Epoch {epoch:03d}: Loss {loss.item():.4f}, Val AUC {avg_auc:.4f}")
            mlflow.log_metric("loss", loss.item(), step=epoch)
            mlflow.log_metric("val_auc", avg_auc, step=epoch)
            
        # Save Model
        mlflow.pytorch.log_model(model, "gnn_model")
        
        # Extract Embeddings
        model.eval()
        with torch.no_grad():
            data = data.to(device)
            final_embeddings = model.encoder(data.x_dict, data.edge_index_dict)
            
        final_embeddings_cpu = {k: v.cpu().numpy() for k, v in final_embeddings.items()}
        return final_embeddings_cpu

# --- Save to MinIO ---
def save_to_minio(embeddings, news_map, keyword_map, stock_map):
    print("Saving to MinIO...")
    
    # Save Embeddings
    buffer = io.BytesIO()
    pickle.dump(embeddings, buffer)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, BUCKET_NAME, "embeddings/final_embeddings.pkl")
    
    # Save Maps
    maps = {'news': news_map, 'keyword': keyword_map, 'stock': stock_map}
    buffer = io.BytesIO()
    pickle.dump(maps, buffer)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, BUCKET_NAME, "embeddings/node_maps.pkl")
    
    print(f"Files uploaded to s3://{BUCKET_NAME}/embeddings/")

# --- Main Execution ---
if __name__ == "__main__":
    try:
        # Experiment Configuration
        EXPERIMENT_PREPROCESSOR = "baseline" 
        
        # 1. Fetch
        df = fetch_data_from_db()
        if df.empty:
            print("No data found. Exiting.")
            exit()
            
        # 2. Clean (Using Strategy)
        preprocessor = get_preprocessor(EXPERIMENT_PREPROCESSOR)
        df = preprocessor.process(df)
        
        # 3. Features
        df = extract_features(df)
        
        # 4. Graph
        data, news_map, keyword_map, stock_map = build_graph(df)
        
        # 5. Train
        news_feat_dim = data['news'].x.shape[1]
        num_keywords = data['keyword'].x.shape[0]
        num_stocks = data['stock'].x.shape[0]
        
        embeddings = train_and_log(data, news_feat_dim, num_keywords, num_stocks, EXPERIMENT_PREPROCESSOR)
        
        # 6. Save
        save_to_minio(embeddings, news_map, keyword_map, stock_map)
        
        print("Pipeline completed successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
