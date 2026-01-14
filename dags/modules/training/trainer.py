import torch
import s3fs
import os
import numpy as np
import pickle
import traceback
import mlflow
import torch_geometric.transforms as T
from torch_geometric.transforms import RandomLinkSplit
from torch.optim import Adam
from sklearn.metrics import roc_auc_score
from modules.training.models import Model


def run_training_pipeline(trainset_path, output_path, aws_info, epochs=50):
    print(f"🚀 [Trainer] Start. Endpoint: {aws_info.get('endpoint_url')}", flush=True)

    os.environ['AWS_ACCESS_KEY_ID'] = aws_info['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_info['secret_key']
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = aws_info['endpoint_url']
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    # --- 1. Load Data ---
    print(f"📂 Loading Trainset from: {trainset_path}", flush=True)
    try:
        fs = s3fs.S3FileSystem(
            key=aws_info['access_key'],
            secret=aws_info['secret_key'],
            client_kwargs={'endpoint_url': aws_info['endpoint_url']}
        )
        full_path = f"silver/{trainset_path}"
        with fs.open(full_path, 'rb') as f:
            data = torch.load(f, weights_only=False)
        print(f"✅ Data Loaded! Nodes: {data.num_nodes}", flush=True)

    except Exception as e:
        print(f"❌ [FATAL ERROR] Failed to load data.", flush=True)
        traceback.print_exc()
        raise e

    # --- 2. Data Preprocessing & Split ---
    print("✂️ Processing & Splitting Data...", flush=True)
    data = T.ToUndirected()(data)

    target_edge_types = [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]
    rev_edge_types = [('keyword', 'rev_has_keyword', 'news'), ('stock', 'rev_has_stock', 'news')]

    transform = RandomLinkSplit(
        num_val=0.1, num_test=0.1,
        is_undirected=True,
        add_negative_train_samples=True,
        edge_types=target_edge_types,
        rev_edge_types=rev_edge_types,
    )

    train_data, val_data, test_data = transform(data)

    # --- 3. Model Setup ---
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"🖥️ Using Device: {device}", flush=True)

    news_feat_dim = data['news'].x.shape[1]
    keyword_feat_dim = data['keyword'].x.shape[1]
    stock_feat_dim = data['stock'].x.shape[1]

    HIDDEN_DIM = 64
    OUT_DIM = 32

    model = Model(HIDDEN_DIM, OUT_DIM, news_feat_dim, keyword_feat_dim, stock_feat_dim).to(device)
    optimizer = Adam(model.parameters(), lr=0.01)
    criterion = torch.nn.BCEWithLogitsLoss()

    train_data = train_data.to(device)
    val_data = val_data.to(device)

    # --- 4. Training Loop ---
    print("🔄 Starting Training Loop...", flush=True)
    mlflow.set_experiment("News_GNN_Training")

    with mlflow.start_run():
        mlflow.log_param("epochs", epochs)

        for epoch in range(1, epochs + 1):
            model.train()
            optimizer.zero_grad()

            edge_label_index_dict = {et: train_data[et].edge_label_index for et in target_edge_types}
            preds_dict = model(train_data.x_dict, train_data.edge_index_dict, edge_label_index_dict)
            labels_dict = {et: train_data[et].edge_label for et in target_edge_types}

            loss = 0
            for edge_type in target_edge_types:
                if edge_type in preds_dict:
                    loss += criterion(preds_dict[edge_type], labels_dict[edge_type].float())

            loss.backward()
            optimizer.step()

            if epoch % 10 == 0:
                model.eval()
                with torch.no_grad():
                    val_edge_label_index_dict = {et: val_data[et].edge_label_index for et in target_edge_types}
                    val_preds_dict = model(val_data.x_dict, val_data.edge_index_dict, val_edge_label_index_dict)
                    val_labels_dict = {et: val_data[et].edge_label for et in target_edge_types}

                    total_auc = 0
                    count = 0
                    for edge_type in target_edge_types:
                        if edge_type in val_preds_dict:
                            preds = val_preds_dict[edge_type].sigmoid().cpu().numpy()
                            labels = val_labels_dict[edge_type].cpu().numpy()
                            try:
                                total_auc += roc_auc_score(labels, preds)
                                count += 1
                            except:
                                pass
                    avg_auc = total_auc / count if count > 0 else 0.0

                print(f"Epoch {epoch:03d}: Loss {loss.item():.4f}, Val AUC {avg_auc:.4f}", flush=True)
                mlflow.log_metric("loss", loss.item(), step=epoch)
                mlflow.log_metric("val_auc", avg_auc, step=epoch)

        # --- 5. Save Artifacts ---
        print("💾 Saving Model & Embeddings...", flush=True)

        # (1) Save Model Weights
        local_model_path = "/tmp/gnn_model.pt"
        torch.save(model.state_dict(), local_model_path)
        fs.put(local_model_path, f"gold/{output_path}model_weights.pt")
        mlflow.log_artifact(local_model_path)

        # 🟢 (2) Generate & Save Node Embeddings (이 부분이 없어서 에러가 났던 것)
        print("⚗️ Generating Node Embeddings for DB...", flush=True)
        model.eval()
        with torch.no_grad():
            full_data = data.to(device)
            # 모델의 Encoder를 통해 최종 벡터 추출
            final_embeddings = model.encoder(full_data.x_dict, full_data.edge_index_dict)

        # GPU Tensor -> CPU Numpy 변환
        final_embeddings_cpu = {k: v.cpu().numpy() for k, v in final_embeddings.items()}

        local_emb_path = "/tmp/node_embeddings.pkl"
        with open(local_emb_path, 'wb') as f:
            pickle.dump(final_embeddings_cpu, f)

        # Gold 버킷에 업로드
        fs.put(local_emb_path, f"gold/{output_path}node_embeddings.pkl")
        print(f"✅ Embeddings saved to gold/{output_path}node_embeddings.pkl")

        # Cleanup
        if os.path.exists(local_model_path): os.remove(local_model_path)
        if os.path.exists(local_emb_path): os.remove(local_emb_path)

        print(f"✅ Done. All artifacts saved to s3://gold/{output_path}", flush=True)