import torch
import os
import pickle
import mlflow
import traceback
from torch.optim import Adam
import torch_geometric.transforms as T  # 🟢 [추가] 변환 도구 Import

from modules.training.models import Model
from modules.training.data_loader import load_data_from_s3, preprocess_data
from modules.training.engine import train_one_epoch, evaluate


def run_training_pipeline(trainset_path, output_path, aws_info, config):
    print(f"🚀 [Trainer] Pipeline Started.", flush=True)

    os.environ['AWS_ACCESS_KEY_ID'] = aws_info['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_info['secret_key']
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = aws_info['endpoint_url']
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    try:
        # 1. 데이터 로드 (S3 -> Raw Data)
        raw_data, fs = load_data_from_s3(trainset_path, aws_info)

        # 2. 데이터 전처리 (Split)
        (train_data, val_data, test_data), target_edge_types = preprocess_data(
            raw_data,
            val_ratio=config['training'].get('val_ratio', 0.1),
            test_ratio=config['training'].get('test_ratio', 0.1)
        )

        # 3. 모델 초기화
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

        model = Model(
            hidden_dim=config['model']['hidden_dim'],
            out_dim=config['model']['out_dim'],
            news_feat_dim=raw_data['news'].x.shape[1],
            keyword_feat_dim=raw_data['keyword'].x.shape[1],
            stock_feat_dim=raw_data['stock'].x.shape[1]
        ).to(device)

        optimizer = Adam(model.parameters(), lr=config['training']['lr'])
        criterion = torch.nn.BCEWithLogitsLoss()

        train_data = train_data.to(device)
        val_data = val_data.to(device)

        # 4. 학습 루프
        EPOCHS = config['training']['epochs']
        mlflow.set_experiment("GNN_Training_Pipeline")

        with mlflow.start_run():
            mlflow.log_params(config['model'])
            mlflow.log_params(config['training'])

            for epoch in range(1, EPOCHS + 1):
                loss = train_one_epoch(model, optimizer, train_data, criterion, target_edge_types)

                if epoch % 10 == 0:
                    val_auc = evaluate(model, val_data, target_edge_types)
                    print(f"Epoch {epoch:03d}: Loss {loss:.4f}, Val AUC {val_auc:.4f}", flush=True)
                    mlflow.log_metric("loss", loss, step=epoch)
                    mlflow.log_metric("val_auc", val_auc, step=epoch)
                else:
                    mlflow.log_metric("loss", loss, step=epoch)

            # 5. 저장 (여기가 문제였던 곳!)
            print("💾 Saving Artifacts...", flush=True)

            # (1) 모델 가중치
            local_model_path = "/tmp/gnn_model.pt"
            torch.save(model.state_dict(), local_model_path)
            fs.put(local_model_path, f"gold/{output_path}model_weights.pt")
            mlflow.log_artifact(local_model_path)

            # (2) Node 임베딩
            model.eval()
            with torch.no_grad():
                full_data = T.ToUndirected()(raw_data)
                full_data_gpu = full_data.to(device)

                final_embeddings = model.encoder(full_data_gpu.x_dict, full_data_gpu.edge_index_dict)

            final_embeddings_cpu = {k: v.cpu().numpy() for k, v in final_embeddings.items()}

            local_emb_path = "/tmp/node_embeddings.pkl"
            with open(local_emb_path, 'wb') as f:
                pickle.dump(final_embeddings_cpu, f)
            fs.put(local_emb_path, f"gold/{output_path}node_embeddings.pkl")

            if os.path.exists(local_model_path): os.remove(local_model_path)
            if os.path.exists(local_emb_path): os.remove(local_emb_path)

        print("✅ Pipeline Finished Successfully.")

    except Exception as e:
        print("❌ Pipeline Failed.")
        traceback.print_exc()
        raise e