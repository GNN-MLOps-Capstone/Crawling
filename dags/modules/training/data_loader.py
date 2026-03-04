# FILE: dags/modules/training/data_loader.py

import s3fs
import torch
import torch_geometric.transforms as T
from torch_geometric.transforms import RandomLinkSplit


def load_data_from_s3(path, aws_info):
    """
    S3(MinIO)에서 .pt 파일을 다운로드하여 로드합니다.
    DAG가 아니라 Docker Container 내부에서 실행됩니다.
    """
    print(f"📂 [DataLoader] Fetching data from: silver/{path}", flush=True)

    fs = s3fs.S3FileSystem(
        key=aws_info['access_key'],
        secret=aws_info['secret_key'],
        client_kwargs={'endpoint_url': aws_info['endpoint_url']}
    )

    # S3 경로에서 직접 로드
    with fs.open(f"silver/{path}", 'rb') as f:
        data = torch.load(f, weights_only=False)

    print(f"✅ [DataLoader] Success! Nodes: {data.num_nodes}", flush=True)
    return data, fs  # fs는 나중에 저장할 때 재사용


def preprocess_data(data, val_ratio=0.1, test_ratio=0.1):
    """
    로드된 데이터를 PyG 포맷에 맞게 전처리하고 Train/Val/Test로 분할합니다.
    """
    print("✂️ [DataLoader] Splitting Dataset...", flush=True)

    # 방향성 제거 (Undirected)
    data = T.ToUndirected()(data)

    target_edge_types = [('news', 'has_keyword', 'keyword'), ('news', 'has_stock', 'stock')]
    rev_edge_types = [('keyword', 'rev_has_keyword', 'news'), ('stock', 'rev_has_stock', 'news')]

    transform = RandomLinkSplit(
        num_val=val_ratio,
        num_test=test_ratio,
        is_undirected=True,
        add_negative_train_samples=True,
        edge_types=target_edge_types,
        rev_edge_types=rev_edge_types,
    )

    # (train_data, val_data, test_data) 반환
    return transform(data), target_edge_types