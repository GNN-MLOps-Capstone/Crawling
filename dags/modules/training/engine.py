# FILE: dags/modules/training/engine.py

import torch
from sklearn.metrics import roc_auc_score


def train_one_epoch(model, optimizer, data, criterion, target_edge_types):
    model.train()
    optimizer.zero_grad()

    edge_label_index_dict = {et: data[et].edge_label_index for et in target_edge_types}
    preds = model(data.x_dict, data.edge_index_dict, edge_label_index_dict)
    labels = {et: data[et].edge_label for et in target_edge_types}

    loss = 0
    for et in target_edge_types:
        if et in preds:
            loss += criterion(preds[et], labels[et].float())

    loss.backward()
    optimizer.step()
    return loss.item()


@torch.no_grad()
def evaluate(model, data, target_edge_types):
    model.eval()
    edge_label_index_dict = {et: data[et].edge_label_index for et in target_edge_types}
    preds = model(data.x_dict, data.edge_index_dict, edge_label_index_dict)
    labels = {et: data[et].edge_label for et in target_edge_types}

    total_auc = 0
    count = 0
    for et in target_edge_types:
        if et in preds:
            preds_sig = preds[et].sigmoid().cpu().numpy()
            labels_np = labels[et].cpu().numpy()
            try:
                score = roc_auc_score(labels_np, preds_sig)
                total_auc += score
                count += 1
            except ValueError:
                pass  # 라벨이 한 종류뿐일 때 등 예외 처리

    return total_auc / count if count > 0 else 0.0