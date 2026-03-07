# FILE: modules/training/trainer.py

import torch
import os
import pickle
import mlflow
import traceback
import copy
import re
from torch.optim import Adam

# Refactored imports
from modules.training.models import Model
from modules.training.data_loader import load_data_from_s3, preprocess_data
from modules.training.engine import train_one_epoch, evaluate, evaluate_golden_set
from modules.training.utils import load_json_file, log_file_artifact, build_gold_similarity_report
from modules.training.utils import set_seed 

def run_training_pipeline(trainset_path, output_path, aws_info, config, raw_data_cache=None, gate_threshold=0.25):
    """
    Airflow 및 Optuna에서 공용으로 사용하는 학습 파이프라인
    - raw_data_cache: (Optuna용) 이미 로드된 데이터를 넘겨받아 재사용
    """
    print(f"🚀 [Trainer] Start Pipeline: {config.get('mlflow', {}).get('experiment_name')}")

    # 환경변수 세팅
    os.environ['AWS_ACCESS_KEY_ID'] = aws_info['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_info['secret_key']
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = aws_info['endpoint_url']
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    try:
        seed = config.get('training', {}).get('seed', 42)
        set_seed(seed)

        # train_date 우선순위:
        # 1) config.training.train_date
        # 2) trainset_path 내 date=YYYYMMDD 또는 YYYY-MM-DD 패턴 추출
        train_date = config.get('training', {}).get('train_date')
        if not train_date:
            m = re.search(r"date=(\d{8})", str(trainset_path))
            if m:
                d = m.group(1)
                train_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            else:
                m2 = re.search(r"(\d{4}-\d{2}-\d{2})", str(trainset_path))
                if m2:
                    train_date = m2.group(1)
        
        # 1. 데이터 로드 (캐시가 있으면 사용, 없으면 로드)
        if raw_data_cache:
            raw_data, name_to_idx_map, fs = raw_data_cache
        else:
            raw_data, name_to_idx_map, fs = load_data_from_s3(trainset_path, aws_info, config=config)

        # 2. Golden Case 로드
        golden_cases = load_json_file('golden_cases.json')

        # 3. 전처리
        (train_data, val_data, test_data), target_edge_types = preprocess_data(
            raw_data,
            val_ratio=config['training'].get('val_ratio', 0.1),
            test_ratio=config['training'].get('test_ratio', 0.1)
        )

        # 4. 모델 설정
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        # [변경] Model 초기화 시 config 전체 전달
        model = Model(
            hidden_dim=config['model']['hidden_dim'],
            out_dim=config['model']['out_dim'],
            news_feat_dim=raw_data['news'].x.shape[1],
            keyword_feat_dim=raw_data['keyword'].x.shape[1],
            stock_feat_dim=raw_data['stock'].x.shape[1],
            config=config # <--- 추가됨
        ).to(device)

        optimizer = Adam(model.parameters(), lr=config['training']['lr'])
        # Criterion은 InfoNCE 내부에서 계산하므로 외부 선언 불필요 (engine.py 참조)

        train_data = train_data.to(device)
        val_data = val_data.to(device)

        # 5. MLflow Experiment 설정
        mlflow_conf = config.get('mlflow', {})
        mlflow.set_experiment(mlflow_conf.get('experiment_name', 'Default'))

        with mlflow.start_run() as run:
            # Artifact 저장 (Golden Set 추적용)
            log_file_artifact('golden_cases.json')
            
            # 파라미터 로깅
            mlflow.log_params(config['model'])
            mlflow.log_params(config['training'])
            mlflow.log_param("trainset_path", str(trainset_path))
            if train_date:
                mlflow.log_param("train_date", str(train_date))
            
            # 태그 로깅
            for k, v in mlflow_conf.get('tags', {}).items():
                mlflow.set_tag(k, v)
            mlflow.set_tag("trainset_path", str(trainset_path))
            if train_date:
                mlflow.set_tag("train_date", str(train_date))
            metric_schema = config.get('training', {}).get('metric_schema', 'v2')
            mlflow.set_tag('metric_schema', metric_schema)
            mlflow.set_tag("artifact_store_mode", "mlflow_only")

            # 6. 학습 루프
            selection_metric = config.get('training', {}).get('selection_metric', 'val_mrr')
            selection_mode = config.get('training', {}).get('selection_mode', 'max')
            mlflow.set_tag('selection_metric', selection_metric)
            mlflow.set_tag('selection_mode', selection_mode)

            best_metric = None
            best_epoch = 0
            EPOCHS = config['training']['epochs']
            
            # [추가] 학습용 파라미터 추출
            batch_size = config['training'].get('batch_size', 4096)
            temperature = config['training'].get('temperature', 0.1)
            loss_mode = config['training'].get('loss_mode', 'infonce')
            weighting = config['training'].get('weighting', 'log1p')
            min_train_cooccur = config['training'].get('min_train_cooccur', 1)
            enable_low_cooccur_filter = config['training'].get('enable_low_cooccur_filter', False)
            eval_k = config['training'].get('eval_k', 10)
            implicit_seed = config['training'].get('implicit_eval_seed', seed)
            implicit_sample_limit = config['training'].get('implicit_sample_limit', 500)
            holdout_metrics = {}
            best_state = None
            
            def _filter_core_metrics(metrics_dict):
                core_keys = {
                    "val_mrr",
                    f"val_hit_at_{eval_k}",
                    "holdout_final_score",
                    "holdout_final_score_missing",
                    "holdout_final_final_score",
                    "holdout_final_final_score_missing",
                    "gold_final_score",
                    "gold_final_score_missing",
                    f"holdout_key2stock_mrr",
                    f"holdout_key2key_mrr",
                    f"holdout_stock2stock_mrr",
                    f"holdout_key2stock_hit_at_{eval_k}",
                    f"holdout_key2key_hit_at_{eval_k}",
                    f"holdout_stock2stock_hit_at_{eval_k}",
                    f"holdout_final_key2stock_mrr",
                    f"holdout_final_key2key_mrr",
                    f"holdout_final_stock2stock_mrr",
                    f"holdout_final_key2stock_hit_at_{eval_k}",
                    f"holdout_final_key2key_hit_at_{eval_k}",
                    f"holdout_final_stock2stock_hit_at_{eval_k}",
                }
                return {k: v for k, v in metrics_dict.items() if k in core_keys}

            for epoch in range(1, EPOCHS + 1):
                # [변경] Config 값 전달
                loss, train_stats = train_one_epoch(
                    model, optimizer, train_data, None, target_edge_types, 
                    batch_size=batch_size, 
                    temperature=temperature,
                    loss_mode=loss_mode,
                    weighting=weighting,
                    min_train_cooccur=min_train_cooccur,
                    enable_low_cooccur_filter=enable_low_cooccur_filter
                )

                # 평가 주기 (10 에폭마다 or 마지막 에폭)
                if epoch % 10 == 0 or epoch == EPOCHS:
                    
                    # 마지막 에폭인 경우에만 무거운 평가(Implicit Eval) 수행
                    is_last_epoch = (epoch == EPOCHS)
                    
                    # [변경] k 값 전달
                    metrics = evaluate(
                        model, 
                        val_data, 
                        target_edge_types, 
                        k=eval_k, 
                        eval_implicit=False,
                        split_prefix="val",
                        implicit_seed=implicit_seed,
                        implicit_sample_limit=implicit_sample_limit,
                        compute_final_score=False,
                    )
                    
                    
                    # Golden Set 평가
                    if name_to_idx_map and golden_cases:
                        gold_metrics = evaluate_golden_set(
                            model, raw_data, golden_cases, name_to_idx_map, device, k=eval_k
                        )
                        metrics.update(gold_metrics)

                    # 로깅
                    print(
                        f"Ep {epoch:03d} | Loss: {loss:.4f} | "
                        f"{selection_metric}: {metrics.get(selection_metric,0):.4f} | "
                        f"avg_w: {train_stats.get('train_avg_weight', 0.0):.4f} | "
                        f"used_ratio: {train_stats.get('train_positive_used_ratio', 0.0):.3f}",
                        flush=True
                    )
                    
                    mlflow.log_metric("loss", loss, step=epoch)
                    for k, v in train_stats.items():
                        mlflow.log_metric(k, v, step=epoch)
                    for k, v in _filter_core_metrics(metrics).items():
                        mlflow.log_metric(k, v, step=epoch)
                    
                    # Best Model 추적
                    current_metric = metrics.get(selection_metric)
                    if current_metric is not None:
                        if best_metric is None:
                            best_metric = current_metric
                            best_epoch = epoch
                            best_state = copy.deepcopy(model.state_dict())
                        else:
                            if selection_mode == 'min':
                                if current_metric < best_metric:
                                    best_metric = current_metric
                                    best_epoch = epoch
                                    best_state = copy.deepcopy(model.state_dict())
                            else:
                                if current_metric > best_metric:
                                    best_metric = current_metric
                                    best_epoch = epoch
                                    best_state = copy.deepcopy(model.state_dict())
                else:
                    mlflow.log_metric("loss", loss, step=epoch)
                    for k, v in train_stats.items():
                        mlflow.log_metric(k, v, step=epoch)

            # 7. Holdout 평가 (Best + Final Checkpoint)
            if best_state is None:
                best_state = copy.deepcopy(model.state_dict())
                best_epoch = EPOCHS
            final_state = copy.deepcopy(model.state_dict())
            model.load_state_dict(best_state)
            holdout_metrics = evaluate(
                model,
                test_data.to(device),
                target_edge_types,
                k=eval_k,
                eval_implicit=True,
                split_prefix="holdout",
                implicit_seed=implicit_seed,
                implicit_sample_limit=implicit_sample_limit,
                compute_final_score=True,
            )
            mlflow.log_metrics(_filter_core_metrics(holdout_metrics), step=best_epoch)
            model.load_state_dict(final_state)

            final_holdout_metrics = evaluate(
                model,
                test_data.to(device),
                target_edge_types,
                k=eval_k,
                eval_implicit=True,
                split_prefix="holdout_final",
                implicit_seed=implicit_seed,
                implicit_sample_limit=implicit_sample_limit,
                compute_final_score=True,
            )
            mlflow.log_metrics(_filter_core_metrics(final_holdout_metrics), step=EPOCHS)

            # 8. 결과 저장 (Weights & Embeddings)
            print("💾 Saving Model & Embeddings...", flush=True)
            local_path = "/tmp/gnn_model.pt"
            torch.save(model.state_dict(), local_path)
            mlflow.log_artifact(local_path)

            # 임베딩 생성 (학습 완료된 모델로 전체 추론)
            model.eval()
            with torch.no_grad():
                full_gpu = raw_data.to(device)
                emb = model.encoder(full_gpu.x_dict, full_gpu.edge_index_dict)
            
            emb_cpu = {k: v.cpu().numpy() for k, v in emb.items()}
            local_emb_path = "/tmp/node_embeddings.pkl"
            with open(local_emb_path, 'wb') as f: pickle.dump(emb_cpu, f)
            mlflow.log_artifact(local_emb_path)

            # Node mapping도 동일 run의 artifact로 보관해 serving 단계에서 동일 버전 참조
            local_mapping_path = "/tmp/node_mapping.pkl"
            mapping_s3_path = f"silver/{str(trainset_path).replace('hetero_graph.pt', 'node_mapping.pkl')}"
            if fs.exists(mapping_s3_path):
                with fs.open(mapping_s3_path, 'rb') as f_in, open(local_mapping_path, 'wb') as f_out:
                    f_out.write(f_in.read())
                mlflow.log_artifact(local_mapping_path)

            # Gold keyword/stock cosine similarity report
            report = build_gold_similarity_report(
                emb,
                golden_cases,
                name_to_idx_map,
                edge_index_dict=raw_data.edge_index_dict,
                top_k=eval_k,
                min_cooccur=3
            )
            if report:
                report_path = "/tmp/gold_similarity.txt"
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(report)
                print(report, flush=True)
                mlflow.log_artifact(report_path)

            # Cleanup
            if os.path.exists(local_path): os.remove(local_path)
            if os.path.exists(local_emb_path): os.remove(local_emb_path)
            if os.path.exists("/tmp/node_mapping.pkl"): os.remove("/tmp/node_mapping.pkl")
            if os.path.exists("/tmp/gold_similarity.txt"): os.remove("/tmp/gold_similarity.txt")

            # Run summary
            if best_metric is not None:
                mlflow.log_metric("summary_best_epoch", best_epoch)
                mlflow.log_metric("summary_selection_value", best_metric)
                mlflow.set_tag("summary_selection_metric", selection_metric)
                mlflow.set_tag("summary_selection_mode", selection_mode)
            if holdout_metrics:
                holdout_final = holdout_metrics.get("holdout_final_score", 0.0)
                holdout_missing = holdout_metrics.get("holdout_final_score_missing", 1.0)
                mlflow.log_metric("summary_holdout_final_score", holdout_final)
                mlflow.log_metric("summary_holdout_final_score_missing", holdout_missing)
                mlflow.set_tag("summary_holdout_eval_epoch", str(best_epoch))
            if final_holdout_metrics:
                holdout_final2 = final_holdout_metrics.get("holdout_final_final_score", 0.0)
                holdout_missing2 = final_holdout_metrics.get("holdout_final_final_score_missing", 1.0)
                mlflow.log_metric("summary_holdout_final_checkpoint_score", holdout_final2)
                mlflow.log_metric("summary_holdout_final_checkpoint_missing", holdout_missing2)
                mlflow.set_tag("summary_holdout_final_checkpoint_epoch", str(EPOCHS))

            # 9. Gate & Promotion Tagging
            holdout_final_checkpoint = final_holdout_metrics.get("holdout_final_final_score", 0.0) if final_holdout_metrics else 0.0
            holdout_final_checkpoint_missing = final_holdout_metrics.get("holdout_final_final_score_missing", 1.0) if final_holdout_metrics else 1.0
            gate_passed = (
                float(holdout_final_checkpoint_missing) == 0.0 and
                float(holdout_final_checkpoint) > float(gate_threshold)
            )
            version_name = str(output_path).strip("/").split("/")[-1] if output_path else run.info.run_id

            mlflow.log_metric("gate_threshold_holdout_final_final_score", float(gate_threshold))
            mlflow.log_metric("gate_passed", 1.0 if gate_passed else 0.0)
            mlflow.set_tag("gate_metric", "holdout_final_final_score")
            mlflow.set_tag("gate_operator", ">")
            mlflow.set_tag("gate_threshold", str(gate_threshold))
            mlflow.set_tag("gate_require_missing_zero", "true")
            mlflow.set_tag("gate_passed", str(gate_passed).lower())
            mlflow.set_tag("candidate_version", version_name)

            if gate_passed:
                mlflow.set_tag("status", "candidate")
                print(
                    f"✅ Gate passed: holdout_final_final_score={holdout_final_checkpoint:.4f} > {gate_threshold}. status=candidate",
                    flush=True
                )
            else:
                print(
                    f"⛔ Gate blocked: score={holdout_final_checkpoint:.4f}, missing={holdout_final_checkpoint_missing}, threshold={gate_threshold}",
                    flush=True
                )

        print("✅ Pipeline Finished.")
        return best_metric if best_metric is not None else 0.0

    except Exception as e:
        print("❌ Pipeline Failed.")
        traceback.print_exc()
        raise e
