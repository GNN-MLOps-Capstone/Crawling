CREATE TABLE test_service_embeddings (
    service_id SERIAL PRIMARY KEY,
    entity_id VARCHAR(20) NOT NULL,
    entity_type VARCHAR(10) NOT NULL,  -- 'STOCK', 'KEYWORD'
    display_name VARCHAR(100),
    gnn_embedding vector,              -- 차원수 미지정 (유연성)
    model_version VARCHAR(50),         -- MLflow Run ID 등 기록
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 필터링 속도를 위한 일반 인덱스
CREATE INDEX idx_test_type_lookup ON test_service_embeddings (entity_type);



CREATE TABLE final_service_embeddings (
    service_id SERIAL PRIMARY KEY,
    entity_id VARCHAR(20) NOT NULL,
    entity_type VARCHAR(10) NOT NULL,
    display_name VARCHAR(100),
    gnn_embedding vector(256),         -- 실험 후 확정된 차원 고정
    model_version VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- [핵심] 종목(STOCK)만 모아서 미리 지도를 그려두는 부분 인덱스
CREATE INDEX idx_final_stock_hnsw ON final_service_embeddings
USING hnsw (gnn_embedding vector_cosine_ops)
WHERE (entity_type = 'STOCK');

-- [핵심] 키워드(KEYWORD)만 모아서 미리 지도를 그려두는 부분 인덱스
CREATE INDEX idx_final_keyword_hnsw ON final_service_embeddings
USING hnsw (gnn_embedding vector_cosine_ops)
WHERE (entity_type = 'KEYWORD');