-- 1. 뉴스-종목 매핑 테이블
CREATE TABLE news_stock_mapping (
    mapping_id SERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    stock_id CHARACTER(6) NOT NULL,
    extractor_version VARCHAR(50),
    weight FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (news_id) REFERENCES naver_news (news_id) ON DELETE CASCADE,
    FOREIGN KEY (stock_id) REFERENCES stocks (stock_id) ON DELETE CASCADE
);

-- 2. 뉴스-키워드 매핑 테이블
CREATE TABLE news_keyword_mapping (
    mapping_id SERIAL PRIMARY KEY,
    news_id BIGINT NOT NULL,
    keyword_id INTEGER NOT NULL,
    extractor_version VARCHAR(50),
    weight FLOAT DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (news_id) REFERENCES naver_news (news_id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords (keyword_id) ON DELETE CASCADE
);

-- 검색 성능을 위한 인덱스 (GNN 데이터 추출 시 필수)
CREATE INDEX idx_ns_mapping_news ON news_stock_mapping (news_id);
CREATE INDEX idx_nk_mapping_news ON news_keyword_mapping (news_id);