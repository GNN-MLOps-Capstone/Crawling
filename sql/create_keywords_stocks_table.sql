CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE stocks (
    stock_id          CHAR(6) PRIMARY KEY,        -- 종목코드 (005930) - 6자리 고정
    isin              CHAR(12),                   -- 표준코드 (KR7005930003) - 선택사항
    market            VARCHAR(10),                -- KOSPI, KOSDAQ
    industry          VARCHAR(100),               -- 업종
    stock_name        VARCHAR(100) NOT NULL,      -- 종목명
    summary_text      TEXT,                       -- 기업 개요

    -- pgvector 임베딩
    summary_embedding vector
);

-- 검색 속도 향상을 위해 종목명에 인덱스 추가 (선택)
CREATE INDEX IF NOT EXISTS idx_stock_name ON stocks(stock_name);

CREATE TABLE IF NOT EXISTS aliases (
    alias_id    SERIAL PRIMARY KEY,           -- 자동 증가 ID
    stock_id    CHAR(6) NOT NULL,             -- FK (stocks 테이블 참조)
    alias_name  VARCHAR(100) NOT NULL,        -- 별명 (삼전, 하이닉스 등)

    CONSTRAINT fk_stock
        FOREIGN KEY(stock_id)
        REFERENCES stocks(stock_id)
        ON DELETE CASCADE
);

ALTER TABLE aliases
ADD CONSTRAINT unique_stock_alias UNIQUE (stock_id, alias_name);


-- 조인 성능 향상을 위해 FK 컬럼에 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_alias_stock_id ON aliases(stock_id);

CREATE TABLE keywords (
    keyword_id       SERIAL PRIMARY KEY,
    word             VARCHAR(100) UNIQUE NOT NULL, -- 단어 중복 방지

    -- pgvector 임베딩
    embedding_vector vector
);