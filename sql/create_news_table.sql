CREATE TYPE process_status_enum AS ENUM (
    'pending',          -- 초기 상태
    'url_filtered',     -- URL 검사에서 필터링됨
    'to_crawl',         -- URL 검사 통과, 크롤링 대기
    'crawling',         -- 크롤링 진행 중
    'crawl_success',    -- 크롤링 성공
    'crawl_failed',     -- 크롤링 실패
    'crawl_skipped'     -- 크롤링 스킵 (재시도 초과 등)
);

CREATE TABLE naver_news (
    news_id BIGSERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    pub_date TIMESTAMP NOT NULL,
    url VARCHAR(1000) UNIQUE NOT NULL,
    
    -- 메타데이터
    search_keyword VARCHAR(200),
    api_request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 처리 상태
    crawl_status process_status_enum DEFAULT 'pending',
    crawl_attempt_count INTEGER DEFAULT 0,
	url_filter_version VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_naver_news_pub_date ON naver_news(pub_date);
CREATE INDEX idx_naver_news_crawl_status ON naver_news(crawl_status);

-- updated_at 자동 업데이트를 위한 트리거
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;

$$ language 'plpgsql';

CREATE TRIGGER update_naver_news_updated_at BEFORE UPDATE ON naver_news
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- create crawled_news table

CREATE TYPE filter_status_enum AS ENUM ('pending', 'processing', 'passed', 'filtered_out');

CREATE TABLE crawled_news (
    crawled_news_id BIGSERIAL PRIMARY KEY,
    news_id BIGINT UNIQUE NOT NULL,
    
    -- 크롤링된 콘텐츠
    text TEXT,
    
    -- 크롤링 정보
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    crawler_version VARCHAR(50),
    response_time_ms INTEGER,
    
    -- 필터링 상태
    filter_status filter_status_enum DEFAULT 'pending',
	filter_version VARCHAR(50),
	filtered_at TIMESTAMP,
    filter_reason VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_news_id FOREIGN KEY (news_id) 
        REFERENCES naver_news(news_id) ON DELETE CASCADE
);

CREATE INDEX idx_crawled_news_filter_status ON crawled_news(filter_status);
CREATE INDEX idx_crawled_news_crawled_at ON crawled_news(crawled_at);
CREATE INDEX idx_crawled_news_news_id ON crawled_news(news_id);

CREATE TRIGGER update_crawled_news_updated_at BEFORE UPDATE ON crawled_news
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- create filtered_news

CREATE TABLE filtered_news (
    filtered_news_id BIGSERIAL PRIMARY KEY,
    news_id BIGINT UNIQUE NOT NULL,
    crawled_news_id BIGINT NOT NULL,

    refined_text text,                  -- 정제된 본문
    news_embedding vector,              -- 차원 미지정 (유연한 실험용)
    embedding_model_version varchar(50), -- 사용된 모델 정보

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_filtered_news_id FOREIGN KEY (news_id)
        REFERENCES naver_news(news_id) ON DELETE CASCADE,
    CONSTRAINT fk_filtered_crawled_id FOREIGN KEY (crawled_news_id)
        REFERENCES crawled_news(crawled_news_id) ON DELETE CASCADE
);

-- 트리거 설정: updated_at 자동 갱신
CREATE TRIGGER update_filtered_news_updated_at
BEFORE UPDATE ON filtered_news
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- 최종 필터링된 뉴스를 조회하기 쉽게 뷰 생성
CREATE VIEW v_filtered_news_full AS
SELECT 
    fn.filtered_news_id,
    nn.news_id,
    nn.title,
    nn.url,
    nn.pub_date,
    cn.text
FROM filtered_news fn
JOIN naver_news nn ON fn.news_id = nn.news_id
JOIN crawled_news cn ON fn.crawled_news_id = cn.crawled_news_id;
