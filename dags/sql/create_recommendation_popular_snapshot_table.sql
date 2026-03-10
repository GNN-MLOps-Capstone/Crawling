CREATE TABLE IF NOT EXISTS public.recommendation_popular_snapshot (
    snapshot_at TIMESTAMPTZ NOT NULL,
    rank INTEGER NOT NULL,
    news_id BIGINT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    domain VARCHAR(255),
    category VARCHAR(255),
    open_count INTEGER NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_at, news_id),
    CONSTRAINT fk_recommendation_popular_snapshot_news
        FOREIGN KEY (news_id) REFERENCES public.naver_news(news_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_recommendation_popular_snapshot_snapshot_at
    ON public.recommendation_popular_snapshot (snapshot_at DESC, rank ASC);

CREATE INDEX IF NOT EXISTS idx_recommendation_popular_snapshot_news_id
    ON public.recommendation_popular_snapshot (news_id);
