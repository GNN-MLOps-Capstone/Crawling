DO $$
BEGIN
    ALTER TYPE filter_status_enum ADD VALUE IF NOT EXISTS 'failed_retryable';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TYPE filter_status_enum ADD VALUE IF NOT EXISTS 'failed_permanent';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TYPE filter_status_enum ADD VALUE IF NOT EXISTS 'skipped';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE public.filtered_news
    ADD COLUMN IF NOT EXISTS summary TEXT,
    ADD COLUMN IF NOT EXISTS sentiment VARCHAR(20);

CREATE TABLE IF NOT EXISTS public.processed_content_hashes (
    content_hash VARCHAR(32) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_content_hashes_created_at
    ON public.processed_content_hashes(created_at);
