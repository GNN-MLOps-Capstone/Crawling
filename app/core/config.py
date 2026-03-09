import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("RECO_APP_NAME", "recommend-api")
    app_version: str = os.getenv("RECO_APP_VERSION", "0.1.0")
    model_version: str = os.getenv("RECO_MODEL_VERSION", "v1")

    db_host: str = os.getenv("NEWS_DB_HOST", "localhost")
    db_port: int = int(os.getenv("NEWS_DB_PORT", "5432"))
    db_name: str = os.getenv("NEWS_DB_NAME", "postgres")
    db_user: str = os.getenv("NEWS_DB_USER", "postgres")
    db_password: str = os.getenv("NEWS_DB_PASSWORD", "postgres")

    default_cursor_version: int = int(os.getenv("RECO_CURSOR_VERSION", "1"))
    default_limit_max: int = int(os.getenv("RECO_LIMIT_MAX", "100"))
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_password: str | None = os.getenv("REDIS_PASSWORD")
    session_cache_ttl_seconds: int = int(os.getenv("RECO_SESSION_TTL_SECONDS", "1800"))
    base_pool_hours: int = int(os.getenv("RECO_BASE_POOL_HOURS", "72"))
    path_a_hours: int = int(os.getenv("RECO_PATH_A_HOURS", "72"))
    path_b_hours: int = int(os.getenv("RECO_PATH_B_HOURS", "2"))
    path_a_candidate_limit: int = int(os.getenv("RECO_PATH_A_LIMIT", "50"))
    path_b_candidate_limit: int = int(os.getenv("RECO_PATH_B_LIMIT", "30"))
    path_a_mix_weight: int = int(os.getenv("RECO_PATH_A_MIX_WEIGHT", "3"))
    path_b_mix_weight: int = int(os.getenv("RECO_PATH_B_MIX_WEIGHT", "1"))
    prefetch_path_a_low_watermark: int = int(os.getenv("RECO_PREFETCH_PATH_A_LOW_WATERMARK", "20"))
    prefetch_policy: str = os.getenv("RECO_PREFETCH_POLICY", "path_a_replenish_first")
    path_b_stale_cutoff_minutes: int = int(os.getenv("RECO_PATH_B_STALE_CUTOFF_MINUTES", "120"))
    blocked_domains: tuple[str, ...] = tuple(
        domain.strip()
        for domain in os.getenv("RECO_BLOCKED_DOMAINS", "").split(",")
        if domain.strip()
    )
    rerank_timeout_seconds: float = float(os.getenv("RECO_RERANK_TIMEOUT_SECONDS", "15"))
    rerank_provider: str = os.getenv("RECO_RERANK_PROVIDER", "heuristic")
    gemini_api_key: str | None = os.getenv("GEMINI_API_KEY")
    gemini_model: str = os.getenv("RECO_GEMINI_MODEL", "gemini-2.0-flash-lite")


settings = Settings()
