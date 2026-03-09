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
    onboarding_hours: int = int(os.getenv("RECO_ONBOARDING_HOURS", os.getenv("RECO_PATH_A_HOURS", "72")))
    behavior_hours: int = int(os.getenv("RECO_BEHAVIOR_HOURS", "72"))
    breaking_hours: int = int(os.getenv("RECO_BREAKING_HOURS", os.getenv("RECO_PATH_B_HOURS", "2")))
    onboarding_candidate_limit: int = int(os.getenv("RECO_ONBOARDING_LIMIT", os.getenv("RECO_PATH_A_LIMIT", "50")))
    behavior_candidate_limit: int = int(os.getenv("RECO_BEHAVIOR_LIMIT", "50"))
    breaking_candidate_limit: int = int(os.getenv("RECO_BREAKING_LIMIT", os.getenv("RECO_PATH_B_LIMIT", "30")))
    behavior_action_limit: int = int(os.getenv("RECO_BEHAVIOR_ACTION_LIMIT", "20"))
    valid_read_dwell_seconds: int = int(os.getenv("RECO_VALID_READ_DWELL_SECONDS", "10"))
    retrieval_candidate_pool_multiplier: int = int(os.getenv("RECO_RETRIEVAL_CANDIDATE_POOL_MULTIPLIER", "4"))
    similarity_weight_keyword_keyword: float = float(os.getenv("RECO_SIM_WEIGHT_KEYWORD_KEYWORD", "1.0"))
    similarity_weight_stock_stock: float = float(os.getenv("RECO_SIM_WEIGHT_STOCK_STOCK", "1.15"))
    similarity_weight_keyword_stock: float = float(os.getenv("RECO_SIM_WEIGHT_KEYWORD_STOCK", "0.9"))
    onboarding_stock_match_boost: float = float(os.getenv("RECO_ONBOARDING_STOCK_MATCH_BOOST", "0.35"))
    behavior_repeat_stock_match_boost: float = float(os.getenv("RECO_BEHAVIOR_REPEAT_STOCK_MATCH_BOOST", "0.2"))
    behavior_decay_half_life_hours: float = float(os.getenv("RECO_BEHAVIOR_DECAY_HALF_LIFE_HOURS", "24"))
    behavior_min_recent_actions: int = int(os.getenv("RECO_BEHAVIOR_MIN_RECENT_ACTIONS", "2"))
    behavior_min_unique_entities: int = int(os.getenv("RECO_BEHAVIOR_MIN_UNIQUE_ENTITIES", "2"))
    behavior_min_candidate_score: float = float(os.getenv("RECO_BEHAVIOR_MIN_CANDIDATE_SCORE", "0.05"))
    onboarding_mix_weight: int = int(os.getenv("RECO_ONBOARDING_MIX_WEIGHT", "2"))
    behavior_mix_weight: int = int(os.getenv("RECO_BEHAVIOR_MIX_WEIGHT", "2"))
    breaking_mix_weight: int = int(os.getenv("RECO_BREAKING_MIX_WEIGHT", os.getenv("RECO_PATH_B_MIX_WEIGHT", "1")))
    prefetch_primary_low_watermark: int = int(
        os.getenv("RECO_PREFETCH_PRIMARY_LOW_WATERMARK", os.getenv("RECO_PREFETCH_PATH_A_LOW_WATERMARK", "20"))
    )
    prefetch_policy: str = os.getenv("RECO_PREFETCH_POLICY", "primary_replenish_first")
    breaking_stale_cutoff_minutes: int = int(
        os.getenv("RECO_BREAKING_STALE_CUTOFF_MINUTES", os.getenv("RECO_PATH_B_STALE_CUTOFF_MINUTES", "120"))
    )
    blocked_domains: tuple[str, ...] = tuple(
        domain.strip()
        for domain in os.getenv("RECO_BLOCKED_DOMAINS", "").split(",")
        if domain.strip()
    )


settings = Settings()
