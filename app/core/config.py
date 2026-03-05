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


settings = Settings()
