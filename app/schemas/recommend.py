from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.config import settings


class RecommendDebugContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1, ge=1)
    profile: dict[str, Any] | None = None
    recent_actions: list[dict[str, Any]] | None = None
    session_signals: dict[str, Any] | None = None


class RecommendNewsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: int
    limit: int = Field(ge=1, le=settings.default_limit_max)
    cursor: str | None = None
    request_id: str | None = None
    # Debug-only override. Normal recommendation flow loads user signals internally by user_id.
    context: RecommendDebugContext = Field(default_factory=RecommendDebugContext)


class RecommendNewsItem(BaseModel):
    news_id: int
    path: str


class RecommendNewsMeta(BaseModel):
    source: str
    fallback_used: bool
    fallback_reason: str | None = None


class RecommendNewsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    items: list[RecommendNewsItem]
    next_cursor: str | None
    meta: RecommendNewsMeta
