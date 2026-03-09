from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.core.config import settings


class RecommendNewsRequest(BaseModel):
    user_id: str = Field(min_length=1)
    limit: int = Field(ge=1, le=settings.default_limit_max)
    cursor: str | None = None
    request_id: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class RecommendNewsItem(BaseModel):
    news_id: int


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
