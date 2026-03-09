from __future__ import annotations

import json
from dataclasses import dataclass

from app.core.config import settings
from app.repositories.news_repository import NewsCandidate
from app.services.context_builder import NormalizedRecommendContext


class ProviderTimeoutError(RuntimeError):
    pass


@dataclass(frozen=True)
class CandidateScore:
    news_id: int
    relevance: float
    novelty: float
    clarity: float
    final: float


@dataclass(frozen=True)
class RerankResult:
    candidates: list[NewsCandidate]
    fallback_used: bool = False
    fallback_reason: str | None = None


@dataclass(frozen=True)
class RerankService:
    def rerank(
        self,
        *,
        context: NormalizedRecommendContext,
        candidates: list[NewsCandidate],
    ) -> RerankResult:
        if not candidates:
            return RerankResult(candidates=[])

        prompt = self._build_prompt(context=context, candidates=candidates)

        try:
            scores = self._invoke_provider(prompt=prompt, candidates=candidates, context=context)
            ranked = self._apply_scores(candidates=candidates, scores=scores)
            return RerankResult(candidates=ranked)
        except ProviderTimeoutError:
            return RerankResult(
                candidates=candidates,
                fallback_used=True,
                fallback_reason="llm_timeout",
            )
        except ValueError as exc:
            return RerankResult(
                candidates=self._apply_partial_scores(
                    candidates=candidates,
                    raw_output=str(exc),
                    context=context,
                ),
                fallback_used=True,
                fallback_reason="llm_parse_failed",
            )
        except Exception:
            return RerankResult(
                candidates=candidates,
                fallback_used=True,
                fallback_reason="llm_rerank_failed",
            )

    def _invoke_provider(
        self,
        *,
        prompt: str,
        candidates: list[NewsCandidate],
        context: NormalizedRecommendContext,
    ) -> list[CandidateScore]:
        if settings.rerank_provider == "heuristic":
            raw_output = self._build_heuristic_response(candidates=candidates, context=context)
        elif settings.rerank_provider == "gemini":
            raw_output = self._invoke_gemini(prompt=prompt)
        else:
            raise ValueError("unsupported provider output")
        return self._parse_scores(raw_output)

    def _invoke_gemini(self, *, prompt: str) -> str:
        if not settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        from google import genai
        from google.genai import types

        client = genai.Client(api_key=settings.gemini_api_key)
        timeout_ms = int(settings.rerank_timeout_seconds * 1000)
        try:
            response = client.models.generate_content(
                model=settings.gemini_model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_json_schema=self._response_schema(),
                    http_options=types.HttpOptions(timeout=timeout_ms),
                ),
            )
        except Exception as exc:
            message = str(exc).lower()
            if "timeout" in message or "timed out" in message or "deadline" in message:
                raise ProviderTimeoutError(str(exc)) from exc
            raise
        return response.text or ""

    def _build_heuristic_response(
        self,
        *,
        candidates: list[NewsCandidate],
        context: NormalizedRecommendContext,
    ) -> str:
        return json.dumps(
            {
                "scores": [
                    {
                        "news_id": item.news_id,
                        "relevance": self._base_relevance(item=item, context=context),
                        "novelty": 0.2 if item.domain != "unknown" else 0.1,
                        "clarity": min(len(item.title.strip()), 120) / 120 if item.title else 0.2,
                    }
                    for item in candidates
                ]
            }
        )

    def _build_prompt(
        self,
        *,
        context: NormalizedRecommendContext,
        candidates: list[NewsCandidate],
    ) -> str:
        prompt_defaults = {
            "user_interests": "No explicit interests provided. Prefer broadly useful finance news.",
            "recent_reads": "No recent reading history provided.",
            "avoidances": "No avoidances provided.",
        }
        prompt_defaults.update(context.prompt_defaults)
        return json.dumps(
            {
                "task": (
                    "Score finance news candidates for a general recommendation feed. "
                    "Return strict JSON with scores[]."
                ),
                "user_interests": context.profile.get("interests") or prompt_defaults["user_interests"],
                "recent_reads": context.recent_actions[:5] or prompt_defaults["recent_reads"],
                "avoidances": context.profile.get("avoidances") or prompt_defaults["avoidances"],
                "candidates": [
                    {
                        "news_id": item.news_id,
                        "title": item.title,
                        "domain": item.domain,
                        "scoring_guide": {
                            "relevance": "How broadly useful this finance news is for the user/feed.",
                            "novelty": "How distinct or fresh the item feels compared with a generic latest feed.",
                            "clarity": "How understandable and headline-clear the item is.",
                            "final": "Overall score combining relevance, novelty, and clarity.",
                        },
                    }
                    for item in candidates
                ],
            },
            ensure_ascii=False,
        )

    @staticmethod
    def _response_schema() -> dict[str, object]:
        return {
            "type": "object",
            "properties": {
                "scores": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "news_id": {"type": "integer"},
                            "relevance": {"type": "number"},
                            "novelty": {"type": "number"},
                            "clarity": {"type": "number"},
                            "final": {"type": "number"},
                        },
                        "required": ["news_id", "relevance", "novelty", "clarity", "final"],
                    },
                }
            },
            "required": ["scores"],
        }

    @staticmethod
    def _parse_scores(raw_output: str) -> list[CandidateScore]:
        data = json.loads(raw_output)
        score_items = data.get("scores")
        if not isinstance(score_items, list):
            raise ValueError(raw_output)

        parsed: list[CandidateScore] = []
        for item in score_items:
            try:
                relevance = float(item["relevance"])
                novelty = float(item["novelty"])
                clarity = float(item["clarity"])
                final = float(item.get("final", relevance + novelty + clarity))
                parsed.append(
                    CandidateScore(
                        news_id=int(item["news_id"]),
                        relevance=relevance,
                        novelty=novelty,
                        clarity=clarity,
                        final=final,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        if not parsed:
            raise ValueError(raw_output)
        return parsed

    def _apply_scores(
        self,
        *,
        candidates: list[NewsCandidate],
        scores: list[CandidateScore],
    ) -> list[NewsCandidate]:
        score_map = {score.news_id: score.final for score in scores}
        if len(score_map) < len(candidates):
            return self._apply_partial_scores(
                candidates=candidates,
                raw_output="partial",
                context=NormalizedRecommendContext(),
                score_map=score_map,
            )
        return sorted(
            candidates,
            key=lambda item: (
                score_map.get(item.news_id, 0.0),
                item.pub_date.timestamp() if item.pub_date else 0.0,
                item.news_id,
            ),
            reverse=True,
        )

    def _apply_partial_scores(
        self,
        *,
        candidates: list[NewsCandidate],
        raw_output: str,
        context: NormalizedRecommendContext,
        score_map: dict[int, float] | None = None,
    ) -> list[NewsCandidate]:
        del raw_output
        base_scores = score_map or {}
        return sorted(
            candidates,
            key=lambda item: (
                base_scores.get(item.news_id, self._score_candidate(item=item, context=context)),
                item.pub_date.timestamp() if item.pub_date else 0.0,
                item.news_id,
            ),
            reverse=True,
        )

    def _score_candidate(self, *, item: NewsCandidate, context: NormalizedRecommendContext) -> float:
        relevance = self._base_relevance(item=item, context=context)
        novelty = 0.2 if item.domain != "unknown" else 0.1
        clarity = min(len(item.title.strip()), 120) / 120 if item.title else 0.2
        return relevance + novelty + clarity

    @staticmethod
    def _base_relevance(*, item: NewsCandidate, context: NormalizedRecommendContext) -> float:
        relevance = 1.0
        profile_bonus = 0.1 if context.is_personalizable else 0.0
        recent_bonus = 0.05 if context.recent_actions else 0.0
        return relevance + profile_bonus + recent_bonus
