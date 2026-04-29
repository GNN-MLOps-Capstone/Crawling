from __future__ import annotations

from dags.modules.snapshot_utils import select_latest_snapshot_on_or_before
from dags.modules.window_utils import (
    build_incremental_key,
    build_window_context,
    build_window_label,
)


def test_select_latest_snapshot_on_or_before_filters_future_snapshot() -> None:
    keys = [
        "keyword_embeddings/date=20260308/keyword_embeddings.parquet",
        "keyword_embeddings/date=20260309/keyword_embeddings.parquet",
        "keyword_embeddings/date=20260310/keyword_embeddings.parquet",
    ]

    selected = select_latest_snapshot_on_or_before(
        keys,
        "2026-03-09",
        prefix="keyword_embeddings/date=",
        suffix="keyword_embeddings.parquet",
    )

    assert selected == "keyword_embeddings/date=20260309/keyword_embeddings.parquet"


def test_select_latest_snapshot_on_or_before_returns_none_when_no_match() -> None:
    keys = [
        "keyword_embeddings/date=20260310/keyword_embeddings.parquet",
    ]

    selected = select_latest_snapshot_on_or_before(
        keys,
        "2026-03-09",
        prefix="keyword_embeddings/date=",
        suffix="keyword_embeddings.parquet",
    )

    assert selected is None


def test_build_incremental_key_uses_window_partition_format() -> None:
    key = build_incremental_key(
        "refined_news_incremental",
        "2026-03-09 01:00:00+09:00",
        "2026-03-09 02:00:00+09:00",
    )

    assert (
        key
        == "refined_news_incremental/window_start=202603090100/window_end=202603090200/data.parquet"
    )


def test_build_window_context_returns_stable_label() -> None:
    context = build_window_context(
        "2026-03-09 01:00:00+09:00",
        "2026-03-09 02:00:00+09:00",
    )

    assert context["window_start"] == "2026-03-09 01:00:00"
    assert context["window_end"] == "2026-03-09 02:00:00"
    assert context["window_label"] == build_window_label(
        "2026-03-09 01:00:00+09:00",
        "2026-03-09 02:00:00+09:00",
    )
