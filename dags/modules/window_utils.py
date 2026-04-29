from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
WINDOW_FORMAT = "%Y%m%d%H%M"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def to_kst(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    else:
        raise TypeError(f"unsupported datetime value: {type(value)}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def build_window_label(window_start, window_end) -> str:
    start = to_kst(window_start).strftime(WINDOW_FORMAT)
    end = to_kst(window_end).strftime(WINDOW_FORMAT)
    return f"{start}_{end}"


def build_incremental_key(base_prefix: str, window_start, window_end, filename: str = "data.parquet") -> str:
    start = to_kst(window_start).strftime(WINDOW_FORMAT)
    end = to_kst(window_end).strftime(WINDOW_FORMAT)
    return f"{base_prefix}/window_start={start}/window_end={end}/{filename}"


def build_window_context(window_start, window_end) -> dict:
    start = to_kst(window_start)
    end = to_kst(window_end)
    return {
        "window_start": start.strftime(DATETIME_FORMAT),
        "window_end": end.strftime(DATETIME_FORMAT),
        "window_label": build_window_label(start, end),
    }
