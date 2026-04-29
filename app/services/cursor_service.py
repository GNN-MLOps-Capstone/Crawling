import base64
import json
from dataclasses import dataclass


class CursorError(ValueError):
    pass


@dataclass(frozen=True)
class CursorPayload:
    v: int
    limit: int
    offset: int
    request_id: str


def encode_cursor(payload: CursorPayload) -> str:
    raw = json.dumps(
        {
            "v": payload.v,
            "limit": payload.limit,
            "offset": payload.offset,
            "request_id": payload.request_id,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def decode_cursor(cursor_token: str) -> CursorPayload:
    try:
        padded = cursor_token + "=" * (-len(cursor_token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("utf-8"))
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise CursorError("Invalid cursor format") from exc

    required = {"v", "limit", "offset", "request_id"}
    if not required.issubset(data.keys()):
        raise CursorError("Cursor missing required fields")

    try:
        payload = CursorPayload(
            v=int(data["v"]),
            limit=int(data["limit"]),
            offset=int(data["offset"]),
            request_id=str(data["request_id"]),
        )
    except Exception as exc:
        raise CursorError("Cursor field types are invalid") from exc

    if payload.offset < 0 or payload.limit <= 0:
        raise CursorError("Cursor values are invalid")

    return payload
