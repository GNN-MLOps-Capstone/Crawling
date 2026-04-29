import pendulum


def extract_snapshot_date(key: str, prefix: str) -> str | None:
    if not key or prefix not in key:
        return None

    try:
        return key.split(prefix, 1)[1].split("/", 1)[0]
    except Exception:
        return None


def select_latest_snapshot_on_or_before(keys: list[str], target_date: str, prefix: str, suffix: str) -> str | None:
    target_nodash = pendulum.parse(target_date).format("YYYYMMDD")
    candidates = []

    for key in keys or []:
        if not key.endswith(suffix):
            continue
        snapshot_date = extract_snapshot_date(key, prefix)
        if snapshot_date and snapshot_date <= target_nodash:
            candidates.append(key)

    return max(candidates) if candidates else None
