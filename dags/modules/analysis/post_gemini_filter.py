import io

import boto3
import pandas as pd
import psycopg2
import yaml


DEFAULT_FILTER_VERSION = "analysis_integration_v2"
DEFAULT_FILTER_REASON = "post_gemini_unmapped_keyword_rule"


def _load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def _count_term_matches(text: str, terms: list[str]) -> int:
    if not isinstance(text, str) or not text.strip() or not terms:
        return 0

    normalized = text.lower()
    return sum(1 for term in terms if isinstance(term, str) and term.strip() and term.lower() in normalized)


def _read_parquet_from_s3(s3_client, bucket: str, key: str | None) -> pd.DataFrame:
    if not key:
        return pd.DataFrame()

    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        print(f"⚠️ File not found or unreadable: {key}", flush=True)
        return pd.DataFrame()


def _write_parquet_to_s3(s3_client, bucket: str, key: str, df: pd.DataFrame):
    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buf.getvalue())
    print(f"💾 Saved filtered parquet: {key} ({len(df)} rows)", flush=True)


def _update_filter_status(db_info: dict, news_ids: set[int], filter_version: str, filter_reason: str):
    if not db_info or not news_ids:
        return 0

    sql = """
        UPDATE public.crawled_news
        SET filter_status = 'filtered_out'::filter_status_enum,
            filter_version = %s,
            filtered_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            filter_reason = %s
        WHERE news_id = ANY(%s)
    """
    ids = sorted(int(news_id) for news_id in news_ids)

    with psycopg2.connect(**db_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (filter_version, filter_reason, ids))
            affected = cur.rowcount
        conn.commit()

    print(f"📝 Updated filter_status to filtered_out: {affected}", flush=True)
    return affected


def run_post_gemini_filter_for_incremental(
    incremental_paths: dict,
    aws_info: dict,
    db_info: dict,
    bucket: str,
    config_path: str,
):
    config = _load_config(config_path)
    pipeline_conf = config.get("pipeline", {})
    filter_conf = config.get("post_gemini_filter", {})

    if not filter_conf.get("enabled", True):
        print("ℹ️ post_gemini_filter is disabled. Skipping.", flush=True)
        return incremental_paths

    include_terms = filter_conf.get("include_terms") or []
    exclude_terms = filter_conf.get("exclude_terms") or []
    min_include_matches = int(filter_conf.get("min_include_matches", 3))
    max_exclude_matches = int(filter_conf.get("max_exclude_matches", 0))
    filter_version = pipeline_conf.get("filter_version", DEFAULT_FILTER_VERSION)
    filter_reason = filter_conf.get("filter_reason", DEFAULT_FILTER_REASON)

    if not include_terms:
        print("ℹ️ include_terms is empty. Skipping post-gemini filter.", flush=True)
        return incremental_paths

    refined_key = incremental_paths.get("refined")
    if not refined_key:
        print("ℹ️ No refined incremental parquet. Skipping post-gemini filter.", flush=True)
        return incremental_paths

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_info["access_key"],
        aws_secret_access_key=aws_info["secret_key"],
        endpoint_url=aws_info["endpoint_url"],
    )

    refined_df = _read_parquet_from_s3(s3, bucket, refined_key)
    if refined_df.empty:
        print("ℹ️ Refined parquet is empty. Skipping post-gemini filter.", flush=True)
        return incremental_paths

    stocks_df = _read_parquet_from_s3(s3, bucket, incremental_paths.get("stocks"))
    mapped_news_ids = set()
    if not stocks_df.empty and "news_id" in stocks_df.columns:
        mapped_news_ids = set(stocks_df["news_id"].dropna().astype("int64").tolist())

    refined_df["news_id"] = refined_df["news_id"].astype("int64")
    unmapped_df = refined_df[~refined_df["news_id"].isin(mapped_news_ids)].copy()

    if unmapped_df.empty:
        print("ℹ️ No unmapped news found after gemini analysis.", flush=True)
        return incremental_paths

    unmapped_df["include_match_count"] = unmapped_df["refined_text"].apply(
        lambda text: _count_term_matches(text, include_terms)
    )
    unmapped_df["exclude_match_count"] = unmapped_df["refined_text"].apply(
        lambda text: _count_term_matches(text, exclude_terms)
    )

    passed_unmapped_df = unmapped_df[
        (unmapped_df["include_match_count"] >= min_include_matches)
        & (unmapped_df["exclude_match_count"] <= max_exclude_matches)
    ].copy()

    kept_news_ids = mapped_news_ids | set(passed_unmapped_df["news_id"].dropna().astype("int64").tolist())
    all_news_ids = set(refined_df["news_id"].dropna().astype("int64").tolist())
    filtered_out_ids = all_news_ids - kept_news_ids

    print(
        "🧪 Post-gemini filter summary: "
        f"total={len(refined_df)}, mapped={len(mapped_news_ids)}, "
        f"unmapped={len(unmapped_df)}, passed_unmapped={len(passed_unmapped_df)}, "
        f"filtered_out={len(filtered_out_ids)}",
        flush=True,
    )

    datasets = {
        "refined": refined_df,
        "stocks": stocks_df,
        "keywords": _read_parquet_from_s3(s3, bucket, incremental_paths.get("keywords")),
        "analysis": _read_parquet_from_s3(s3, bucket, incremental_paths.get("analysis")),
    }

    for name, df in datasets.items():
        key = incremental_paths.get(name)
        if not key or df.empty or "news_id" not in df.columns:
            continue

        filtered_df = df[df["news_id"].astype("int64").isin(kept_news_ids)].copy()
        _write_parquet_to_s3(s3, bucket, key, filtered_df)

    _update_filter_status(db_info, filtered_out_ids, filter_version, filter_reason)
    return incremental_paths
