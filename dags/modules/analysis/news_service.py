import hashlib
import io

import boto3
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from psycopg2.extras import execute_values

from modules.ingestion.reader import read_news_by_time_window

from modules.analysis.news_embedding import add_embeddings_to_df
from modules.analysis.preprocessor import NewsPreProcessor


DEFAULT_FILTER_VERSION = "analysis_integration_v2"
DEFAULT_CONFIG_PATH = "/opt/airflow/dags/config/analysis_config.yaml"
VALID_FILTER_STATUSES = {
    "pending",
    "processing",
    "passed",
    "filtered_out",
    "failed_retryable",
    "failed_permanent",
    "skipped",
}


def _generate_content_hash(text):
    if not isinstance(text, str):
        return ""
    clean_content = text.replace(" ", "").replace("\n", "").replace("\r", "").replace("\t", "")
    return hashlib.md5(clean_content.encode("utf-8")).hexdigest()


def _load_filter_version(config_path: str):
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
        return config.get("pipeline", {}).get("filter_version", DEFAULT_FILTER_VERSION)
    except Exception:
        return DEFAULT_FILTER_VERSION


def _update_filter_status(db_info: dict, news_ids, status: str, filter_version: str, reason=None, allowed_from=None):
    if status not in VALID_FILTER_STATUSES:
        raise ValueError(f"invalid filter_status: {status}")
    if not news_ids:
        return 0

    ids = sorted({int(nid) for nid in news_ids if pd.notna(nid)})
    if not ids:
        return 0

    sql = """
        UPDATE public.crawled_news
        SET filter_status = %s::filter_status_enum,
            filter_version = %s,
            filtered_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            filter_reason = %s
        WHERE news_id = ANY(%s)
    """
    params = [status, filter_version, reason, ids]

    if allowed_from:
        sql += " AND filter_status::text = ANY(%s)"
        params.append(list(allowed_from))

    with psycopg2.connect(**db_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            affected = cur.rowcount
        conn.commit()

    return affected


def _mark_failed_for_remaining(db_info: dict, remaining_ids, exc: Exception, filter_version: str):
    if not remaining_ids:
        return 0

    error_text = str(exc).lower()
    permanent_error_markers = (
        "invalid input syntax",
        "undefined column",
        "does not exist",
        "enum",
        "schema",
    )
    failed_status = "failed_permanent" if any(t in error_text for t in permanent_error_markers) else "failed_retryable"
    reason = str(exc)[:100] if str(exc) else "refinement_error"
    return _update_filter_status(
        db_info,
        remaining_ids,
        failed_status,
        filter_version=filter_version,
        reason=reason,
    )


def _load_input_df_for_refinement(
    aws_info: dict,
    db_info: dict,
    window_start: str | None = None,
    window_end: str | None = None,
    input_bucket: str | None = None,
    input_key: str | None = None,
):
    if input_bucket and input_key:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_info["access_key"],
            aws_secret_access_key=aws_info["secret_key"],
            endpoint_url=aws_info["endpoint_url"],
        )
        response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
        return pd.read_parquet(io.BytesIO(response["Body"].read()))

    if window_start and window_end:
        return read_news_by_time_window(window_start, window_end, db_info)

    raise ValueError("either input_bucket/input_key or window_start/window_end must be provided")


def _run_refinement_for_df(
    df: pd.DataFrame,
    output_key: str,
    aws_info: dict,
    db_info: dict,
    config_path: str,
    target_news_ids: list[int] | None = None,
):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_info["access_key"],
        aws_secret_access_key=aws_info["secret_key"],
        endpoint_url=aws_info["endpoint_url"],
    )

    ollama_host = "http://ollama:11434"
    filter_version = _load_filter_version(config_path)
    print(f"🧾 Using filter_version: {filter_version}")

    if df.empty:
        print("⚠️ No input rows for refinement.")
        return None

    if target_news_ids:
        target_ids = {int(nid) for nid in target_news_ids}
        df = df[df["news_id"].isin(target_ids)].copy()
        print(f"🎯 Filtered target rows for refinement: {len(df)}")

    if df.empty:
        print("⚠️ No rows left after target filtering.")
        return None

    processing_ids = set()

    try:
        all_news_ids = set(df["news_id"].dropna().astype("int64").tolist())
        processing_ids = set(all_news_ids)
        moved = _update_filter_status(
            db_info,
            processing_ids,
            "processing",
            filter_version=filter_version,
            reason="refinement_started",
            allowed_from=("pending", "failed_retryable", "processing"),
        )
        print(f"  - Marked processing: {moved}")
        print(f"  - Raw count: {len(df)}")

        before_text_ids = set(df["news_id"].dropna().astype("int64").tolist())
        df = df.dropna(subset=["text"]).copy()
        after_text_ids = set(df["news_id"].dropna().astype("int64").tolist())
        empty_text_ids = before_text_ids - after_text_ids
        if empty_text_ids:
            _update_filter_status(
                db_info,
                empty_text_ids,
                "filtered_out",
                filter_version=filter_version,
                reason="empty_text",
            )
            processing_ids -= empty_text_ids

        df["content_hash"] = df["text"].apply(_generate_content_hash)

        before_dedup_ids = set(df["news_id"].dropna().astype("int64").tolist())
        df = df.drop_duplicates(subset=["content_hash"], keep="first").copy()
        after_dedup_ids = set(df["news_id"].dropna().astype("int64").tolist())
        duplicate_in_batch_ids = before_dedup_ids - after_dedup_ids
        if duplicate_in_batch_ids:
            _update_filter_status(
                db_info,
                duplicate_in_batch_ids,
                "skipped",
                filter_version=filter_version,
                reason="duplicate_in_batch",
            )
            processing_ids -= duplicate_in_batch_ids

        if df.empty:
            print("  💤 All rows filtered before DB hash check.")
            return None

        unique_hashes = df["content_hash"].dropna().unique().tolist()
        target_hashes = set(unique_hashes)

        with psycopg2.connect(**db_info) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM processed_content_hashes")
                total_rows = cur.fetchone()[0]
                print(f"  👀 [Debug] Total rows in DB table: {total_rows}")

                if unique_hashes:
                    cur.execute(
                        "SELECT content_hash FROM processed_content_hashes WHERE content_hash IN %s",
                        (tuple(unique_hashes),),
                    )
                    existing_hashes = {row[0] for row in cur.fetchall()}
                else:
                    existing_hashes = set()

                print(f"  - Found {len(existing_hashes)} duplicates in DB.")
                new_hashes_to_process = target_hashes - existing_hashes

                if new_hashes_to_process:
                    insert_query = """
                        INSERT INTO processed_content_hashes (content_hash)
                        VALUES %s
                        ON CONFLICT (content_hash) DO NOTHING
                    """
                    execute_values(cur, insert_query, [(h,) for h in new_hashes_to_process])
                    print(f"  - Registered {len(new_hashes_to_process)} new hashes to DB.")

            conn.commit()

        duplicate_processed_ids = set(
            df[df["content_hash"].isin(existing_hashes)]["news_id"].dropna().astype("int64").tolist()
        )
        if duplicate_processed_ids:
            _update_filter_status(
                db_info,
                duplicate_processed_ids,
                "skipped",
                filter_version=filter_version,
                reason="duplicate_processed",
            )
            processing_ids -= duplicate_processed_ids

        initial_count = len(df)
        df = df[df["content_hash"].isin(new_hashes_to_process)].copy()
        dropped_count = initial_count - len(df)
        if dropped_count > 0:
            print(f"  🔥 {dropped_count} duplicates skipped (Already processed).")

        if df.empty:
            print("  💤 All data in this batch has been processed before. Skipping.")
            return None

        processor = NewsPreProcessor()
        df["refined_text"] = df["text"].apply(processor.clean_text_basic)
        df["refined_text"] = df["refined_text"].apply(processor.is_english_only)

        mask_sports = df["refined_text"].apply(processor.is_sports_news)
        mask_short = df["refined_text"].str.len() <= 20
        mask_filtered = mask_sports | mask_short
        filtered_out_ids = set(df[mask_filtered]["news_id"].dropna().astype("int64").tolist())
        if filtered_out_ids:
            _update_filter_status(
                db_info,
                filtered_out_ids,
                "filtered_out",
                filter_version=filter_version,
                reason="sports_or_too_short",
            )
            processing_ids -= filtered_out_ids

        df_final = df[~mask_filtered].copy()
        if df_final.empty:
            print("  ⚠️ No valid data after filtering.")
            return None

        df_embedded = add_embeddings_to_df(df_final, model_name="bge-m3", host=ollama_host)

        out_cols = ["news_id", "refined_text", "news_embedding", "pub_date"]
        df_save = df_embedded[out_cols].copy()
        df_save["news_id"] = df_save["news_id"].astype("int64")
        df_save["pub_date"] = df_save["pub_date"].astype(str)

        arrow_schema = pa.schema(
            [
                ("news_id", pa.int64()),
                ("refined_text", pa.string()),
                ("news_embedding", pa.list_(pa.float32())),
                ("pub_date", pa.string()),
            ]
        )

        out_buf = io.BytesIO()
        table = pa.Table.from_pandas(df_save, schema=arrow_schema)
        pq.write_table(table, out_buf, compression="SNAPPY")

        s3_client.put_object(Bucket="silver", Key=output_key, Body=out_buf.getvalue())
        print(f"✅ Saved: {output_key} ({len(df_save)} rows)")

        passed_ids = set(df_save["news_id"].dropna().astype("int64").tolist())
        if passed_ids:
            _update_filter_status(db_info, passed_ids, "passed", filter_version=filter_version, reason=None)
            processing_ids -= passed_ids

        if processing_ids:
            _update_filter_status(
                db_info,
                processing_ids,
                "skipped",
                filter_version=filter_version,
                reason="not_selected_for_refinement",
            )

        return output_key
    except Exception as e:
        print(f"❌ Error during refinement: {e}")
        _mark_failed_for_remaining(db_info, processing_ids, e, filter_version=filter_version)
        raise


def run_refinement_process(updated_dates: list, aws_info: dict, db_info: dict, config_path: str = DEFAULT_CONFIG_PATH):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_info["access_key"],
        aws_secret_access_key=aws_info["secret_key"],
        endpoint_url=aws_info["endpoint_url"],
    )

    ollama_host = "http://ollama:11434"
    filter_version = _load_filter_version(config_path)
    print(f"🧾 Using filter_version: {filter_version}")
    processed_files = []

    for date_str in updated_dates:
        print(f"🔎 Processing date: {date_str}")
        if not date_str or "-" not in date_str:
            continue

        y, m, d = date_str.split("-")
        input_key = f"crawled_news/year={y}/month={m}/day={d}/data.parquet"
        output_key = f"refined_news/year={y}/month={m}/day={d}/data.parquet"
        processing_ids = set()

        try:
            try:
                response = s3_client.get_object(Bucket="bronze", Key=input_key)
                df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            except s3_client.exceptions.NoSuchKey:
                print(f"⚠️ Input data not found: {input_key}")
                continue

            if df.empty:
                continue

            all_news_ids = set(df["news_id"].dropna().astype("int64").tolist())
            processing_ids = set(all_news_ids)
            moved = _update_filter_status(
                db_info,
                processing_ids,
                "processing",
                filter_version=filter_version,
                reason="refinement_started",
                allowed_from=("pending", "failed_retryable", "processing"),
            )
            print(f"  - Marked processing: {moved}")
            print(f"  - Raw count: {len(df)}")

            before_text_ids = set(df["news_id"].dropna().astype("int64").tolist())
            df = df.dropna(subset=["text"]).copy()
            after_text_ids = set(df["news_id"].dropna().astype("int64").tolist())
            empty_text_ids = before_text_ids - after_text_ids
            if empty_text_ids:
                _update_filter_status(
                    db_info,
                    empty_text_ids,
                    "filtered_out",
                    filter_version=filter_version,
                    reason="empty_text",
                )
                processing_ids -= empty_text_ids

            df["content_hash"] = df["text"].apply(_generate_content_hash)

            before_dedup_ids = set(df["news_id"].dropna().astype("int64").tolist())
            df = df.drop_duplicates(subset=["content_hash"], keep="first").copy()
            after_dedup_ids = set(df["news_id"].dropna().astype("int64").tolist())
            duplicate_in_batch_ids = before_dedup_ids - after_dedup_ids
            if duplicate_in_batch_ids:
                _update_filter_status(
                    db_info,
                    duplicate_in_batch_ids,
                    "skipped",
                    filter_version=filter_version,
                    reason="duplicate_in_batch",
                )
                processing_ids -= duplicate_in_batch_ids

            if df.empty:
                print("  💤 All rows filtered before DB hash check.")
                continue

            unique_hashes = df["content_hash"].dropna().unique().tolist()
            target_hashes = set(unique_hashes)

            with psycopg2.connect(**db_info) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM processed_content_hashes")
                    total_rows = cur.fetchone()[0]
                    print(f"  👀 [Debug] Total rows in DB table: {total_rows}")

                    if unique_hashes:
                        cur.execute(
                            "SELECT content_hash FROM processed_content_hashes WHERE content_hash IN %s",
                            (tuple(unique_hashes),),
                        )
                        existing_hashes = {row[0] for row in cur.fetchall()}
                    else:
                        existing_hashes = set()

                    print(f"  - Found {len(existing_hashes)} duplicates in DB.")
                    new_hashes_to_process = target_hashes - existing_hashes

                    if new_hashes_to_process:
                        insert_query = """
                            INSERT INTO processed_content_hashes (content_hash)
                            VALUES %s
                            ON CONFLICT (content_hash) DO NOTHING
                        """
                        execute_values(cur, insert_query, [(h,) for h in new_hashes_to_process])
                        print(f"  - Registered {len(new_hashes_to_process)} new hashes to DB.")

                conn.commit()

            duplicate_processed_ids = set(
                df[df["content_hash"].isin(existing_hashes)]["news_id"].dropna().astype("int64").tolist()
            )
            if duplicate_processed_ids:
                _update_filter_status(
                    db_info,
                    duplicate_processed_ids,
                    "skipped",
                    filter_version=filter_version,
                    reason="duplicate_processed",
                )
                processing_ids -= duplicate_processed_ids

            initial_count = len(df)
            df = df[df["content_hash"].isin(new_hashes_to_process)].copy()
            dropped_count = initial_count - len(df)
            if dropped_count > 0:
                print(f"  🔥 {dropped_count} duplicates skipped (Already processed).")

            if df.empty:
                print("  💤 All data in this batch has been processed before. Skipping.")
                continue

            processor = NewsPreProcessor()
            df["refined_text"] = df["text"].apply(processor.clean_text_basic)
            df["refined_text"] = df["refined_text"].apply(processor.is_english_only)

            mask_sports = df["refined_text"].apply(processor.is_sports_news)
            mask_short = df["refined_text"].str.len() <= 20
            mask_filtered = mask_sports | mask_short
            filtered_out_ids = set(df[mask_filtered]["news_id"].dropna().astype("int64").tolist())
            if filtered_out_ids:
                _update_filter_status(
                    db_info,
                    filtered_out_ids,
                    "filtered_out",
                    filter_version=filter_version,
                    reason="sports_or_too_short",
                )
                processing_ids -= filtered_out_ids

            df_final = df[~mask_filtered].copy()
            if df_final.empty:
                print("  ⚠️ No valid data after filtering.")
                continue

            df_embedded = add_embeddings_to_df(df_final, model_name="bge-m3", host=ollama_host)

            out_cols = ["news_id", "refined_text", "news_embedding", "pub_date"]
            df_save = df_embedded[out_cols].copy()
            df_save["news_id"] = df_save["news_id"].astype("int64")
            df_save["pub_date"] = df_save["pub_date"].astype(str)

            arrow_schema = pa.schema(
                [
                    ("news_id", pa.int64()),
                    ("refined_text", pa.string()),
                    ("news_embedding", pa.list_(pa.float32())),
                    ("pub_date", pa.string()),
                ]
            )

            out_buf = io.BytesIO()
            table = pa.Table.from_pandas(df_save, schema=arrow_schema)
            pq.write_table(table, out_buf, compression="SNAPPY")

            s3_client.put_object(Bucket="silver", Key=output_key, Body=out_buf.getvalue())
            print(f"✅ Saved: {output_key} ({len(df_save)} rows)")

            passed_ids = set(df_save["news_id"].dropna().astype("int64").tolist())
            if passed_ids:
                _update_filter_status(db_info, passed_ids, "passed", filter_version=filter_version, reason=None)
                processing_ids -= passed_ids

            if processing_ids:
                _update_filter_status(
                    db_info,
                    processing_ids,
                    "skipped",
                    filter_version=filter_version,
                    reason="not_selected_for_refinement",
                )
                processing_ids.clear()

            processed_files.append(output_key)

        except Exception as e:
            print(f"❌ Error processing {date_str}: {e}")
            _mark_failed_for_remaining(db_info, processing_ids, e, filter_version=filter_version)
            raise

    return processed_files


def run_refinement_process_for_window(
    window_start: str,
    window_end: str,
    aws_info: dict,
    db_info: dict,
    output_key: str,
    config_path: str = DEFAULT_CONFIG_PATH,
    input_bucket: str | None = None,
    input_key: str | None = None,
    target_news_ids: list[int] | None = None,
):
    df = _load_input_df_for_refinement(
        aws_info=aws_info,
        db_info=db_info,
        window_start=window_start,
        window_end=window_end,
        input_bucket=input_bucket,
        input_key=input_key,
    )
    return _run_refinement_for_df(
        df=df,
        output_key=output_key,
        aws_info=aws_info,
        db_info=db_info,
        config_path=config_path,
        target_news_ids=target_news_ids,
    )
