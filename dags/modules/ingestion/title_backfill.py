def run_title_backfill(
    db_config,
    limit=0,
    offset=0,
    days_back=30,
    dry_run=False,
    max_workers=8,
    timeout_sec=10,
):
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import psycopg2
    import requests
    from bs4 import BeautifulSoup

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')
    root_logger.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)

    def is_truncated(title):
        if not title:
            return False
        normalized = title.strip()
        return normalized.endswith("...") or normalized.endswith("…")

    def normalize_title(text):
        if not text:
            return ""
        return " ".join(str(text).split()).strip()

    def extract_title_from_meta(url, timeout):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        candidates = []
        node = soup.select_one("meta[property='og:title']")
        if node and node.get("content"):
            candidates.append(node.get("content"))

        node = soup.select_one("meta[name='twitter:title']")
        if node and node.get("content"):
            candidates.append(node.get("content"))

        node = soup.select_one("h2#title_area span")
        if node:
            candidates.append(node.get_text(" ", strip=True))

        if soup.title and soup.title.string:
            candidates.append(soup.title.string)

        for candidate in candidates:
            cleaned = normalize_title(candidate)
            if cleaned:
                return cleaned
        return ""

    def extract_title(url, timeout):
        # 안정성 우선: HTML meta/title 기반 추출만 사용
        return extract_title_from_meta(url, timeout)

    def should_update(old_title, new_title):
        if not new_title:
            return False
        old_clean = normalize_title(old_title)
        new_clean = normalize_title(new_title)
        if not new_clean or new_clean == old_clean:
            return False
        if is_truncated(new_clean):
            return False
        # 보수적으로: 기존이 truncation이고 새 값이 더 짧지 않을 때만 교체
        old_without_ellipsis = old_clean.rstrip(".…").strip()
        return len(new_clean) >= len(old_without_ellipsis)

    conn = psycopg2.connect(
        host=db_config.get("host"),
        port=db_config.get("port"),
        database=db_config.get("dbname"),
        user=db_config.get("user"),
        password=db_config.get("password"),
    )
    conn.autocommit = False

    summary = {
        "scanned": 0,
        "candidates": 0,
        "updated": 0,
        "unchanged": 0,
        "failed": 0,
        "dry_run": dry_run,
        "limit": limit,
        "offset": offset,
        "days_back": days_back,
    }

    try:
        sql = """
            SELECT news_id, url, title
            FROM naver_news
            WHERE (title LIKE %s OR title LIKE %s)
              AND crawl_status IS DISTINCT FROM 'url_filtered'
              AND api_request_date >= NOW() - make_interval(days => %s)
            ORDER BY api_request_date DESC, news_id DESC
        """
        params = ["%...", "%…", days_back]
        if limit and limit > 0:
            sql += "\nLIMIT %s OFFSET %s"
            params.extend([limit, offset])
        elif offset and offset > 0:
            sql += "\nOFFSET %s"
            params.append(offset)

        logger.info("Title backfill started: dry_run=%s, workers=%s", dry_run, max_workers)

        fetch_batch_size = 1000
        with conn.cursor(name="title_backfill_cursor") as read_cursor:
            read_cursor.itersize = fetch_batch_size
            read_cursor.execute(sql, tuple(params))

            def process_row(row):
                news_id, url, old_title = row
                try:
                    new_title = extract_title(url, timeout_sec)
                    return news_id, old_title, new_title, None
                except Exception as exc:
                    return news_id, old_title, "", str(exc)

            while True:
                rows = read_cursor.fetchmany(fetch_batch_size)
                if not rows:
                    break

                summary["scanned"] += len(rows)
                summary["candidates"] += len(rows)

                updates = []
                failures = 0
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(process_row, row) for row in rows]
                    for future in as_completed(futures):
                        news_id, old_title, new_title, error = future.result()
                        if error:
                            failures += 1
                            logger.warning("news_id=%s title extraction failed: %s", news_id, error)
                            continue

                        if should_update(old_title, new_title):
                            updates.append((new_title, news_id, old_title))
                        else:
                            summary["unchanged"] += 1

                summary["failed"] += failures

                if updates and not dry_run:
                    with conn.cursor() as write_cursor:
                        write_cursor.executemany(
                            """
                            UPDATE naver_news
                            SET title = %s
                            WHERE news_id = %s
                              AND title = %s
                            """,
                            updates,
                        )
                        summary["updated"] += write_cursor.rowcount
                    conn.commit()
                else:
                    summary["updated"] += len(updates)

        logger.info("Title backfill done: %s", summary)
        return summary
    except Exception:
        conn.rollback()
        logger.exception("Title backfill failed.")
        raise
    finally:
        conn.close()
