# collect_naver_news.py


def run_collect_naver_news():
    import requests
    import psycopg2
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.hooks.base import BaseHook
    from datetime import datetime
    import re
    from email.utils import parsedate_to_datetime
    import time

    logger = logging.getLogger(__name__)

    class NaverNewsCollector:
        def __init__(self):
            pg_hook = PostgresHook(postgres_conn_id='news_data_db')
            conn_obj = pg_hook.get_connection(conn_id='news_data_db')

            api_conn_obj = BaseHook.get_connection(conn_id='naver_api')

            self.client_id = api_conn_obj.extra_dejson.get('naver_client_id')
            self.client_secret = api_conn_obj.extra_dejson.get('naver_client_secret')
            self.api_url = "https://openapi.naver.com/v1/search/news.json"

            try:
                self.conn = psycopg2.connect(
                    host=conn_obj.host,
                    port=conn_obj.port,
                    database=conn_obj.schema,
                    user=conn_obj.login,
                    password=conn_obj.get_password()
                )
            except psycopg2.OperationalError as e:
                logger.error(f"데이터베이스 연결 실패: {e}")
                raise

            # 세션 내 중복 체크용
            self.collected_urls = set()

        def __del__(self):
            if hasattr(self, 'conn'):
                self.conn.close()

        def get_existing_urls(self):
            """DB에서 기존 URL 목록 가져오기"""
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT url FROM naver_news 
                    WHERE api_request_date > CURRENT_TIMESTAMP - INTERVAL '3 days'
                """)
                return {row[0] for row in cursor.fetchall()}

        def search_news(self, query, display=100, start=1):
            """네이버 뉴스 API 호출"""
            headers = {
                'X-Naver-Client-Id': self.client_id,
                'X-Naver-Client-Secret': self.client_secret
            }
            params = {
                'query': query,
                'display': display,
                'start': start,
                'sort': 'date'
            }
            try:
                response = requests.get(self.api_url, headers=headers, params=params)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"API 호출 실패: {e}")
                return None

        def parse_item(self, item, search_keyword):
            """뉴스 아이템 파싱"""
            title = re.sub('<.*?>', '', item['title'])  # HTML 태그 제거
            pub_date = parsedate_to_datetime(item['pubDate'])
            return {
                'title': title,
                'pub_date': pub_date,
                'url': item['link'],
                'search_keyword': search_keyword,
                'api_request_date': datetime.now()
            }

        def insert_news(self, news_items):
            """뉴스 데이터 DB 삽입"""
            if not news_items:
                return 0
            inserted = 0
            with self.conn.cursor() as cursor:
                for item in news_items:
                    try:
                        cursor.execute("""
                            INSERT INTO naver_news (title, pub_date, url, search_keyword, api_request_date)
                            VALUES (%(title)s, %(pub_date)s, %(url)s, %(search_keyword)s, %(api_request_date)s)
                            ON CONFLICT (url) DO NOTHING
                        """, item)
                        inserted += cursor.rowcount
                    except Exception as e:
                        logger.error(f"삽입 오류: {e}")
                        self.conn.rollback()
                        continue
                self.conn.commit()
            return inserted

        def collect(self, query='다', total=1000):
            """메인 수집 함수"""
            logger.info(f"수집 시작: '{query}' 검색어로 {total}개 목표")

            existing_urls = self.get_existing_urls()
            self.collected_urls = set()

            collected = 0
            inserted_total = 0
            start = 1

            while collected < total:
                time.sleep(0.11)  # API 호출 제한

                display = min(100, total - collected)
                result = self.search_news(query, display, start)

                if not result or not result.get('items'):
                    logger.info("더 이상 결과가 없습니다.")
                    break

                # 중복 제거 및 파싱
                new_items = []
                for item in result['items']:
                    url = item['link']
                    if url in existing_urls or url in self.collected_urls:
                        continue
                    parsed = self.parse_item(item, query)
                    new_items.append(parsed)
                    self.collected_urls.add(url)

                # 이전 삽입 총합 저장
                prev_inserted_total = inserted_total

                # DB 삽입
                inserted = self.insert_news(new_items)
                inserted_total += inserted

                collected += len(result['items'])
                logger.info(f"진행: {collected}개 수집, 이번 삽입 {inserted}개, 누적 {inserted_total}개")

                # 새로 삽입된 것이 하나도 없으면 중단
                if inserted_total == prev_inserted_total:
                    logger.info("이전 삽입 총합과 현재 삽입 총합이 동일합니다. 새 데이터가 없어 수집을 종료합니다.")
                    break

                # 다음 페이지
                start += display

                # API 제한 (최대 1000개)
                if start > 1000:
                    logger.info("API 제한 도달 (최대 1000개)")
                    break

            logger.info(f"완료: {collected}개 수집, {inserted_total}개 삽입")
            return inserted_total

    try:
        collector = NaverNewsCollector()
        collector.collect(query='다', total=1000)
    except Exception as e:
        logger.error(f"수집 중 오류: {e}")
        raise


if __name__ == "__main__":
    run_collect_naver_news()
