import psycopg2
from psycopg2 import pool
import time
from datetime import datetime
from urllib.parse import urlparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from newspaper import Article, Config
import requests
from bs4 import BeautifulSoup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsCrawler:
    def __init__(self, filter_file_path, crawler_version='1.0.0', max_workers=5):
        # ... (기존 __init__ 로직은 그대로 유지) ...
        self.filter_file_path = filter_file_path
        self.crawler_version = crawler_version
        self.filter_version = self._extract_filter_version(filter_file_path)
        self.filtered_domains = self._load_filter_domains()
        self.max_workers = max_workers
        try:
            pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
            conn_obj = pg_hook.get_connection(conn_id='my_postgres_conn')

            self.db_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=max_workers + 5,
                host=conn_obj.host,
                port=conn_obj.port,
                database=conn_obj.schema,
                user=conn_obj.login,
                password=conn_obj.get_password()
            )
            logger.info(f"데이터베이스 커넥션 풀 생성 완료 (최대 {max_workers + 5}개)")
        except psycopg2.OperationalError as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise

    # ------------------------------------------------------------------
    # <--- 이 아래 3개의 함수가 핵심 변경 사항입니다 ---
    # ------------------------------------------------------------------

    def crawl_article(self, url):
        """
        URL을 확인하여 적절한 크롤링 메서드를 호출하는 분배기(dispatcher) 역할
        """
        # URL에 'n.news.naver.com'이 포함되어 있으면 BeautifulSoup 방식 사용
        if 'n.news.naver.com' in url:
            return self._crawl_with_beautifulsoup(url)
        # 그 외의 모든 URL은 기존 newspaper3k 방식 사용
        else:
            return self._crawl_with_newspaper(url)

    def _crawl_with_newspaper(self, url):
        """[기본 크롤러] newspaper3k를 사용하여 기사 크롤링"""
        start_time = time.time()
        try:
            config = Config()
            config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            config.request_timeout = 10
            article = Article(url, config=config, language='ko')
            article.download()
            article.parse()
            response_time_ms = int((time.time() - start_time) * 1000)
            text = article.text.strip()
            return text if text else None, response_time_ms
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.debug(f"[Newspaper] 크롤링 실패 ({url}): {e}")
            return None, response_time_ms

    def _crawl_with_beautifulsoup(self, url):
        """[Naver News 전용 크롤러] requests + BeautifulSoup 사용"""
        start_time = time.time()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            article_body = soup.select_one('div#newsct_article')
            response_time_ms = int((time.time() - start_time) * 1000)
            if article_body:
                text = article_body.get_text(separator='\n', strip=True)
                return text, response_time_ms
            else:
                return None, response_time_ms
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.debug(f"[BeautifulSoup] 크롤링 실패 ({url}): {e}")
            return None, response_time_ms

    def close_pool(self):
        if self.db_pool:
            self.db_pool.closeall()
            logger.info("데이터베이스 커넥션 풀이 모두 종료되었습니다.")

    def _extract_filter_version(self, file_path):
        try:
            version = file_path.split('_v')[-1].replace('.txt', '')
            return f'v{version}'
        except:
            return 'v1.00'

    def _load_filter_domains(self):
        try:
            with open(self.filter_file_path, 'r', encoding='utf-8') as f:
                domains = {line.strip().replace('www.', '') for line in f if line.strip()}
            logger.info(f"필터 도메인 {len(domains)}개 로드 완료 (버전: {self.filter_version})")
            return domains
        except Exception as e:
            logger.error(f"필터 파일 로드 중 오류: {e}")
            return set()

    def _is_url_filtered(self, url):
        try:
            domain = urlparse(url).netloc.replace('www.', '')
            return any(fd in domain or domain in fd for fd in self.filtered_domains)
        except:
            return False

    def _get_db_connection(self):
        return self.db_pool.getconn()

    def _release_db_connection(self, conn):
        self.db_pool.putconn(conn)

    def filter_urls(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT news_id, url FROM naver_news WHERE crawl_status = 'pending' ORDER BY news_id DESC")
            news_list = cursor.fetchall()
            if not news_list:
                logger.info("필터링할 pending 데이터가 없습니다.")
                return 0
            logger.info(f"URL 필터링 시작: {len(news_list):,}개")
            filtered_ids = [news_id for news_id, url in news_list if self._is_url_filtered(url)]
            passed_ids = [news_id for news_id, url in news_list if not self._is_url_filtered(url)]
            if filtered_ids:
                cursor.execute(
                    "UPDATE naver_news SET crawl_status = 'url_filtered', url_filter_version = %s WHERE news_id = ANY(%s)",
                    (self.filter_version, filtered_ids))
            if passed_ids:
                cursor.execute("UPDATE naver_news SET url_filter_version = %s WHERE news_id = ANY(%s)",
                               (self.filter_version, passed_ids))
            conn.commit()
            logger.info(f"URL 필터링 완료 - 필터링: {len(filtered_ids):,}개, 통과: {len(passed_ids):,}개")
            return len(filtered_ids)
        except Exception as e:
            conn.rollback()
            logger.error(f"URL 필터링 중 오류: {e}")
            raise
        finally:
            cursor.close()
            self._release_db_connection(conn)

    def _process_single_news(self, news_data, idx, total):
        news_id, url, title = news_data
        conn = self._get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE naver_news SET crawl_status = 'crawling', crawl_attempt_count = crawl_attempt_count + 1 WHERE news_id = %s",
                (news_id,))
            conn.commit()
            logger.info(f"[{idx}/{total}] {title[:50]}...")
            text, response_time_ms = self.crawl_article(url)  # -> 이 함수가 내부적으로 분기 처리
            if text:
                try:
                    cursor.execute(
                        "INSERT INTO crawled_news (news_id, text, crawler_version, response_time_ms, crawled_at) VALUES (%s, %s, %s, %s, %s)",
                        (news_id, text, self.crawler_version, response_time_ms, datetime.now()))
                    cursor.execute("UPDATE naver_news SET crawl_status = 'crawl_success' WHERE news_id = %s",
                                   (news_id,))
                    conn.commit()
                    logger.info(f"  ✓ 성공 ({len(text):,}자, {response_time_ms}ms)")
                    return 'success'
                except psycopg2.IntegrityError:
                    conn.rollback()
                    cursor.execute("UPDATE naver_news SET crawl_status = 'crawl_success' WHERE news_id = %s",
                                   (news_id,))
                    conn.commit()
                    logger.warning(f"  ⚠ 이미 크롤링됨")
                    return 'success'
            else:
                cursor.execute("UPDATE naver_news SET crawl_status = 'crawl_failed' WHERE news_id = %s", (news_id,))
                conn.commit()
                logger.warning(f"  ✗ 실패: 본문 없음")
                return 'failed'
        except Exception as e:
            conn.rollback()
            logger.error(f"  ✗ 오류: {e}")
            return 'failed'
        finally:
            cursor.close()
            self._release_db_connection(conn)

    def crawl_news(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT news_id, url, title FROM naver_news WHERE crawl_status = 'pending' AND url_filter_version IS NOT NULL ORDER BY pub_date DESC")
            news_list = cursor.fetchall()
            if not news_list:
                logger.info("크롤링할 데이터가 없습니다.")
                return 0, 0
            total = len(news_list)
            logger.info(f"크롤링 시작: {total:,}개 (스레드: {self.max_workers}개)")
            success_count, failed_count = 0, 0
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_news = {executor.submit(self._process_single_news, news_data, idx, total): news_data for
                                  idx, news_data in enumerate(news_list, 1)}
                for future in as_completed(future_to_news):
                    result = future.result()
                    if result == 'success':
                        success_count += 1
                    else:
                        failed_count += 1
            logger.info(f"크롤링 완료 - 성공: {success_count:,}개, 실패: {failed_count:,}개")
            return success_count, failed_count
        except Exception as e:
            logger.error(f"크롤링 중 오류: {e}")
            raise
        finally:
            cursor.close()
            self._release_db_connection(conn)

    def run(self):
        start_time = time.time()
        logger.info("=" * 60)
        logger.info(
            f"뉴스 크롤링 시작\n- 크롤러 버전: {self.crawler_version}\n- 필터 버전: {self.filter_version}\n- 병렬 스레드 수: {self.max_workers}")
        logger.info("=" * 60)
        logger.info("\n[1단계] URL 필터링")
        filtered_count = self.filter_urls()
        logger.info("\n[2단계] 뉴스 크롤링")
        success_count, failed_count = self.crawl_news()
        elapsed_time = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info(f"완료 - 필터링: {filtered_count:,}, 성공: {success_count:,}, 실패: {failed_count:,}")
        logger.info(f"총 실행 시간: {elapsed_time:.2f}초")
        if (success_count + failed_count) > 0:
            logger.info(f"평균 처리 시간: {elapsed_time / (success_count + failed_count):.2f}초/건")
        logger.info("=" * 60)

def run_news_crawler():
    crawler = None
    try:
        crawler = NewsCrawler(
            filter_file_path='filter_domain_list_v1.00.txt',
            crawler_version='0.02',  # Hybrid 버전
            max_workers=10
        )
        crawler.run()
    except Exception as e:
        logger.error(f"크롤러 실행 중 심각한 오류 발생: {e}")
    finally:
        if crawler:
            crawler.close_pool()

if __name__ == "__main__":
    run_news_crawler()