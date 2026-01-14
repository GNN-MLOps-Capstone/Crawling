import requests
from bs4 import BeautifulSoup
import time
import logging
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 로거 설정
logger = logging.getLogger(__name__)


def crawl_naver_stock_info(stock_code):
    """
    단일 종목에 대한 크롤링 수행 함수
    """
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'  # 한글 깨짐 방지

        if response.status_code != 200:
            logger.warning(f"⚠️ [{stock_code}] 접속 실패: Status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        result = {'market': None, 'industry': None, 'summary': None}

        # 1. Market (코스피/코스닥)
        description_div = soup.select_one('.description')
        if description_div:
            img_tag = description_div.select_one('img.kospi, img.kosdaq, img.konex')
            if img_tag:
                result['market'] = img_tag['alt']

        # 2. Industry (업종)
        industry_tag = soup.select_one('h4.h_sub.sub_tit7 em a')
        if industry_tag:
            result['industry'] = industry_tag.get_text(strip=True)

        # 3. Summary (기업개요)
        summary_div = soup.select_one('#summary_info')
        if summary_div:
            p_tags = summary_div.find_all('p')
            summary_lines = [p.get_text(strip=True) for p in p_tags]
            result['summary'] = "\n".join(summary_lines)

        return result

    except Exception as e:
        logger.error(f"❌ [{stock_code}] 크롤링 중 에러: {e}")
        return None


def update_stock_details(conn_id, **kwargs):
    """
    DB의 모든 종목을 순회하며 상세 정보를 업데이트하는 메인 함수
    """
    logger.info("🚀 종목 상세 정보 크롤링 및 업데이트 시작...")

    # 1. DB 연결
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn_obj = pg_hook.get_connection(conn_id=conn_id)

    conn = psycopg2.connect(
        host=conn_obj.host,
        port=conn_obj.port,
        database=conn_obj.schema,
        user=conn_obj.login,
        password=conn_obj.password
    )
    cursor = conn.cursor()

    try:
        # 2. 대상 종목 가져오기 (이미 정보가 있는 것은 건너뛰려면 WHERE summary_text IS NULL 추가)
        # 여기서는 전체 업데이트를 가정하여 모두 가져옵니다.
        logger.info("📊 업데이트 대상 종목 목록 조회 중...")
        cursor.execute("SELECT stock_id, stock_name FROM stocks ORDER BY stock_id")
        rows = cursor.fetchall()

        total_count = len(rows)
        logger.info(f"👉 총 {total_count}개의 종목을 처리할 예정입니다.")

        success_count = 0

        # 3. 반복 처리
        for index, (stock_id, stock_name) in enumerate(rows):
            # (선택사항) 이미 데이터가 꽉 차있으면 스킵하는 로직을 추가할 수도 있음

            # 크롤링 수행
            data = crawl_naver_stock_info(stock_id)

            if data:
                # DB 업데이트 쿼리
                update_query = """
                               UPDATE stocks
                               SET market       = %s,
                                   industry     = %s,
                                   summary_text = %s
                               WHERE stock_id = %s \
                               """
                cursor.execute(update_query, (
                    data['market'],
                    data['industry'],
                    data['summary'],
                    stock_id
                ))
                success_count += 1

            # 4. 배치 커밋 (안전장치)
            # 100개마다 커밋해서 중간에 끊겨도 저장은 되게 함
            if (index + 1) % 50 == 0:
                conn.commit()
                logger.info(f"⏳ 진행률: {index + 1}/{total_count} 완료... (현재: {stock_name})")

            # 5. 차단 방지 (필수)
            time.sleep(0.3)  # 0.3초 대기 (너무 빠르면 네이버가 차단함)

        # 최종 커밋
        conn.commit()
        logger.info(f"🎉 모든 작업 완료! (성공: {success_count}/{total_count})")

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ 작업 중 치명적 오류 발생: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("🔌 DB 연결 종료")