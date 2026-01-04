import pandas as pd
import logging
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 로거 설정 (전역 또는 함수 내에서 정의 필요)
logger = logging.getLogger(__name__)


def parse_and_load_all(csv_path, conn_id, **kwargs):
    logging.info(f"📂 로직 시작: CSV 파일({csv_path}) 로딩 중...")

    # [수정 1] 인자 변수명 통일 (db_conn_id -> conn_id)
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn_obj = pg_hook.get_connection(conn_id=conn_id)

    conn = None
    cursor = None  # [수정 2] finally 블록을 위해 초기화

    try:
        # 3. DB 수동 연결 (psycopg2)
        conn = psycopg2.connect(
            host=conn_obj.host,
            port=conn_obj.port,
            database=conn_obj.schema,
            user=conn_obj.login,
            password=conn_obj.password
        )
        logger.info(f"✅ 데이터베이스({conn_obj.host}) 수동 연결 성공")

        # [수정 3] 커서 생성 (이게 없으면 쿼리 실행 불가!)
        cursor = conn.cursor()

    except psycopg2.OperationalError as e:
        logger.error(f"❌ 데이터베이스 연결 실패: {e}")
        raise

    # CSV 읽기 및 전처리
    try:
        # 한글 CSV는 cp949 또는 euc-kr 인코딩이 많음
        df = pd.read_csv(csv_path, encoding='cp949')
    except FileNotFoundError:
        logging.error(f"❌ 파일을 찾을 수 없습니다: {csv_path}")
        if conn: conn.close()  # 연결 닫고 종료
        raise
    except UnicodeDecodeError:
        # 만약 cp949로 안되면 utf-8로 재시도 하는 로직 등을 추가할 수 있음
        logging.error(f"❌ 인코딩 에러. utf-8로 다시 시도해보세요.")
        if conn: conn.close()
        raise

    # 데이터 전처리
    df = df.dropna(subset=['표준코드'])
    df['stock_id'] = df['표준코드'].astype(str).str.slice(3, 9)

    try:
        # [Step 1] Stocks 테이블 Upsert
        logging.info("🚀 Stocks 테이블 데이터 적재 중...")

        # 쿼리 가독성을 위해 f-string이나 줄바꿈 정리
        stock_upsert_query = """
                             INSERT INTO stocks (stock_id, stock_name, isin)
                             VALUES (%s, %s, %s) ON CONFLICT (stock_id) 
            DO \
                             UPDATE SET
                                 isin = EXCLUDED.isin, \
                                 stock_name = EXCLUDED.stock_name; \
                             """
        stock_data = list(zip(df['stock_id'], df['한글명'], df['표준코드']))
        cursor.executemany(stock_upsert_query, stock_data)
        logging.info(f"✅ Stocks 테이블 처리 완료")

        # [Step 2] 별명(Aliases) 테이블 Insert
        logging.info("🚀 별명 데이터 파싱 및 적재 중...")

        df_alias = df.dropna(subset=['추가명'])
        insert_alias_data = []

        for _, row in df_alias.iterrows():
            stock_id = row['stock_id']
            # 쉼표 분리
            aliases = [a.strip() for a in str(row['추가명']).split(',') if a.strip()]
            for alias in aliases:
                insert_alias_data.append((stock_id, alias))

        if insert_alias_data:
            alias_insert_query = """
                                 INSERT INTO aliases (stock_id, alias_name)
                                 VALUES (%s, %s) ON CONFLICT (stock_id, alias_name) 
                DO NOTHING; \
                                 """
            cursor.executemany(alias_insert_query, insert_alias_data)
            logging.info(f"✅ 별명 처리 완료 (중복된 데이터는 자동으로 건너뜀)")

        conn.commit()
        logging.info("🎉 모든 데이터 적재 완료")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"❌ DB 작업 중 에러 발생: {e}")

        if 'constraint' in str(e).lower():
            logging.error("💡 힌트: aliases 테이블에 UNIQUE 제약조건이 있는지 확인하세요.")
        raise

    finally:
        # [수정 4] 객체가 존재할 때만 close (연결 실패 시 에러 방지)
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("🔌 연결 종료")