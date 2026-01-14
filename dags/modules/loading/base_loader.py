import psycopg2
from psycopg2.extras import execute_values

class BaseLoader:
    def __init__(self, pg_info: dict):
        self.pg_info = pg_info

    def get_connection(self):
        return psycopg2.connect(**self.pg_info)

    def bulk_insert(self, sql: str, data: list, page_size: int = 1000):
        if not data: return
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, data, page_size=page_size)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()