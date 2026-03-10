import psycopg2
from psycopg2.extras import RealDictCursor
from app.config import settings

def get_conn():
    return psycopg2.connect(settings.DATABASE_URL, cursor_factory=RealDictCursor)

def query(sql: str, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            try:
                return cur.fetchall()
            except:
                return []
    finally:
        conn.close()

def query_one(sql: str, params=None):
    result = query(sql, params)
    return result[0] if result else None