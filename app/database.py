import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from app.config import settings

# Global connection pool initialized lazily
_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=15,
            dsn=settings.DATABASE_URL,
            cursor_factory=RealDictCursor
        )
    return _pool

def query(sql: str, params=None):
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            try:
                return cur.fetchall()
            except:
                # Handle cases like INSERT or UPDATE that don't return rows
                return []
    finally:
        # Always return the connection to the pool
        pool.putconn(conn)

def query_one(sql: str, params=None):
    result = query(sql, params)
    return result[0] if result else None