# ============================================================
# db_connection.py
# Handles RDS PostgreSQL connection for Metadata Store
# ============================================================

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# ── Connection config: read from environment variables ──────
DB_CONFIG = {
    "host":     os.getenv("META_DB_HOST",     "your-rds-endpoint.rds.amazonaws.com"),
    "port":     int(os.getenv("META_DB_PORT", "5432")),
    "dbname":   os.getenv("META_DB_NAME",     "metadata_store"),
    "user":     os.getenv("META_DB_USER",     "etl_user"),
    "password": os.getenv("META_DB_PASSWORD", ""),   # always use env var, never hardcode
}


def get_connection():
    """Return a raw psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def get_cursor(commit: bool = False):
    """
    Context manager that yields a DictCursor.
    Automatically commits if commit=True, rolls back on error.

    Usage:
        with get_cursor() as cur:
            cur.execute("SELECT ...")
            rows = cur.fetchall()
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def test_connection() -> bool:
    """Quick connectivity check — returns True if DB is reachable."""
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
        print("[DB] Connection successful.")
        return True
    except Exception as e:
        print(f"[DB] Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
