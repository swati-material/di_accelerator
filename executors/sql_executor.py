# ============================================================
# executors/sql_executor.py
# Executes SQL against the configured PostgreSQL database.
# Used by the TRANSFORM_SQL step type.
#
# PySpark extension point is marked with a comment.
# ============================================================

import pandas as pd

from db_connection import get_connection


def execute_sql(sql: str) -> tuple[int, pd.DataFrame | None]:
    """
    Execute a SQL string against PostgreSQL.

    Behaviour by statement type:
        SELECT / WITH  → fetches all rows, returns (row_count, DataFrame)
        INSERT/UPDATE/DELETE/DDL → commits and returns (rows_affected, None)

    Returns:
        (row_count_or_affected, DataFrame_or_None)

    Raises on any DB error (caller handles and audits).

    # ── PySpark extension point ──────────────────────────────
    # For Spark SQL against registered temp views:
    #   df = spark.sql(sql)
    #   return df.count(), df
    #
    # For Spark reading from JDBC:
    #   df = spark.read.jdbc(url=jdbc_url, table=f"({sql}) t", properties=props)
    #   return df.count(), df
    # ────────────────────────────────────────────────────────
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)

            if cur.description:                             # result-set query
                rows    = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                df      = pd.DataFrame(rows, columns=columns)
                conn.commit()
                print(f"[SQL] SELECT returned {len(df):,} rows")
                return len(df), df

            else:                                           # DML / DDL
                affected = cur.rowcount
                conn.commit()
                print(f"[SQL] DML affected {affected:,} rows")
                return affected, None
    finally:
        conn.close()
