# ============================================================
# executors/csv_executor.py
# Handles CSV ingestion and Parquet output using pandas.
#
# PySpark extension points are marked with comments.
# To switch to Spark, replace the pandas calls with their
# Spark equivalents — the function signatures stay the same.
# ============================================================

import pandas as pd


def read_csv(path: str, params: dict) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    Recognised params (all optional, sensible defaults applied):
        delimiter  — column separator   (default: ,)
        encoding   — file encoding      (default: utf-8)
        header     — header row index   (default: 0)

    Returns a DataFrame with all columns as strings (schema inferred by pandas).

    # ── PySpark extension point ──────────────────────────────
    # Replace with:
    #   spark.read \\
    #       .option("header", "true") \\
    #       .option("inferSchema", "true") \\
    #       .option("sep", delimiter) \\
    #       .csv(path)
    # ────────────────────────────────────────────────────────
    """
    delimiter = params.get("delimiter", ",")
    encoding  = params.get("encoding",  "utf-8")
    header    = int(params.get("header", 0))

    df = pd.read_csv(path, sep=delimiter, encoding=encoding, header=header)
    print(f"[CSV] Read {len(df):,} rows from {path}")
    return df


def write_parquet(df: pd.DataFrame, path: str) -> int:
    """
    Write a pandas DataFrame to a Parquet file (overwrite).
    Returns the number of rows written.

    # ── PySpark extension point ──────────────────────────────
    # Replace with:
    #   df.write.mode("overwrite").parquet(path)
    #   return df.count()
    # ────────────────────────────────────────────────────────
    """
    import os
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"[PARQUET] Wrote {len(df):,} rows to {path}")
    return len(df)
