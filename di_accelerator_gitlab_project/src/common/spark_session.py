import os
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "LOCAL_TEST") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .getOrCreate()
    )

def ensure_dir(path: str) -> None:
    if not path.startswith("s3://"):
        os.makedirs(path, exist_ok=True)