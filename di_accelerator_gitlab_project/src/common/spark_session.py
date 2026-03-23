import os
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "DI_Accelerator_POC") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def ensure_dir(path: str) -> None:
    if not path.startswith("s3://"):
        os.makedirs(path, exist_ok=True)
