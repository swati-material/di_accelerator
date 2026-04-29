import os
from pyspark.sql import SparkSession

_JAVA_OPTS = (
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-exports=java.base/sun.security.action=ALL-UNNAMED "
    "--enable-native-access=ALL-UNNAMED"
)


def create_spark_session(app_name: str = "LOCAL_TEST") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.extraJavaOptions", _JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", _JAVA_OPTS)
        .getOrCreate()
    )


def ensure_dir(path: str) -> None:
    if not path.startswith("s3://"):
        os.makedirs(path, exist_ok=True)