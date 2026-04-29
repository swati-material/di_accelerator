import os


os.environ.setdefault(
    "JAVA_TOOL_OPTIONS",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
)


import argparse
import sys
from datetime import datetime

# make local imports work from IntelliJ / terminal
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from config.etl_job_config import get_job_config, OUTPUT_DIR
from src.common.logger import setup_logger
from src.common.spark_session import create_spark_session, ensure_dir
from src.common.audit import write_audit_record
from src.steps import STEP_FUNCTION_REGISTRY


def run_job(job_id: str, env: str = "local") -> None:
    logger = setup_logger()
    spark = create_spark_session(f"DI_Accelerator_{job_id}")
    config = get_job_config(job_id=job_id, env=env)

    ensure_dir(OUTPUT_DIR)
    ensure_dir(os.path.join(OUTPUT_DIR, "audit"))

    context = {
        "job_id": job_id,
        "run_ts": datetime.utcnow().isoformat(),
        "env": env,
    }
    step_status = []
    step_name = None

    try:
        logger.info("Starting job: %s", job_id)
        for idx, step in enumerate(config["steps"], start=1):
            step_name = step["name"]
            logger.info("Executing step %s: %s", idx, step_name)
            func = STEP_FUNCTION_REGISTRY[step_name]
            context = func(spark, context, step["params"])
            step_status.append({
                "step_number": idx,
                "step_name": step_name,
                "status": "SUCCESS"
            })

        audit_record = {
            "job_id": job_id,
            "status": "SUCCESS",
            "run_ts": context["run_ts"],
            "steps": step_status
        }
        logger.info("Job completed successfully.")
    except Exception as exc:
        logger.exception("Job failed at step %s", step_name)
        step_status.append({
            "step_name": step_name,
            "status": "FAILED",
            "error": str(exc)
        })
        audit_record = {
            "job_id": job_id,
            "status": "FAILED",
            "run_ts": context["run_ts"],
            "steps": step_status,
            "error": str(exc)
        }
        raise
    finally:
        write_audit_record(os.path.join(OUTPUT_DIR, "audit"), audit_record)
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DI Accelerator ETL job")
    parser.add_argument("--job-id", required=True, help="Job ID to execute")
    parser.add_argument("--env", default="local", help="Execution environment")
    args = parser.parse_args()

    run_job(job_id=args.job_id, env=args.env)