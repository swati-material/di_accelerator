# ============================================================
# run_etl_job.py
# Main orchestrator — reads config from Metadata Store,
# runs each step function in SEQUENCE order, writes full audit trail.
# ============================================================

import sys
import traceback
from datetime import date, timedelta

from pyspark.sql import SparkSession

from config_loader import load_full_config
from audit_manager  import (
    start_job_audit, end_job_audit,
    start_batch_audit, end_batch_audit,
    log_message, log_error, update_schedule_last_run
)

# ── S3 base paths ────────────────────────────────────────────
S3_BUCKET = "s3://di-accelerator"
RAW_PATH    = f"{S3_BUCKET}/raw"
SILVER_PATH = f"{S3_BUCKET}/silver/curated_datasets"
GOLD_PATH   = f"{S3_BUCKET}/gold/business_kpis"
CONFIG_PATH = f"{S3_BUCKET}/config"


# ── Spark session ────────────────────────────────────────────

def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ── Delta window resolver ────────────────────────────────────

def resolve_delta(delta_to_pull: str) -> str:
    """
    Converts a delta expression to an actual date string.
    'Current_Date -1'  → yesterday's date  e.g. '2024-06-14'
    'full_refresh'     → None (no date filter)
    """
    if not delta_to_pull or delta_to_pull.lower() == "full_refresh":
        return None
    if "Current_Date" in delta_to_pull:
        offset = int(delta_to_pull.replace("Current_Date", "").replace(" ", "") or "0")
        return str(date.today() + timedelta(days=offset))
    return delta_to_pull


# ============================================================
# STEP FUNCTIONS
# ============================================================

def step1_read_csv_to_raw(spark: SparkSession, param: dict, job_audit_id: int) -> int:
    """Step 1: Read CSV from S3 landing zone, write to RAW as Parquet."""
    resource = param["resource_group"]
    fields   = param.get("fields", "").split(",") if param.get("fields") else None

    log_message(job_audit_id, f"[Step1] Reading CSV for: {resource}")

    # Determine provider path from resource group name
    if "provider_a" in resource or resource == "customer_master_a":
        source_path = f"{S3_BUCKET}/landing/provider_a/{resource}.csv"
    elif "provider_b" in resource or resource == "customer_master_b":
        source_path = f"{S3_BUCKET}/landing/provider_b/{resource}.csv"
    else:
        source_path = f"{S3_BUCKET}/landing/{resource}.csv"

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

    # Select only configured fields if provided
    if fields:
        df = df.select([f.strip() for f in fields if f.strip() in df.columns])

    # Add audit fields
    from pyspark.sql.functions import current_timestamp, lit
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", lit(source_path))

    output_path = f"{RAW_PATH}/{resource}"
    df.write.mode("overwrite").parquet(output_path)

    row_count = df.count()
    log_message(job_audit_id, f"[Step1] Written {row_count} rows to {output_path}")
    return row_count


def step2_read_raw_parquet(spark: SparkSession, resource: str, job_audit_id: int):
    """Step 2: Read RAW parquet into a Spark DataFrame."""
    path = f"{RAW_PATH}/{resource}"
    log_message(job_audit_id, f"[Step2] Reading RAW parquet from: {path}")
    return spark.read.parquet(path)


def step3_register_temp_table(df, resource: str, job_audit_id: int) -> str:
    """Step 3: Register DataFrame as a Spark temp view."""
    view_name = resource.replace("-", "_").replace(" ", "_").lower()
    df.createOrReplaceTempView(view_name)
    log_message(job_audit_id, f"[Step3] Registered temp view: {view_name}")
    return view_name


def step4_transform_silver(spark: SparkSession, job_audit_id: int):
    """Step 4 & 5: Execute SQL transformation for Silver layer."""
    log_message(job_audit_id, "[Step4] Executing Silver transformation SQL")

    sql = """
        SELECT
            COALESCE(a.customer_id, b.customer_id)  AS customer_id,
            COALESCE(a.customer_name, b.customer_name) AS customer_name,
            a.email,
            a.region,
            b.phone,
            b.country,
            act.activity_id,
            act.activity_type,
            act.activity_date,
            act.amount,
            current_timestamp()                       AS _transformed_at
        FROM customer_activity act
        LEFT JOIN customer_master_a a ON act.customer_id = a.customer_id
        LEFT JOIN customer_master_b b ON act.customer_id = b.customer_id
    """
    return spark.sql(sql)


def step4_transform_gold(spark: SparkSession, job_audit_id: int):
    """Step 4 & 5: Execute SQL aggregation for Gold layer."""
    log_message(job_audit_id, "[Step4] Executing Gold aggregation SQL")

    sql = """
        SELECT
            region,
            activity_type,
            COUNT(*)      AS activity_count,
            SUM(amount)   AS total_amount,
            current_date()  AS report_date
        FROM curated_customer_activity
        GROUP BY region, activity_type
    """
    return spark.sql(sql)


def step6_write_output(df, resource: str, layer: str, job_audit_id: int) -> int:
    """Step 6: Write DataFrame to SILVER or GOLD layer."""
    if layer == "SILVER":
        output_path = f"{SILVER_PATH}/{resource}"
    else:
        output_path = f"{GOLD_PATH}/{resource}"

    df.write.mode("overwrite").parquet(output_path)
    row_count = df.count()
    log_message(job_audit_id, f"[Step6] Written {row_count} rows to {output_path}")
    return row_count


# ============================================================
# MAIN ORCHESTRATOR
# ============================================================

def run_job(job_id: int, triggered_by: str = "Manual") -> None:
    """
    Entry point. Loads config from Metadata Store and runs all steps.
    Usage: python run_etl_job.py <job_id>
    """
    # ── Load config ──────────────────────────────────────────
    config  = load_full_config(job_id)
    job     = config["job"]
    params  = config["params"]

    # ── Start audit ──────────────────────────────────────────
    job_audit_id   = start_job_audit(job_id, triggered_by)
    spark          = get_spark(job["job_name"])
    total_rows     = 0
    total_errors   = 0
    job_status     = "SUCCESS"

    log_message(job_audit_id, f"Starting job: {job['job_name']} | Source: {job['source_system']}")

    try:
        etl_params       = [p for p in params if p["param_type"] == "ETL"]
        transform_params = [p for p in params if p["param_type"] == "TRANSFORM"]

        # ── ETL Step Functions (one per input resource) ──────
        for param in etl_params:
            resource     = param["resource_group"]
            delta        = resolve_delta(param["delta_to_pull"])
            batch_id     = start_batch_audit(job_audit_id, param["delta_to_pull"])

            try:
                # Step 1: CSV → RAW
                rows = step1_read_csv_to_raw(spark, param, job_audit_id)

                # Step 2: RAW parquet → DataFrame
                df = step2_read_raw_parquet(spark, resource, job_audit_id)

                # Step 3: Register as temp view
                step3_register_temp_table(df, resource, job_audit_id)

                end_batch_audit(batch_id, "SUCCESS", rows_processed=rows)
                total_rows += rows

            except Exception as e:
                tb = traceback.format_exc()
                log_error(job_audit_id, "EXTRACTION", str(e), tb)
                end_batch_audit(batch_id, "FAILED", rows_failed=1)
                total_errors += 1
                job_status = "PARTIAL"

        # ── TRANSFORM Step Functions ─────────────────────────
        for param in transform_params:
            resource = param["resource_group"]
            batch_id = start_batch_audit(job_audit_id, param["delta_to_pull"])

            try:
                if resource == "curated_customer_activity":
                    # Step 4 & 5: Silver transform SQL
                    df = step4_transform_silver(spark, job_audit_id)
                    # Step 3: Register for downstream Gold step
                    step3_register_temp_table(df, resource, job_audit_id)
                    # Step 6: Write to Silver
                    rows = step6_write_output(df, resource, "SILVER", job_audit_id)

                elif resource == "activity_kpi_summary":
                    # Step 4 & 5: Gold aggregation SQL
                    df = step4_transform_gold(spark, job_audit_id)
                    # Step 6: Write to Gold
                    rows = step6_write_output(df, resource, "GOLD", job_audit_id)

                else:
                    log_message(job_audit_id, f"[WARN] No transform defined for: {resource}", "WARN")
                    rows = 0

                end_batch_audit(batch_id, "SUCCESS", rows_processed=rows)
                total_rows += rows

            except Exception as e:
                tb = traceback.format_exc()
                log_error(job_audit_id, "TRANSFORMATION", str(e), tb)
                end_batch_audit(batch_id, "FAILED", rows_failed=1)
                total_errors += 1
                job_status = "PARTIAL"

    except Exception as e:
        tb = traceback.format_exc()
        log_error(job_audit_id, "LOAD", str(e), tb)
        job_status = "FAILED"
        total_errors += 1

    finally:
        # ── Close audit ──────────────────────────────────────
        end_job_audit(job_audit_id, job_status, total_rows, total_errors)
        update_schedule_last_run(job_id)
        spark.stop()
        print(f"\n[DONE] Job finished: status={job_status}, rows={total_rows}, errors={total_errors}")


# ── CLI entry point ──────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_etl_job.py <job_id> [triggered_by]")
        sys.exit(1)

    job_id       = int(sys.argv[1])
    triggered_by = sys.argv[2] if len(sys.argv) > 2 else "Manual"
    run_job(job_id, triggered_by)
