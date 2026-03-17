# ============================================================
# audit_manager.py
# Writes run-time audit data to AUDIT_* tables.
# Called by run_etl_job.py at the start, end, and on errors.
# ============================================================

from datetime import datetime, timezone
from db_connection import get_cursor


# ── Job-level audit ──────────────────────────────────────────

def start_job_audit(job_id: int, triggered_by: str = "Manual") -> int:
    """
    Insert a new AUDIT_ETL_JOB row at job start.
    Returns job_audit_id to be passed to all subsequent audit calls.
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit_etl_job (job_id, run_status, start_time, triggered_by)
            VALUES (%s, 'RUNNING', %s, %s)
            RETURNING job_audit_id
            """,
            (job_id, datetime.now(timezone.utc), triggered_by)
        )
        job_audit_id = cur.fetchone()["job_audit_id"]

    print(f"[Audit] Job audit started: JOB_AUDIT_ID={job_audit_id}")
    return job_audit_id


def end_job_audit(
    job_audit_id: int,
    status: str,
    rows_processed: int = 0,
    error_count: int = 0
) -> None:
    """
    Update AUDIT_ETL_JOB at job completion with final status and metrics.
    status: 'SUCCESS' | 'FAILED' | 'PARTIAL'
    """
    now = datetime.now(timezone.utc)
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE audit_etl_job
               SET run_status      = %s,
                   end_time        = %s,
                   rows_processed  = %s,
                   error_count     = %s,
                   runtime_seconds = EXTRACT(EPOCH FROM (%s - start_time))::INT
             WHERE job_audit_id = %s
            """,
            (status, now, rows_processed, error_count, now, job_audit_id)
        )
    print(f"[Audit] Job audit closed: status={status}, rows={rows_processed}")


# ── Batch-level audit ────────────────────────────────────────

def start_batch_audit(job_audit_id: int, delta_to_pull: str) -> int:
    """
    Insert a new AUDIT_ETL_BATCH row at batch start.
    Returns batch_id.
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit_etl_batch (job_audit_id, delta_to_pull, batch_status, start_time)
            VALUES (%s, %s, 'RUNNING', %s)
            RETURNING batch_id
            """,
            (job_audit_id, delta_to_pull, datetime.now(timezone.utc))
        )
        batch_id = cur.fetchone()["batch_id"]

    print(f"[Audit] Batch started: BATCH_ID={batch_id}, delta={delta_to_pull}")
    return batch_id


def end_batch_audit(
    batch_id: int,
    status: str,
    rows_processed: int = 0,
    rows_failed: int = 0
) -> None:
    """Update AUDIT_ETL_BATCH at batch completion."""
    now = datetime.now(timezone.utc)
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE audit_etl_batch
               SET batch_status    = %s,
                   end_time        = %s,
                   rows_processed  = %s,
                   rows_failed     = %s,
                   runtime_seconds = EXTRACT(EPOCH FROM (%s - start_time))::INT
             WHERE batch_id = %s
            """,
            (status, now, rows_processed, rows_failed, now, batch_id)
        )
    print(f"[Audit] Batch closed: BATCH_ID={batch_id}, status={status}")


# ── Logging ──────────────────────────────────────────────────

def log_message(
    job_audit_id: int,
    message: str,
    level: str = "INFO",
    stack_trace: str = None
) -> None:
    """
    Write a log line to AUDIT_ETL_JOB_LOG.
    level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG'
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit_etl_job_log (job_audit_id, log_level, log_message, stack_trace, log_time)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_audit_id, level, message, stack_trace, datetime.now(timezone.utc))
        )
    print(f"[{level}] {message}")


# ── Error recording ──────────────────────────────────────────

def log_error(
    job_audit_id: int,
    error_type: str,
    error_message: str,
    error_detail: str = None,
    error_code: str = None
) -> None:
    """
    Write a structured error to AUDIT_ETL_ERROR.
    error_type: EXTRACTION | TRANSFORMATION | LOAD | NETWORK | SCHEMA | PERMISSION
    """
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit_etl_error
                (job_audit_id, error_type, error_message, error_detail, error_code, is_resolved)
            VALUES (%s, %s, %s, %s, %s, FALSE)
            """,
            (job_audit_id, error_type, error_message, error_detail, error_code)
        )
    print(f"[ERROR] {error_type}: {error_message}")


# ── Schedule update ──────────────────────────────────────────

def update_schedule_last_run(job_id: int) -> None:
    """Stamp LAST_RUN on the job's schedule record after each execution."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE etl_job_schedule
               SET last_run = %s
             WHERE job_id    = %s
               AND is_active = 'Y'
            """,
            (datetime.now(timezone.utc), job_id)
        )
    print(f"[Audit] Schedule last_run updated for JOB_ID={job_id}")
