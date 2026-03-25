# ============================================================
# audit_manager.py
# Writes runtime audit data to audit.* tables.
#
# Table mapping (matches 01_create_tables.sql exactly):
#   audit.job_run   — one row per job execution
#   audit.step_run  — one row per step execution
#   audit.batch     — delta window detail per job run
#   audit.log       — free-text log lines
#   audit.error     — structured errors
#
# Each function commits immediately so audit data is always
# persisted regardless of caller success or failure.
# ============================================================

from datetime import datetime, timezone
from db_connection import get_cursor


# ── audit.job_run ────────────────────────────────────────────

def start_job_run(job_id: int, triggered_by: str = "Manual") -> int:
    """Insert a RUNNING row into audit.job_run. Returns job_run_id."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit.job_run (job_id, run_status, start_time, triggered_by)
            VALUES (%s, 'RUNNING', %s, %s)
            RETURNING job_run_id
            """,
            (job_id, datetime.now(timezone.utc), triggered_by),
        )
        job_run_id = cur.fetchone()["job_run_id"]
    print(f"[AUDIT] job_run started: job_run_id={job_run_id}")
    return job_run_id


def end_job_run(
    job_run_id: int,
    status: str,
    rows_processed: int = 0,
    error_count: int = 0,
) -> None:
    """Update audit.job_run at job completion. status: SUCCESS | FAILED | PARTIAL"""
    now = datetime.now(timezone.utc)
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE audit.job_run
               SET run_status      = %s,
                   end_time        = %s,
                   rows_processed  = %s,
                   error_count     = %s,
                   runtime_seconds = EXTRACT(EPOCH FROM (%s - start_time))::INT
             WHERE job_run_id = %s
            """,
            (status, now, rows_processed, error_count, now, job_run_id),
        )
    print(f"[AUDIT] job_run closed: status={status} rows={rows_processed} errors={error_count}")


# ── audit.step_run ───────────────────────────────────────────

def start_step_run(job_run_id: int, step_id: int) -> int:
    """Insert a RUNNING row into audit.step_run. Returns step_run_id."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit.step_run (job_run_id, step_id, step_status, start_time)
            VALUES (%s, %s, 'RUNNING', %s)
            RETURNING step_run_id
            """,
            (job_run_id, step_id, datetime.now(timezone.utc)),
        )
        return cur.fetchone()["step_run_id"]


def end_step_run(
    step_run_id: int,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
) -> None:
    """Update audit.step_run at step completion. status: SUCCESS | FAILED"""
    now = datetime.now(timezone.utc)
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE audit.step_run
               SET step_status     = %s,
                   end_time        = %s,
                   rows_in         = %s,
                   rows_out        = %s,
                   runtime_seconds = EXTRACT(EPOCH FROM (%s - start_time))::INT
             WHERE step_run_id = %s
            """,
            (status, now, rows_in, rows_out, now, step_run_id),
        )


# ── audit.batch ──────────────────────────────────────────────

def write_batch(
    job_run_id: int,
    delta_to_pull: str,
    status: str,
    rows_processed: int = 0,
    rows_failed: int = 0,
) -> int:
    """Insert a completed batch entry into audit.batch. Returns batch_id."""
    now = datetime.now(timezone.utc)
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit.batch
                (job_run_id, delta_to_pull, batch_status,
                 rows_processed, rows_failed, start_time, end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING batch_id
            """,
            (job_run_id, delta_to_pull, status, rows_processed, rows_failed, now, now),
        )
        return cur.fetchone()["batch_id"]


# ── audit.log ────────────────────────────────────────────────

def log(
    job_run_id: int,
    message: str,
    level: str = "INFO",
    step_run_id: int | None = None,
    stack_trace: str | None = None,
) -> None:
    """Write a log line to audit.log. level: INFO | WARN | ERROR | DEBUG"""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit.log
                (job_run_id, step_run_id, log_level, log_message, stack_trace, log_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (job_run_id, step_run_id, level, message, stack_trace, datetime.now(timezone.utc)),
        )
    print(f"[{level}] {message}")


# ── audit.error ──────────────────────────────────────────────

def log_error(
    job_run_id: int,
    error_type: str,
    error_message: str,
    step_run_id: int | None = None,
    error_detail: str | None = None,
) -> None:
    """Write a structured error to audit.error."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO audit.error
                (job_run_id, step_run_id, error_type, error_message, error_detail, is_resolved)
            VALUES (%s, %s, %s, %s, %s, FALSE)
            """,
            (job_run_id, step_run_id, error_type, error_message, error_detail),
        )
    print(f"[ERROR] {error_type}: {error_message[:120]}")


# ── config.job.last_run ──────────────────────────────────────

def stamp_last_run(job_id: int) -> None:
    """Update config.job.last_run after each execution."""
    with get_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE config.job SET last_run = %s WHERE job_id = %s",
            (datetime.now(timezone.utc), job_id),
        )
    print(f"[AUDIT] last_run stamped for job_id={job_id}")
