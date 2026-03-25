# ============================================================
# runner.py
# Main ETL orchestrator — metadata-driven, no hardcoded logic.
#
# Execution flow:
#   1. Load all active jobs that are due (or a specific job_id)
#   2. For each job:
#        a. Open audit.job_run   (status = RUNNING)
#        b. Load steps + params from config.*
#        c. For each step (in sequence order):
#             i.  Open  audit.step_run  (status = RUNNING)
#             ii. Execute step via step_executor
#             iii.Write audit.batch entry
#             iv. Close audit.step_run  (SUCCESS | FAILED)
#             v.  Log to audit.log
#             vi. On failure: write audit.error, continue to next step
#        d. Close audit.job_run (SUCCESS | PARTIAL | FAILED)
#        e. Stamp config.job.last_run
#
# Failure strategy:
#   - A failing step is audited and skipped; remaining steps continue.
#   - job_status = PARTIAL when some steps fail.
#   - job_status = FAILED  when every step fails.
#
# Usage:
#   python runner.py              → runs all due jobs
#   python runner.py 1            → runs job_id=1 only
#   python runner.py 1 scheduler  → runs job_id=1, triggered_by=scheduler
# ============================================================

import sys
import traceback

import audit_manager as audit
from config_loader  import get_job
from job_manager    import get_due_jobs, load_job_with_steps
from step_executor  import execute_step
from utils.param_utils import build_param_dict

def _header(text: str) -> None:
    bar = "=" * 60
    print(f"\n{bar}\n  {text}\n{bar}")

# ─────────────────────────────────────────────────────────────
# SINGLE JOB RUNNER
# ─────────────────────────────────────────────────────────────

def run_job(job: dict, triggered_by: str = "Manual") -> None:
    """Execute one job end-to-end with a full audit trail."""
    job_id   = job["job_id"]
    job_name = job["job_name"]

    _header(f"JOB: {job_name}  (job_id={job_id})")

    # Open job-level audit row
    job_run_id = audit.start_job_run(job_id, triggered_by)
    audit.log(
        job_run_id,
        f"Job started | source={job.get('source_system')} "
        f"| schedule={job.get('schedule_type')} "
        f"| triggered_by={triggered_by}",
    )

    total_rows   = 0
    total_errors = 0
    job_status   = "SUCCESS"

    # Load steps (already ordered by sequence)
    config = load_job_with_steps(job_id)
    steps  = config["steps"]

    if not steps:
        audit.log(job_run_id, "No active steps found — nothing to execute", level="WARN")
        audit.end_job_run(job_run_id, "SUCCESS", 0, 0)
        audit.stamp_last_run(job_id)
        return

    # Shared context: holds in-memory DataFrames and metadata
    context: dict = {
        "views":      {},
        "job_run_id": job_run_id,
        "job":        job,
    }

    # ── Execute steps in sequence ────────────────────────────
    for step in steps:
        step_id   = step["step_id"]
        step_name = step["step_name"]
        step_type = step["step_type"]
        seq       = step["sequence"]

        params      = build_param_dict(step["params"])
        step_run_id = audit.start_step_run(job_run_id, step_id)
        delta       = params.get("delta_to_pull", "full_refresh")

        audit.log(
            job_run_id,
            f"Step [{seq}] '{step_name}' ({step_type}) — START",
            step_run_id=step_run_id,
        )

        rows_in = rows_out = 0
        try:
            rows_in, rows_out = execute_step(step, params, context)

            # ── step succeeded ───────────────────────────────
            audit.end_step_run(step_run_id, "SUCCESS", rows_in, rows_out)
            audit.write_batch(job_run_id, delta, "SUCCESS", rows_out, 0)
            audit.log(
                job_run_id,
                f"Step [{seq}] '{step_name}' — SUCCESS "
                f"| rows_in={rows_in:,} rows_out={rows_out:,}",
                step_run_id=step_run_id,
            )
            total_rows += rows_out

        except Exception as exc:
            # ── step failed — log and continue ───────────────
            tb = traceback.format_exc()
            total_errors += 1
            job_status = "PARTIAL"

            audit.end_step_run(step_run_id, "FAILED", rows_in, rows_out)
            audit.write_batch(job_run_id, delta, "FAILED", 0, 1)
            audit.log(
                job_run_id,
                f"Step [{seq}] '{step_name}' — FAILED: {exc}",
                level="ERROR",
                step_run_id=step_run_id,
                stack_trace=tb,
            )
            audit.log_error(
                job_run_id,
                error_type=step_type,        # e.g. READ_CSV, TRANSFORM_SQL
                error_message=str(exc),
                step_run_id=step_run_id,
                error_detail=tb,
            )
            # Do NOT re-raise — continue to the next step

    # Promote PARTIAL → FAILED only when every single step failed
    if total_errors == len(steps):
        job_status = "FAILED"

    # ── Close job audit ──────────────────────────────────────
    audit.end_job_run(job_run_id, job_status, total_rows, total_errors)
    audit.stamp_last_run(job_id)
    audit.log(
        job_run_id,
        f"Job finished | status={job_status} "
        f"| total_rows={total_rows:,} | errors={total_errors}",
    )

    _header(
        f"DONE  status={job_status}  "
        f"rows={total_rows:,}  errors={total_errors}"
    )


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main(job_id: int | None = None, triggered_by: str = "Manual") -> None:
    """
    Run a specific job by ID, or all active jobs that are currently due.
    """
    if job_id is not None:
        run_job(get_job(job_id), triggered_by)
    else:
        jobs = get_due_jobs()
        if not jobs:
            print("[RUNNER] No jobs due to run right now.")
            return
        for job in jobs:
            run_job(job, triggered_by)


# ─────────────────────────────────────────────────────────────
# CLI  —  python runner.py [job_id] [triggered_by]
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _job_id       = int(sys.argv[1]) if len(sys.argv) > 1 else None
    _triggered_by = sys.argv[2]      if len(sys.argv) > 2 else "Manual"
    main(_job_id, _triggered_by)


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────

def _header(text: str) -> None:
    bar = "=" * 60
    print(f"\n{bar}\n  {text}\n{bar}")
