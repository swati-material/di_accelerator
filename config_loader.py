# ============================================================
# config_loader.py
# Thin query layer over config.* tables.
# Returns plain dicts — no business logic here.
#
# Table mapping (matches 01_create_tables.sql exactly):
#   config.job        — master job registry (schedule merged in)
#   config.job_step   — ordered step definitions
#   config.step_param — runtime parameters per step
# ============================================================

from db_connection import get_cursor


def get_all_active_jobs() -> list[dict]:
    """Fetch all active jobs from config.job, ordered by job_id."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT job_id, job_name, job_description, source_system,
                   client_id, cron_expression, frequency, timezone,
                   schedule_type, next_run, last_run,
                   is_active, created_by, created_date
            FROM   config.job
            WHERE  is_active = TRUE
            ORDER  BY job_id
            """
        )
        return [dict(r) for r in cur.fetchall()]


def get_job(job_id: int) -> dict:
    """
    Fetch a single active job by ID.
    Raises ValueError if not found or inactive.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT job_id, job_name, job_description, source_system,
                   client_id, cron_expression, frequency, timezone,
                   schedule_type, next_run, last_run,
                   is_active, created_by, created_date
            FROM   config.job
            WHERE  job_id    = %s
              AND  is_active = TRUE
            """,
            (job_id,),
        )
        row = cur.fetchone()

    if not row:
        raise ValueError(f"No active job found for job_id={job_id}")

    print(f"[CONFIG] Loaded job: {row['job_name']} (job_id={job_id})")
    return dict(row)


def get_job_steps(job_id: int) -> list[dict]:
    """
    Fetch all active steps for a job from config.job_step,
    ordered by sequence ascending.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT step_id, job_id, step_name, step_type,
                   step_action, source_path, target_path,
                   sequence, is_active
            FROM   config.job_step
            WHERE  job_id    = %s
              AND  is_active = TRUE
            ORDER  BY sequence ASC
            """,
            (job_id,),
        )
        rows = cur.fetchall()

    steps = [dict(r) for r in rows]
    print(f"[CONFIG] Loaded {len(steps)} step(s) for job_id={job_id}")
    return steps


def get_step_params(step_id: int) -> list[dict]:
    """
    Fetch all active params for a step from config.step_param.
    Returns an empty list when none are defined.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT param_id, step_id, param_name, param_value,
                   param_data_type, resource_group,
                   fields, filters, delta_to_pull
            FROM   config.step_param
            WHERE  step_id    = %s
              AND  is_active  = TRUE
            """,
            (step_id,),
        )
        return [dict(r) for r in cur.fetchall()]


# ── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    import json

    jobs = get_all_active_jobs()
    print(json.dumps(jobs, indent=2, default=str))
