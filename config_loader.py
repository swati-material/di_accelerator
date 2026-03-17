# ============================================================
# config_loader.py
# Loads ETL job configuration from the Metadata Store DB.
# Replaces the hardcoded etl_job_config.py.
# ============================================================

from db_connection import get_cursor


def get_job_config(job_id: int) -> dict:
    """
    Load the master job record from ETL_JOB_CONFIG.

    Returns a dict with job details, or raises ValueError if not found / inactive.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT job_id, client_id, job_name, job_description,
                   source_system, secret_scope, secret_key_name,
                   status, is_active, version
            FROM   etl_job_config
            WHERE  job_id   = %s
              AND  is_active = 'Y'
              AND  status    = 'ACTIVE'
            """,
            (job_id,)
        )
        row = cur.fetchone()

    if not row:
        raise ValueError(f"[ConfigLoader] No active job found for JOB_ID={job_id}")

    print(f"[ConfigLoader] Loaded job: {row['job_name']} (ID={job_id})")
    return dict(row)


def get_job_params(job_id: int) -> list[dict]:
    """
    Load all active execution parameters for a job, ordered by SEQUENCE.

    Returns a list of dicts — one per RESOURCE_GROUP / processing step.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT param_id, job_id, resource_group, param_name,
                   fields, filters, delta_to_pull, param_type,
                   masked_fields, sequence, validation_rules,
                   dependencies, description
            FROM   etl_param_config
            WHERE  job_id    = %s
              AND  is_active = 'Y'
            ORDER  BY sequence ASC
            """,
            (job_id,)
        )
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"[ConfigLoader] No active params found for JOB_ID={job_id}")

    params = [dict(r) for r in rows]
    print(f"[ConfigLoader] Loaded {len(params)} param(s) for JOB_ID={job_id}")
    return params


def get_job_schedule(job_id: int) -> dict | None:
    """
    Load the active schedule for a job.

    Returns a dict, or None if no schedule is configured.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT schedule_id, job_id, cron_expression,
                   frequency, timezone, next_run, last_run, schedule_type
            FROM   etl_job_schedule
            WHERE  job_id    = %s
              AND  is_active = 'Y'
            LIMIT  1
            """,
            (job_id,)
        )
        row = cur.fetchone()

    if row:
        print(f"[ConfigLoader] Schedule: {row['cron_expression']} ({row['frequency']})")
        return dict(row)

    print(f"[ConfigLoader] No schedule found for JOB_ID={job_id}")
    return None


def load_full_config(job_id: int) -> dict:
    """
    Convenience method — loads job + params + schedule in one call.

    Returns:
        {
            "job":      { ...ETL_JOB_CONFIG fields... },
            "params":   [ { ...ETL_PARAM_CONFIG fields... }, ... ],
            "schedule": { ...ETL_JOB_SCHEDULE fields... } or None
        }
    """
    return {
        "job":      get_job_config(job_id),
        "params":   get_job_params(job_id),
        "schedule": get_job_schedule(job_id),
    }


# ── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    import json
    config = load_full_config(job_id=1)
    print(json.dumps(config, indent=2, default=str))
