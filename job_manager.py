# ============================================================
# job_manager.py
# Adds scheduling logic on top of config_loader.
# Decides which jobs are due, loads jobs + steps + params
# into one cohesive structure for the runner.
# ============================================================

from datetime import datetime, timezone

from config_loader import get_all_active_jobs, get_job, get_job_steps, get_step_params


def get_due_jobs() -> list[dict]:
    """
    Return all active jobs that are due to run now.

    A job is considered due when:
      - next_run is NULL  → never been scheduled, always runnable
      - next_run <= now   → scheduled time has arrived or passed

    Jobs with a future next_run are skipped and logged.
    """
    now  = datetime.now(timezone.utc)
    jobs = get_all_active_jobs()
    due  = []

    for job in jobs:
        next_run = job.get("next_run")
        # Make next_run timezone-aware if the DB returns a naive datetime
        if next_run and next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)

        if next_run is None or next_run <= now:
            due.append(job)
        else:
            print(f"[SKIP] Job '{job['job_name']}' not due until {next_run} (now={now})")

    print(f"[SCHEDULER] {len(due)}/{len(jobs)} job(s) due to run")
    return due


def load_job_with_steps(job_id: int) -> dict:
    """
    Load a single job together with all its active steps and their params.

    Returns:
        {
            "job":   { ...config.job fields... },
            "steps": [
                {
                    ...config.job_step fields...,
                    "params": [ { ...config.step_param fields... } ]
                },
                ...
            ]
        }

    Steps are already ordered by sequence (ascending) from config_loader.
    """
    job   = get_job(job_id)
    steps = get_job_steps(job_id)

    for step in steps:
        step["params"] = get_step_params(step["step_id"])

    return {"job": job, "steps": steps}
