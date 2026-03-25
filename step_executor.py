# ============================================================
# step_executor.py
# Dispatches each step to the correct executor based on step_type.
#
# Supported step types (matches config.job_step.step_type values):
#   READ_CSV       — read CSV file, store DataFrame in context
#   REGISTER_VIEW  — alias an existing context view to a new name
#   TRANSFORM_SQL  — run SQL against PostgreSQL, store result in context
#   WRITE_PARQUET  — write a named context DataFrame to a Parquet file
#   PYTHON         — execute a Python script with injected context
#
# Contract: every handler returns (rows_in: int, rows_out: int).
# Raise on unrecoverable errors — the runner catches and audits them.
# ============================================================

import pandas as pd

from executors.csv_executor    import read_csv, write_parquet
from executors.sql_executor    import execute_sql
from executors.python_executor import execute_python
from utils.param_utils         import substitute
from utils.file_loader         import load_sql, is_file_path


def execute_step(step: dict, params: dict, context: dict) -> tuple[int, int]:
    """
    Dispatch one step to the appropriate handler.

    Args:
        step    : row from config.job_step (with string fields)
        params  : flat param dict built by utils.param_utils.build_param_dict()
        context : shared mutable dict with keys:
                      "views"      → { view_name: pd.DataFrame }
                      "job_run_id" → int
                      "job"        → job config dict

    Returns:
        (rows_in, rows_out) — counts for audit.step_run
    """
    step_type   = (step.get("step_type") or "").upper().strip()
    step_action = (step.get("step_action") or "").strip()
    source_path = (step.get("source_path") or "").strip()
    target_path = (step.get("target_path") or "").strip()
    step_name   = step["step_name"]

    if step_type == "READ_CSV":
        return _read_csv(step_name, source_path, target_path, params, context)

    if step_type == "REGISTER_VIEW":
        return _register_view(step_name, source_path, context)

    if step_type == "TRANSFORM_SQL":
        return _transform_sql(step_name, step_action, target_path, params, context)

    if step_type == "WRITE_PARQUET":
        return _write_parquet(step_name, source_path, target_path, context)

    if step_type == "PYTHON":
        return _python(step_action, params, context)

    raise ValueError(f"[StepExecutor] Unknown step_type: '{step_type}'")


# ── handlers ─────────────────────────────────────────────────

def _read_csv(
    step_name: str,
    source_path: str,
    target_path: str,
    params: dict,
    context: dict,
) -> tuple[int, int]:
    """Load CSV → pandas DataFrame → register in context["views"].
    If target_path is set (from config.job_step), write the DataFrame to Parquet.
    """
    path = substitute(source_path, params)
    df   = read_csv(path, params)
    _set_view(context, step_name, df)

    if target_path:
        write_parquet(df, substitute(target_path, params))

    return len(df), len(df)


def _register_view(
    step_name: str,
    source_name: str,
    context: dict,
) -> tuple[int, int]:
    """
    Alias an existing view under a new name.
    source_path column holds the name of the view to alias.
    If source_name is blank, the step itself is re-registered (no-op alias).
    """
    src_df = _get_view(context, source_name or step_name)
    _set_view(context, step_name, src_df)
    return len(src_df), len(src_df)


def _transform_sql(
    step_name: str,
    step_action: str,
    target_path: str,
    params: dict,
    context: dict,
) -> tuple[int, int]:
    """
    Execute SQL (from a .sql file or inline) against PostgreSQL.
    If the query returns rows, store result in context and optionally write Parquet.
    """
    # Resolve SQL source: file or inline
    raw_sql = load_sql(step_action) if is_file_path(step_action) else step_action
    sql     = substitute(raw_sql, params)

    rows_affected, df = execute_sql(sql)

    if df is not None:
        _set_view(context, step_name, df)
        rows_out = len(df)
        if target_path:
            write_parquet(df, substitute(target_path, params))
    else:
        rows_out = rows_affected

    return 0, rows_out


def _write_parquet(
    step_name: str,
    source_name: str,
    target_path: str,
    context: dict,
) -> tuple[int, int]:
    """Write a named DataFrame from context to a Parquet file."""
    df   = _get_view(context, source_name or step_name)
    rows = write_parquet(df, target_path)
    return rows, rows


def _python(
    step_action: str,
    params: dict,
    context: dict,
) -> tuple[int, int]:
    """Execute a Python script or inline code block."""
    outcome = execute_python(step_action, params, context)
    return outcome.get("rows_in", 0), outcome.get("rows_out", 0)


# ── view registry helpers ─────────────────────────────────────

def _set_view(context: dict, name: str, df: pd.DataFrame) -> None:
    context.setdefault("views", {})[_key(name)] = df
    print(f"[CONTEXT] Registered view '{_key(name)}' ({len(df):,} rows)")


def _get_view(context: dict, name: str) -> pd.DataFrame:
    key   = _key(name)
    views = context.get("views", {})
    if key not in views:
        raise KeyError(
            f"[StepExecutor] View '{key}' not found. "
            f"Available: {list(views.keys())}"
        )
    return views[key]


def _key(name: str) -> str:
    """Normalise a step or resource name to a safe dict key."""
    return name.strip().lower().replace(" ", "_").replace("-", "_")
