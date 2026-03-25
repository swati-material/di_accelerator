# ============================================================
# executors/python_executor.py
# Executes a Python script supplied via step_action.
#
# The script receives two injected globals:
#   params  — merged param dict from utils.param_utils
#   context — shared runner context (views dict, job metadata)
#
# The script must set a module-level variable named `result`:
#   result = {"rows_in": int, "rows_out": int}
#   Optionally: result["df"] = a pandas DataFrame to store in context.
#
# PySpark extension point is marked with a comment.
# ============================================================

import importlib.util
import os


def execute_python(
    script_path_or_code: str,
    params: dict,
    context: dict,
) -> dict:
    """
    Run a Python script (file path or inline code) with injected globals.

    The script can read shared DataFrames via:
        df = context["views"]["my_view_name"]

    And write results back via:
        result = {"rows_in": 100, "rows_out": 80, "df": transformed_df}

    Returns the `result` dict. Defaults rows to 0 if not set by the script.

    # ── PySpark extension point ──────────────────────────────
    # Add spark session to injected globals:
    #   globs["spark"] = spark_session
    # The script can then call spark.sql(...), df.show(), etc.
    # ────────────────────────────────────────────────────────
    """
    globs: dict = {
        "params":  params,
        "context": context,
        "result":  {"rows_in": 0, "rows_out": 0},
    }

    value = script_path_or_code.strip()

    if _is_file(value):
        _exec_file(value, globs)
    else:
        # Inline code block stored directly in step_action
        exec(compile(value, "<step_action>", "exec"), globs)  # noqa: S102

    return globs.get("result", {"rows_in": 0, "rows_out": 0})


# ── internal ─────────────────────────────────────────────────

def _is_file(value: str) -> bool:
    return value.endswith(".py") and os.path.isfile(value)


def _exec_file(path: str, globs: dict) -> None:
    """Load and execute a .py file, copying result back into globs."""
    spec   = importlib.util.spec_from_file_location("_step_script", path)
    module = importlib.util.module_from_spec(spec)
    module.__dict__.update(globs)
    spec.loader.exec_module(module)
    if hasattr(module, "result"):
        globs["result"] = module.result
