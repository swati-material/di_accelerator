# ============================================================
# utils/file_loader.py
# Reads SQL and Python source files from disk.
# Provides a helper to detect whether a step_action value is a
# file path or inline content.
# ============================================================

import os


def load_sql(path: str) -> str:
    """Read and return the contents of a .sql file."""
    _assert_exists(path)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def load_python(path: str) -> str:
    """Read and return the contents of a .py file."""
    _assert_exists(path)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def is_file_path(value: str) -> bool:
    """
    Return True when value looks like a file path rather than inline content.
    Heuristic: the stripped value ends with .sql or .py.
    The file does not need to exist yet — existence is checked at load time.
    """
    if not value:
        return False
    v = value.strip()
    return v.endswith(".sql") or v.endswith(".py")


# ── internal ─────────────────────────────────────────────────

def _assert_exists(path: str) -> None:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"[FileLoader] File not found: {path}")
