# ============================================================
# utils/param_utils.py
# Builds a flat param dict from config.step_param rows and
# substitutes ${key} placeholders into SQL / file paths.
# ============================================================

import re


def build_param_dict(params: list[dict]) -> dict:
    """
    Flatten a list of config.step_param rows into a key → value dict.

    Primary key:   param_name  → param_value
    Also surfaces: delta_to_pull, fields, resource_group as top-level
                   keys when not already occupied by param_name.

    Example:
        [{"param_name": "delimiter", "param_value": "|", "delta_to_pull": "full_refresh"}]
        → {"delimiter": "|", "delta_to_pull": "full_refresh"}
    """
    result: dict = {}

    for p in params:
        name  = (p.get("param_name") or "").strip()
        value = p.get("param_value") or ""

        if name:
            result[name] = value

        # Surface standard metadata fields unless a param already owns the key
        for special in ("delta_to_pull", "fields", "resource_group"):
            if p.get(special) and special not in result:
                result[special] = p[special]

    return result


def substitute(template: str, params: dict) -> str:
    """
    Replace every ${key} placeholder in template with the matching value
    from params. Unresolved placeholders are left unchanged.

    Example:
        substitute("SELECT * FROM ${table} WHERE dt = '${date}'",
                   {"table": "orders", "date": "2026-03-25"})
        → "SELECT * FROM orders WHERE dt = '2026-03-25'"
    """
    def _replace(match: re.Match) -> str:
        key = match.group(1)
        return str(params.get(key, match.group(0)))   # keep ${key} if missing

    return re.sub(r"\$\{(\w+)\}", _replace, template)
