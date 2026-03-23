import json
import os
from datetime import datetime

def write_audit_record(audit_path: str, record: dict) -> None:
    os.makedirs(audit_path, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(audit_path, f"audit_{ts}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, default=str)
