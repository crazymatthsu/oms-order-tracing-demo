"""Raw rows as JSON lines, one file per system (``orders_a.jsonl`` ...), pure python.

Used by ``OMS_TRACE_SOURCE=files`` and by the pre-cutover check. Timestamps are epoch
nanoseconds or ISO-8601 strings; every raw contract column is kept, missing ones read as null.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from oms_trace import config


def _ts(value: Any) -> Optional[int]:
    if value is None or isinstance(value, int):
        return value
    return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1e9)


def rows_from_dir(directory: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for system in config.SYSTEMS:
        path = os.path.join(directory, f"orders_{system.lower()}.jsonl")
        rows: List[Dict[str, Any]] = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    for col in ("RecvTs", "EventTs"):
                        row[col] = _ts(row.get(col))
                    rows.append({name: row.get(name) for name in config.RAW_COLUMN_NAMES})
        out[system] = rows
    return out


def write_dir(directory: str, rows_by_system: Dict[str, List[Dict[str, Any]]]) -> None:
    os.makedirs(directory, exist_ok=True)
    for system in config.SYSTEMS:
        with open(os.path.join(directory, f"orders_{system.lower()}.jsonl"), "w", encoding="utf-8") as fh:
            for r in rows_by_system.get(system, []):
                fh.write(json.dumps(r) + "\n")
