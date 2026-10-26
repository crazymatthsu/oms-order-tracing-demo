"""The application entry: build everything and export the tables into a namespace.

Used two ways:

* **Community demo** (Application Mode, ``docker/apps/oms-trace/main.py``):
  ``from oms_trace import app; app.main(globals())``
* **Enterprise** (``queries/oms_trace/lineage.py``, a Persistent Query script body): the same
  call with ``OMS_TRACE_SOURCE=enterprise``.

Sources (``OMS_TRACE_SOURCE``): ``mock`` (the scenario book, re-based to the wall clock so the
aging and feed-health logic runs live), ``files`` (JSON lines per system under
``OMS_TRACE_FILES_DIR``), ``enterprise`` (``db.live_table`` over the DIS tables).
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, MutableMapping

from oms_trace import config, dag, mockdata, sources

EXPORTED_RAW = {"A": "raw_orders_a", "B": "raw_orders_b", "C": "raw_orders_c"}


def _rebase(book: mockdata.Book, now_nanos: int) -> mockdata.Book:
    """Shift every timestamp so the book's NOW is the wall clock, and move the Dates with it."""
    from datetime import datetime, timedelta, timezone

    shift = now_nanos - book.now
    today = datetime.fromtimestamp(now_nanos / 1e9, tz=timezone.utc).date()
    dates = {mockdata.TODAY: str(today), mockdata.YESTERDAY: str(today - timedelta(days=1))}
    for rows in book.rows.values():
        for r in rows:
            r["RecvTs"] += shift
            if r.get("EventTs") is not None:
                r["EventTs"] += shift
            r["Date"] = dates.get(r["Date"], r["Date"])
    book.now = now_nanos
    return book


def _files_rows(directory: str) -> Dict[str, List[Dict[str, Any]]]:
    """``orders_a.jsonl`` etc.: one raw row per line, timestamps as epoch nanos or ISO-8601."""
    from datetime import datetime

    def ts(value):
        if value is None or isinstance(value, int):
            return value
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1e9)

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
                        row[col] = ts(row.get(col))
                    rows.append({name: row.get(name) for name in config.RAW_COLUMN_NAMES})
        out[system] = rows
    return out


def build(settings: config.Settings) -> Dict[str, Any]:
    """Raw tables for the configured source, then the DAG; returns every exported table."""
    exported: Dict[str, Any] = {}
    if settings.source == "mock":
        book = mockdata.generate(seed=settings.mock_seed, families=settings.mock_families)
        _rebase(book, time.time_ns())
        raw = sources.mock_tables(book)
        settings = settings.with_(as_of=None)  # the window follows the wall clock
    elif settings.source == "files":
        raw = {s: sources.raw_table(rows) for s, rows in _files_rows(os.environ.get("OMS_TRACE_FILES_DIR", "/oms-files")).items()}
    elif settings.source == "enterprise":
        from oms_trace import enterprise

        raw = enterprise.raw_tables(settings)
    else:  # pragma: no cover - load_settings validates the choice
        raise config.SettingsError(f"unknown OMS_TRACE_SOURCE {settings.source!r}")
    for system, table in raw.items():
        exported[EXPORTED_RAW[system]] = table
    exported.update(dag.build_lineage(raw, settings))
    return exported


def main(namespace: MutableMapping[str, Any], settings: config.Settings | None = None) -> Dict[str, Any]:
    """Build and export; also the ``oms_trace_dashboard`` when ``deephaven.ui`` is available."""
    settings = settings or config.load_settings()
    exported = build(settings)
    try:
        from oms_trace import ui_slice

        exported["lineage_nodes"] = lambda root_ids: ui_slice.lineage_nodes(exported, root_ids, settings)
    except Exception as exc:  # noqa: BLE001 - the tables are still exported
        print(f"[oms-trace] ui_slice unavailable: {type(exc).__name__}: {exc}", flush=True)
    try:
        from oms_trace import dashboard

        exported["oms_trace_dashboard"] = dashboard.build(exported, settings)
    except Exception as exc:  # noqa: BLE001 - deephaven.ui is optional
        print(f"[oms-trace] dashboard unavailable: {type(exc).__name__}: {exc}", flush=True)
    namespace.update(exported)
    print(f"[oms-trace] source={settings.source} window={settings.dates()} tables={len(exported)}", flush=True)
    return exported
