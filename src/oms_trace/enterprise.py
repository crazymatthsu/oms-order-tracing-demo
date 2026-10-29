"""Every ``deephaven_enterprise`` touchpoint, in one file (docs/design/10 section 2).

Resolved only inside a Core+ worker. The identifiers follow the Enterprise documentation for
release 1.202601 (build 2026.01.054) as read at design time; **confirm each one against the
installed release's pages** before the first deployment (docs/design/05 section 11) — that is
milestone M2's first task, and this is the only module it touches.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional

from oms_trace import config


def database(db: Optional[Any] = None) -> Any:
    """The worker's ``db``: passed in, or the Core+ module-level object, or the script global."""
    if db is not None:
        return db
    try:
        from deephaven_enterprise.database import db as core_db  # Core+ Python workers

        return core_db
    except Exception:  # noqa: BLE001 - fall back to the script namespace
        import __main__

        if hasattr(__main__, "db"):
            return __main__.db
        raise RuntimeError("no Enterprise `db` object: run inside a Core+ worker (or pass db=...)")


def raw_tables(settings: config.Settings, db: Optional[Any] = None) -> Dict[str, Any]:
    """``db.live_table`` over each system's raw table(s), merged per system (docs/design/05 §4.3b).

    The Date filter is applied by the DAG; one table per system when every topic's ingester
    writes its own internal partition, else one table per topic merged here.
    """
    from deephaven import merge

    db = database(db)
    out: Dict[str, Any] = {}
    for system in config.SYSTEMS:
        parts = [db.live_table(settings.raw_namespace, name) for name in settings.raw_table_names("Orders", system)]
        out[system] = parts[0] if len(parts) == 1 else merge(parts)
    return out


def ingester_options(settings: config.Settings, system: str, topic: str, kafka_props: Mapping[str, str],
                     one_table: bool = True) -> Dict[str, Any]:
    """The keyword arguments for the Core+ Kafka ingester of one topic (docs/design/05 section 4).

    Returned as a dict so the PQ script (``queries/oms_trace/ingest.py``) can pass them to
    ``kafka_table_writer_options`` once the signature is confirmed on the installed release.
    """
    table = f"Orders{system}" if one_table else f"Orders{system}_{topic.rsplit('.', 1)[-1]}"
    return {
        "dis_name": f"oms_ingest_{system.lower()}",
        "namespace": settings.raw_namespace,
        "table_name": table,
        "topic": topic,
        "kafka_properties": {**dict(kafka_props), "group.id": f"dh-oms-ingest-{system.lower()}-{topic}"},
        # key: the order id (also in the value); value: JSON with the raw contract minus the ingester columns
        "value_columns": [name for name, _ in config.RAW_COLUMNS
                          if name != "Date" and name not in config.INGESTER_COLUMNS],
        "partition_column": "Date",            # column partition, daily, in settings.tz, from RecvTs
        "internal_partition": topic,           # preferred layout: one table per system (confirm on 2026.01.054)
        "on_reject": "LOG",                    # never TERMINATE in production
        "timezone": settings.tz,
    }


def dtw_options(settings: config.Settings, system: str, column_partition: Optional[str] = None,
                internal_partition: str = "amps", sequence_column: Optional[str] = None) -> Dict[str, Any]:
    """The Derived Table Writer call for one system's AMPS state stream (docs/design/11 section 5).

    Returned as a dict so the PQ script (``queries/oms_trace/ingest_amps.py``) can pass the values to
    ``DerivedTableWriter.ingestTable(dis, namespace, tableName, columnPartition, internalPartition,
    table, options)`` once the signature is confirmed on the installed release. The column partition
    is today's Date in ``settings.tz`` (the PQ restarts daily, like the Kafka ingester); the internal
    partition names the transport so a Kafka-fed and an AMPS-fed day never share one.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    name = f"oms_ingest_{system.lower()}"
    return {
        "dis_name": name,
        "storage": f"{os.environ.get('DIS_STORAGE_ROOT', '/db/dataImportServers')}/{name}",  # routing/dis.sh
        "namespace": settings.raw_namespace,
        "table_name": f"Orders{system}",
        "column_partition": column_partition or datetime.now(ZoneInfo(settings.tz)).date().isoformat(),
        "internal_partition": internal_partition,
        # exactly-once across a restart needs a per-partition monotonic sequence; AMPS rows carry none,
        # so duplicates after a restart are possible and harmless under last_by(OrderID)
        "sequence_column": sequence_column,
    }


def drift_writer_options(settings: config.Settings, column_partition: Optional[str] = None,
                         internal_partition: str = "lineage", source_pq: str = "oms-lineage") -> Dict[str, Any]:
    """The Derived Table Writer call of ``oms-drift-writer`` (docs/design/05 section 8.1, 03 section 5.1).

    ``break_events`` of the lineage PQ, reached with the Core+ client inside the worker (doc 07
    section 4), appended to ``OMS_TRACE.DriftEvents`` on the ``oms_trace`` DIS (routing/dis.sh). The
    column partition is today's Date in ``settings.tz``; the PQ restarts daily (doc 07 section 2).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    name = "oms_trace"
    return {
        "dis_name": name,
        "storage": f"{os.environ.get('DIS_STORAGE_ROOT', '/db/dataImportServers')}/{name}",
        "namespace": settings.out_namespace,
        "table_name": "DriftEvents",
        "column_partition": column_partition or datetime.now(ZoneInfo(settings.tz)).date().isoformat(),
        "internal_partition": internal_partition,
        "source_pq": os.environ.get("OMS_LINEAGE_PQ", source_pq),
        "source_table": "break_events",
    }
