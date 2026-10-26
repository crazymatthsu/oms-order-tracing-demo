"""oms-ingest-<x>-<group>: one Core+ Kafka ingester per topic (docs/design/05 section 4).

Live Query - Merge Server, Core+ Python. Environment: OMS_SYSTEM (A|B|C), OMS_TOPICS (this
PQ's comma-separated topics), KAFKA_BOOTSTRAP and the Kafka security properties from the
environment's secrets, OMS_TRACE_ONE_TABLE_PER_SYSTEM.

The ingester identifiers below are the ones the Core+ "Ingest Kafka Data" page names
(kafka_table_writer_options, get_dis_by_name, dis.consume, OnReject). CONFIRM their import
path and signature on Enterprise 2026.01.054 before the first run (docs/design/05 section 11);
oms_trace.enterprise.ingester_options holds every option in one place.
"""

import os
import sys

_SRC = os.environ.get("OMS_TRACE_SRC", "")
if _SRC and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from oms_trace import config, enterprise  # noqa: E402

settings = config.load_settings()
SYSTEM = os.environ["OMS_SYSTEM"]
TOPICS = [t.strip() for t in os.environ["OMS_TOPICS"].split(",") if t.strip()]
kafka_props = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    # security.protocol / sasl.* come from the environment's secret store, never from this file
    **{k[len("KAFKA_PROP_"):].replace("_", ".").lower(): v for k, v in os.environ.items() if k.startswith("KAFKA_PROP_")},
}

# --- Enterprise imports: confirm on 2026.01.054 ---------------------------------------------
# from deephaven_enterprise.<kafka module> import kafka_table_writer_options, get_dis_by_name, OnReject
# from deephaven.stream.kafka.consumer import KeyValueSpec, json_spec
# from deephaven.table import TableDefinition
# from deephaven import dtypes as dt

dis = get_dis_by_name(f"oms_ingest_{SYSTEM.lower()}")  # noqa: F821 - resolved in the worker; private storage, claims OMS_RAW.Orders<X>
for topic in TOPICS:
    opts = enterprise.ingester_options(settings, SYSTEM, topic, kafka_props, one_table=settings.one_table_per_system)
    value_def = TableDefinition({  # noqa: F821
        "OrderId": dt.string, "UpstreamRef": dt.string, "ParentOrderId": dt.string, "Symbol": dt.string, "Side": dt.string,  # noqa: F821
        "OrderQty": dt.long, "CumQty": dt.long, "LeavesQty": dt.long, "AvgPx": dt.double, "OrdStatus": dt.string,  # noqa: F821
        "EventTs": dt.Instant, "SeqNo": dt.long,  # noqa: F821
    })
    writer_opts = kafka_table_writer_options(  # noqa: F821
        dis_name=opts["dis_name"],
        namespace=opts["namespace"],
        table_name=opts["table_name"],
        topic=opts["topic"],
        kafka_properties=opts["kafka_properties"],
        key_spec=KeyValueSpec.IGNORE,  # noqa: F821 - the order id is also in the value
        value_spec=json_spec(value_def),  # noqa: F821
        # partition_func: the row's RecvTs as yyyy-MM-dd in opts["timezone"] (docs/design/05 §4.2)
        # internal_partition=opts["internal_partition"]: the preferred one-table-per-system layout
        on_reject=OnReject.LOG,  # noqa: F821 - never TERMINATE in production
    )
    dis.consume(writer_opts)
    print(f"[oms-ingest] {SYSTEM} {topic} -> {opts['namespace']}.{opts['table_name']}", flush=True)
