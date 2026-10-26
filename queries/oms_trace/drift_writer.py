"""oms-drift-writer: the lineage PQ's break_events -> OMS_TRACE.DriftEvents through the Derived
Table Writer (docs/design/05 section 8.1, 03 section 5.1, 07 section 4).

Live Query - Merge Server, Core+ Python, Dependent on oms-lineage. Environment:
DH_CONNECTION_JSON_URL (the cluster's connection.json), DH_UI_KEYFILE (a service account's private
key; never a password in a script), OMS_TRACE_OUT_NAMESPACE, OMS_TRACE_TZ, DIS_STORAGE_ROOT,
OMS_LINEAGE_PQ (default oms-lineage).

break_events is a blink table: one row per node whose BreakKind or ParentState changed, and one
row per alert (Level ALERT). It is small by construction, so a Barrage subscription inside this
worker is the right way to reach it (doc 07 section 4); the Derived Table Writer takes a blink or
add-only table and appends it to the DIS partition, which makes DriftEvents the durable break and
alert history. Why a separate PQ: writes to a DIS belong on the merge dispatcher, and the lineage
worker's update graph never waits on storage.

The Enterprise identifiers below follow the Core+ pages as read at design time: the client
(SessionManager, private_key, connect_to_persistent_query, open_table, barrage_subscribe) and the
writer (DataImportServerTools.getDisByNameWithStorage, DerivedTableWriter.ingestTable with
namespace, tableName, columnPartition, internalPartition, table, options; Options.onReject).
CONFIRM their names and parameter order on Enterprise 2026.01.054 before the first run (docs/design/05
section 11); oms_trace.enterprise.drift_writer_options holds every value in one place.
"""

import os
import sys

_SRC = os.environ.get("OMS_TRACE_SRC", "")
if _SRC and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from oms_trace import config, enterprise  # noqa: E402

settings = config.load_settings()
opts = enterprise.drift_writer_options(settings)

# --- the Core+ client inside the worker (doc 07 section 4): confirm on 2026.01.054 ---------------------
from deephaven_enterprise.client.session_manager import SessionManager  # noqa: E402

sm = SessionManager(os.environ["DH_CONNECTION_JSON_URL"])
sm.private_key(os.environ["DH_UI_KEYFILE"])
lineage = sm.connect_to_persistent_query(opts["source_pq"])                       # the PQ must be running (Dependent schedule)
break_events = lineage.barrage_subscribe(lineage.open_table(opts["source_table"]))  # a live local copy of the blink table

# --- the Derived Table Writer: confirm on 2026.01.054 --------------------------------------------------
import jpy  # noqa: E402

DataImportServerTools = jpy.get_type("io.deephaven.enterprise.dataimportserver.DataImportServerTools")
DerivedTableWriter = jpy.get_type("io.deephaven.enterprise.derivedtablewriter.DerivedTableWriter")

dis = DataImportServerTools.getDisByNameWithStorage(opts["dis_name"], opts["storage"])  # routing/dis.sh: claims OMS_TRACE
options = DerivedTableWriter.Options()
# options = options.onReject(DerivedTableWriter.OnReject.LOG)   # never TERMINATE in production (doc 05 section 8.1)
# The table carries exactly the non-partitioning columns of schemas/OMS_TRACE.DriftEvents.schema; the Date
# partition is the argument. If the installed writer wants the partitioning column present as well:
#   break_events = break_events.update_view([f"Date = `{opts['column_partition']}`"])
DerivedTableWriter.ingestTable(dis, opts["namespace"], opts["table_name"], opts["column_partition"],
                               opts["internal_partition"], break_events.j_table, options)
print(f"[oms-drift-writer] {opts['source_pq']}.{opts['source_table']} -> {opts['namespace']}.{opts['table_name']}"
      f" partition {opts['column_partition']}/{opts['internal_partition']}", flush=True)
