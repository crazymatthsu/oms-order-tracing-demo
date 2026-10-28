"""oms-ingest-amps-<x>: one system's AMPS order states into OMS_RAW.Orders<X> through the Derived
Table Writer (docs/design/11 section 5, decision D17).

The subscription is a bookmark subscribe over the topics' transaction log (OMS_AMPS_MODE=bookmark,
the default here): the client resumes from the bookmark it persisted under OMS_AMPS_BOOKMARK_DIR
(shared storage, so a PQ restarted on another node finds it), a reconnect replays only what was
missed, and nothing is a re-snapshot, so the writer sees each message once (an at-least-once
window of one flush on a crash). The first start of a trading day replays from EPOCH or NOW
(OMS_AMPS_BOOKMARK_FIRST) within the journal's retention. OMS_AMPS_MODE=sow is the older
snapshot path with OMS_AMPS_PERSIST_SNAPSHOT.

Live Query - Merge Server, Core+ Python. Environment: OMS_SYSTEM (A|B|C), OMS_AMPS_URI (credentials
from the environment's secret store, never from this file), OMS_AMPS_TOPICS_<X>, OMS_AMPS_FORMAT,
OMS_AMPS_FIELD_MAP / OMS_AMPS_TAG_MAP, OMS_AMPS_HEARTBEAT_SEC, OMS_TRACE_TZ, DIS_STORAGE_ROOT, and
OMS_AMPS_PERSIST_SNAPSHOT=true on the first start of a trading day only (the snapshot rows fill the
empty partition; a restart within the day persists the change rows alone, the DIS already holds
the states).

The AMPS client runs in this worker (amps-python-client must be in the worker's Python
environment, docs/design/11 section 6); the state stream is a blink table, which is what the
Derived Table Writer takes. The lineage PQ (lineage.py) is unchanged: it reads db.live_table over
OMS_RAW.Orders<X> whatever transport fed it.

The Enterprise identifiers below follow the Core+ "Derived Table Writer" page and javadoc as read
at design time (DataImportServerTools.getDisByNameWithStorage, DerivedTableWriter.ingestTable with
namespace, tableName, columnPartition, internalPartition, table, options; DerivedTableWriter.Options
with sequenceColumn). CONFIRM their names and parameter order on Enterprise 2026.01.054 before the
first run (docs/design/05 section 11); oms_trace.enterprise.dtw_options holds every value in one
place.
"""

import os
import sys

_SRC = os.environ.get("OMS_TRACE_SRC", "")
if _SRC and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from oms_trace import amps_sow, config, enterprise  # noqa: E402

os.environ.setdefault("OMS_AMPS_MODE", "bookmark")

settings = config.load_settings()
SYSTEM = os.environ["OMS_SYSTEM"]
amps = amps_sow.AmpsSettings.from_env(settings=settings).for_system(SYSTEM)

# The source connects in the background and keeps reconnecting; its tables exist at once.
source = amps_sow.start_source(settings, amps)
states = source.persist_table(SYSTEM)       # change rows (+ snapshot rows when OMS_AMPS_PERSIST_SNAPSHOT)
amps_states = source.raw_tables()[SYSTEM]   # exported: everything the mirror emits, with SowStream / SowEpoch
amps_topics = source.topics_table()         # exported: this PQ's own health view (Epoch, SnapshotInProgress, ...)
opts = enterprise.dtw_options(settings, SYSTEM)

# --- Enterprise: confirm on 2026.01.054 --------------------------------------------------------------
import jpy  # noqa: E402

DataImportServerTools = jpy.get_type("io.deephaven.enterprise.dataimportserver.DataImportServerTools")
DerivedTableWriter = jpy.get_type("io.deephaven.enterprise.derivedtablewriter.DerivedTableWriter")

dis = DataImportServerTools.getDisByNameWithStorage(opts["dis_name"], opts["storage"])  # routing/dis.sh: private storage, claims OMS_RAW.Orders<X>
options = DerivedTableWriter.Options()
# options = options.sequenceColumn(opts["sequence_column"])  # only with a monotonic per-partition sequence (AMPS rows have none)
DerivedTableWriter.ingestTable(dis, opts["namespace"], opts["table_name"], opts["column_partition"],
                               opts["internal_partition"], states.j_table, options)
print(f"[oms-ingest-amps] {SYSTEM} {', '.join(amps.topics[SYSTEM])} -> {opts['namespace']}.{opts['table_name']}"
      f" partition {opts['column_partition']}/{opts['internal_partition']}"
      f" ({amps.mode} mode: {'every' if amps.mode == 'bookmark' or amps.persist_snapshot else 'change'} rows)", flush=True)
