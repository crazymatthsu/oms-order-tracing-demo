"""oms-trace-ui: the desks' dashboard over the small tables of oms-lineage (docs/design/07 section 4).

Live Query (Script), Core+ Python, query dispatcher, Dependent on oms-lineage. Environment:
DH_CONNECTION_JSON_URL, DH_UI_KEYFILE (the service account's private key; never a password),
OMS_LINEAGE_PQ (default oms-lineage), OMS_TRACE_* settings.

Why a separate PQ: the desks get viewer rights on this PQ only (doc 07 section 5), the lineage
worker's update graph never serves UI subscriptions, and a dashboard restart does not rebuild the
lineage. It subscribes with the Core+ client inside the worker to tables that are small by
construction (aggregates, breaks, alerts, health); never to the node tables. The lineage tree
of a selected root needs the node tables, so this dashboard shows every panel but the tree; the
tree is on the lineage PQ's own `oms_trace_dashboard`, which operators open directly.

The client identifiers follow the Core+ Python documentation as read at design time
(SessionManager, private_key, connect_to_persistent_query, open_table, barrage_subscribe);
CONFIRM on Enterprise 2026.01.054 before the first run.
"""

import os
import sys

_SRC = os.environ.get("OMS_TRACE_SRC", "")
if _SRC and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from oms_trace import config, dashboard  # noqa: E402

settings = config.load_settings()
LINEAGE_PQ = os.environ.get("OMS_LINEAGE_PQ", "oms-lineage")
SMALL_TABLES = ("drift_by_a_order", "drift_edges", "feed_health", "unresolved_links", "alerts", "break_history",
                "trace_stats", "system_health", "drift_by_symbol")

# --- the Core+ client inside the worker (doc 07 section 4): confirm on 2026.01.054 ---------------------
from deephaven_enterprise.client.session_manager import SessionManager  # noqa: E402

sm = SessionManager(os.environ["DH_CONNECTION_JSON_URL"])
sm.private_key(os.environ["DH_UI_KEYFILE"])
lineage = sm.connect_to_persistent_query(LINEAGE_PQ)
tables = {name: lineage.barrage_subscribe(lineage.open_table(name)) for name in SMALL_TABLES}
globals().update(tables)

oms_trace_dashboard = dashboard.build(tables, settings, tree=False)
print(f"[oms-trace-ui] dashboard over {LINEAGE_PQ}: {', '.join(SMALL_TABLES)}", flush=True)
