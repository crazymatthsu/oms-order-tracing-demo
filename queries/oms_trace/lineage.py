"""oms-lineage: the Persistent Query script body (docs/design/07 section 2).

Live Query (Script), Core+ Python, query dispatcher. Reads OMS_RAW.Orders{A,B,C} through
db.live_table over the Date window, builds every table of docs/design/03 section 4 and exports
them as this PQ's tables. Settings come from the PQ's environment (docs/design/07 section 8);
OMS_TRACE_SOURCE must be `enterprise`.

The Git-sourced module path mapping makes `oms_trace` importable (sibling doc
docs/git-persistent-query-scripts.md, "Modularizing Scripts"); until it is configured, the
sys.path line below points at the checkout the controller cloned.
"""

import os
import sys

_SRC = os.environ.get("OMS_TRACE_SRC", "")
if _SRC and _SRC not in sys.path:
    sys.path.insert(0, _SRC)

os.environ.setdefault("OMS_TRACE_SOURCE", "enterprise")

from oms_trace import app  # noqa: E402

app.main(globals())
