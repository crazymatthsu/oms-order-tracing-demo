"""App entrypoint for the Community demo: build the lineage and export the tables.

/oms-scripts is the mounted ``src`` folder (PYTHONPATH is set in the compose file because
Application Mode scripts run with ``__file__`` unset). ``globals()`` is passed on purpose: the
tables must land in this module's namespace to become server-side globals.
"""

import sys

if "/oms-scripts" not in sys.path:
    sys.path.insert(0, "/oms-scripts")

from oms_trace import app  # noqa: E402

app.main(globals())
