"""Cross-system order tracing: A (Raptor) -> B (algo) -> C (order router).

The design is ``docs/design``; this package is its implementation.

* ``config``      the fixed five-level topology, the four edges, settings from the environment
* ``mockdata``    a five-level mock book and the scenario catalog (docs/design/09 section 2)
* ``reference``   the pure-python oracle: every output table computed with dicts and loops
* ``sources``     raw tables for the Deephaven layer: mock (and, in a Core+ worker, ``enterprise``)
* ``dag``         the Deephaven table graph (docs/design/04): last state, links, edges, aging,
                  feed health, aggregates
* ``ui_slice``    the tree for a selection of roots
* ``dashboard``   the deephaven.ui panels
* ``app``         ``main(globals())``: builds everything and exports the tables
* ``trace_check`` the pre-cutover feed validation (docs/design/09 section 6)
* ``enterprise``  every ``deephaven_enterprise`` import, isolated (docs/design/10 section 2)
"""

__all__ = ["config", "mockdata", "reference", "sources", "dag", "ui_slice", "dashboard", "app", "trace_check"]
