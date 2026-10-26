# oms-order-tracing-demo

Cross-system order tracing on Deephaven: link **A** (Raptor, the client order) → **B** (algo:
one parent per root, many children) → **C** (order router: one parent per B child, many
children), reconcile executed and open quantity on each of the four edges, and name the system
and book that is short. Built for Deephaven Enterprise 2026.01.054 (Core+ on Community Core
42.5); the lineage logic is Community-Core table operations and runs in a Community server for
the demo.

The design is in [`docs/design/`](docs/design/README.md) (binding for this code); the original
architecture note it reviews is [`docs/original-architecture-note.md`](docs/original-architecture-note.md).

Status: implementation in progress — see the pull request description for what is built and
verified.
