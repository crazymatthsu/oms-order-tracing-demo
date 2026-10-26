# oms-order-tracing-demo

Cross-system order tracing on Deephaven: link **A** (Raptor, the client order) → **B** (algo:
one parent per root, many children) → **C** (order router: one parent per B child, many
children), reconcile executed and open quantity on each of the four edges (`A-Bp`, `Bp-Bc`,
`Bc-Cp`, `Cp-Cc`), and name the system and book that is short.

Built for Deephaven Enterprise 2026.01.054 (Core+ on Community Core 42.5). The lineage logic is
Community-Core table operations and runs unchanged in a Community server, which is what the demo
and the tests use; the Enterprise pieces (ingestion into a Data Import Server, `db.live_table`
sources, persistent queries) are isolated in one module and two PQ scripts.

The design is [`docs/design/`](docs/design/README.md) and is binding for this code. The
architecture note it reviews is [`docs/original-architecture-note.md`](docs/original-architecture-note.md).

## What is here

| Path | What |
|---|---|
| `src/oms_trace/config.py` | the five levels, four edges, break taxonomy, raw column contract, validated `Settings` (`OMS_TRACE_*`) |
| `src/oms_trace/mockdata.py` | a consistent five-level mock book and the scenario catalog of docs/design/09 §2 |
| `src/oms_trace/reference.py` | the pure-python oracle: every output table computed with dicts and loops |
| `src/oms_trace/dag.py` | the Deephaven DAG (docs/design/04): last state, role split, four links carrying the root, four edge rollups, feed health per channel, aged gaps, drift by root and by symbol, stats |
| `src/oms_trace/sources.py` | the raw contract as typed tables; a fixed clock; input tables for live tests |
| `src/oms_trace/ui_slice.py`, `dashboard.py` | the tree of a capped root selection; the `deephaven.ui` panels |
| `src/oms_trace/app.py` | `main(globals())`: mock / files / enterprise source → DAG → exported tables |
| `src/oms_trace/enterprise.py` | every `deephaven_enterprise` touchpoint (`db.live_table` sources, ingester options) |
| `src/oms_trace/trace_check.py` | the pre-cutover feed validation (docs/design/09 §6), JSON lines in/out |
| `queries/oms_trace/` | the PQ script bodies (`lineage.py`, `ingest.py`) the controller pulls from Git |
| `schemas/`, `routing/`, `pq/` | `OMS_RAW.Orders{A,B,C}` schema XML, `dhconfig dis` commands, PQ exports folder |
| `docker/`, `scripts/run_demo.sh` | the Community demo on `deephaven/server:42.5` |
| `tests/` | 26 pure-python tests; 11 embedded-engine tests behind `OMS_TRACE_DH_TEST=1` |

## Run the tests

```bash
bash run_tests.sh                          # pure python: config, mock book, oracle, check CLI
OMS_TRACE_DH_TEST=1 bash run_tests.sh      # + the engine: installs deephaven-server 42.5 (~600 MB), needs JDK 17+
# JAVA_HOME must point at the JDK (e.g. export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64)
```

The engine tests assert that every DAG table equals the oracle row for row on the scenario book
(links, edges, feed health, gaps, unresolved links, roots, symbols, stats), that a B child
arriving late on an input table resolves its C parent and re-roots the C children in place, that
the UI slice builds the tree of a root, and that the app builds on the mock source re-based to
the wall clock.

## Run the demo

```bash
bash scripts/run_demo.sh up      # podman compose (or docker compose) on the image from repos.env
# open http://localhost:10000/ide -> Panels: oms_trace_dashboard, drift_by_a_order, drift_edges,
#                                             feed_health, unresolved_links, gaps, trace_stats
bash scripts/run_demo.sh down
```

The book is the scenario catalog plus random healthy families, shifted to the current time at
start-up, so aging and feed health run live: the `TR` scenario turns from `TRANSIENT` into drift
after 30 s, the `oms.c.stale` channel is `STALE`, `oms.b.catchup` is `CATCHING_UP`. Every image
and the pip index come from `repos.env`; a JFrog-proxied company overrides them in
`repos.local.env` (see `repos.local.env.example`).

`OMS_TRACE_SOURCE=files` reads `orders_{a,b,c}.jsonl` from `docker/files`
(`bash scripts/run_demo.sh export-files` writes the mock book there as a template).

## Validate a real feed before cutover

```bash
.venv/bin/python -m oms_trace.trace_check --dir /path/with/orders_{a,b,c}.jsonl [--json]
```

Exit code 1 on a contract violation (empty-string parent ids, children carrying `UpstreamRef`,
resolution below 99.9 % on a hop, two children on a 1 : 1 hop). On the mock book it flags
exactly the scenarios built to violate those rules.

## Enterprise

- `queries/oms_trace/lineage.py` is the `oms-lineage` PQ body (`OMS_TRACE_SOURCE=enterprise`,
  `db.live_table` over `OMS_RAW.Orders{A,B,C}`); `queries/oms_trace/ingest.py` the per-topic Kafka
  ingester PQ. Both import `oms_trace` through the controller's Git module path.
- The ingester identifiers follow the Core+ documentation as read at design time and are marked
  **confirm on 2026.01.054**; `src/oms_trace/enterprise.py` is the only file that changes if a
  name differs. That confirmation is milestone M2's first task (docs/design/10).
- Open layout question for M2: one table per system (each topic's ingester in its own internal
  partition, preferred) or one table per topic merged in the adapter (`OMS_TRACE_ONE_TABLE_PER_SYSTEM=false`).

## Settings

Every knob of docs/design/03 §8 and 07 §8 is an environment variable (`OMS_TRACE_GAP_AGE_SEC`,
`OMS_TRACE_UNRESOLVED_ALERT_SEC`, `OMS_TRACE_FEED_STALE_SEC`, `OMS_TRACE_FEED_CATCHUP_SEC`,
`OMS_TRACE_AGE_CLOCK=recv|event`, `OMS_TRACE_DATE_WINDOW_DAYS`, `OMS_TRACE_UI_MAX_ROOTS`,
`OMS_TOPICS_{A,B,C}`, `OMS_TRACE_ONE_TABLE_PER_SYSTEM`, ...). A malformed value is a start-up
error that names the key.

## Status and what is not in this repository yet

Implemented and verified on the engine: the lineage DAG with executed- and open-quantity
reconciliation, per-channel feed health, the taxonomy, the roots and symbols rollups, the UI
slice and dashboard, the mock and files sources, the pre-cutover check.

Designed, not yet built (docs/design/10 milestones): execution-level identification
(`exec_breaks`, phase P2b), the break-transition blink table and the Derived Table Writer to
`OMS_TRACE.DriftEvents`, the alert sink, the EOD summary, edge-family sharding (contingency),
and the Enterprise ingestion itself, which needs a cluster.
