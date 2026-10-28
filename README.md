# oms-order-tracing-demo

Cross-system order tracing on Deephaven: link **A** (Raptor: the client order and the destination order it routes to, tag 5011) → **B** (algo:
one parent per A destination order, many children) → **C** (order router: one parent per B child, many
children), reconcile executed and open quantity on each of the five edges (`Cli-Dest`, `A-Bp`, `Bp-Bc`,
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
| `src/oms_trace/config.py` | the six levels, five edges, break taxonomy, raw column contract, validated `Settings` (`OMS_TRACE_*`) |
| `src/oms_trace/mockdata.py` | a consistent six-level mock book and the scenario catalog of docs/design/09 §2 |
| `src/oms_trace/reference.py` | the pure-python oracle: every output table computed with dicts and loops |
| `src/oms_trace/dag.py` | the Deephaven DAG (docs/design/04): last state, role split, four links carrying the root, four edge rollups, feed health per channel, aged gaps, drift by root and by symbol, stats |
| `src/oms_trace/sources.py` | the raw contract as typed tables; a fixed clock; input tables for live tests |
| `src/oms_trace/ui_slice.py`, `dashboard.py` | the tree of a capped root selection; the `deephaven.ui` panels |
| `src/oms_trace/app.py` | `main(globals())`: mock / files / enterprise / amps source → DAG → exported tables |
| `src/oms_trace/amps_sow.py` | the AMPS SOW source (docs/design/11): one `sow_and_subscribe` per topic, the per-topic fold (snapshot, publish, `oof`, reconnect drops, `SowDeleted`), blink raw tables per system, the `amps_topics` status table that overrides feed health |
| `src/oms_trace/amps_publish.py`, `amps_testing.py` | the mock book into a broker's SOW topics; the client doubles the tests (and a console) play the broker with |
| `src/oms_trace/alerting.py` | pure: break transitions (`break_events` rows), the alert rules of docs/design/08 §2 as a deterministic policy (persistence, escalation, dedupe, summaries, growth, CLEAR, market hours), the one HTTP sink (Q9) |
| `src/oms_trace/events.py` | the engine glue: listeners on `gaps`, `unresolved_links` and the clock publishing `break_events` / `alert_events`; `break_history` and `alerts` (with the sink's `Posted`) as ring tables |
| `src/oms_trace/enterprise.py` | every `deephaven_enterprise` touchpoint (`db.live_table` sources, Kafka ingester options, Derived Table Writer options) |
| `src/oms_trace/trace_check.py` | the pre-cutover feed validation (docs/design/09 §6), JSON lines in/out |
| `queries/oms_trace/` | the PQ script bodies the controller pulls from Git: `lineage.py`, `ingest.py` (Kafka), `ingest_amps.py` (AMPS → Derived Table Writer), `drift_writer.py` (`break_events` → `OMS_TRACE.DriftEvents`), `ui.py` (the desks' dashboard PQ) |
| `schemas/`, `pq/`, `deploy/` | `OMS_RAW.Orders{A,B,C}` and `OMS_TRACE.DriftEvents` schema XML; one JSON definition per `oms-*` PQ (`oms_trace.pqdefs` validates and applies them); the deployment settings template |
| `scripts/check_env.sh`, `deploy_schemas.sh`, `deploy_dis.sh`, `deploy_git_repo.sh`, `build_wheel.sh`, `install_worker_venv.sh`, `deploy_pqs.sh` | the Podman on-prem deployment kit, step by step: [`docs/deployment.md`](docs/deployment.md) |
| `docker/`, `scripts/run_demo.sh` | the Community demo on `deephaven/server:42.5`; `docker-compose.amps.yml` + `docker/amps/` run it over a real AMPS broker |
| `tests/` | 56 pure-python tests; 15 embedded-engine tests behind `OMS_TRACE_DH_TEST=1` (`.github/workflows/tests.yml` runs both) |

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
the wall clock. For the AMPS source they deliver the same book as SOW messages through a stub
client and prove the lineage over the blink tables equals the oracle, that a late child published
to the SOW heals its parent, that an `oof` leaves a `SowDeleted` state that still explains its
parent's fills, and that a disconnect turns every topic `STALE`, the re-snapshot `CATCHING_UP`
and its completion `CURRENT` without changing a finding.

`run_tests.sh` installs the `amps` extra (`amps-python-client`, a binary wheel) so the fold is
also driven with real `AMPS.Message` objects; `OMS_TRACE_SKIP_AMPS=1` leaves it out and those
tests skip.

For break events and alerts the engine test drives input tables and a clock it advances: after
every step the `break_history` rows equal the oracle's diff of consecutive states, the `alerts`
rows equal the shared policy run on the oracle's observations, and every alert reaches a local
HTTP server once with `Posted = 200`.

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

### Over AMPS SOW topics

```bash
bash scripts/run_demo.sh up-amps         # derived image (server + amps-python-client), a local broker, OMS_TRACE_SOURCE=amps
bash scripts/run_demo.sh publish-amps    # the scenario book into the ten SOW topics; the panels fill as the states land
bash scripts/run_demo.sh down-amps
```

The broker image is `AMPS_IMAGE` in `repos.env` (60East publishes none; build one from your
distribution or point the key at your registry). Against your firm's AMPS, drop the `broker`
profile and set `OMS_AMPS_URI` and `OMS_AMPS_TOPICS_{A,B,C}`; the `amps_topics` panel shows
every subscription's epoch, snapshot and connection state. `python -m oms_trace.amps_publish
--delete <topic> <order id>` sends a `sow_delete`, and the record's last state stays in the
lineage flagged `SowDeleted` (docs/design/11 §4.1).

## Validate a real feed before cutover

```bash
.venv/bin/python -m oms_trace.trace_check --dir /path/with/orders_{a,b,c}.jsonl [--json]
```

Exit code 1 on a contract violation (empty-string parent ids, children carrying `UpstreamRef`,
resolution below 99.9 % on a hop, two children on a 1 : 1 hop). On the mock book it flags
exactly the scenarios built to violate those rules.

## Enterprise

Deploying to a Podman on-prem cluster, step by step with the scripts of this repository: [`docs/deployment.md`](docs/deployment.md).

- `queries/oms_trace/lineage.py` is the `oms-lineage` PQ body (`OMS_TRACE_SOURCE=enterprise`,
  `db.live_table` over `OMS_RAW.Orders{A,B,C}`); `queries/oms_trace/ingest.py` the per-topic Kafka
  ingester PQ. Both import `oms_trace` through the controller's Git module path.
- The ingester identifiers follow the Core+ documentation as read at design time and are marked
  **confirm on 2026.01.054**; `src/oms_trace/enterprise.py` is the only file that changes if a
  name differs. That confirmation is milestone M2's first task (docs/design/10).
- Open layout question for M2: one table per system (each topic's ingester in its own internal
  partition, preferred) or one table per topic merged in the adapter (`OMS_TRACE_ONE_TABLE_PER_SYSTEM=false`).
- **Break history and alerts.** `queries/oms_trace/drift_writer.py` is the `oms-drift-writer` PQ:
  it subscribes to the lineage PQ's `break_events` with the Core+ client inside the worker and
  appends them to `OMS_TRACE.DriftEvents` (`schemas/OMS_TRACE.DriftEvents.schema`, DIS `oms_trace`
  in `routing/dis.sh`) through the Derived Table Writer, alerts included. `OMS_TRACE_ALERT_URL` on
  the lineage PQ turns on the HTTP sink; the `alerts` table is the record either way.
- **AMPS instead of Kafka** (docs/design/11, decision D16): the AMPS client runs inside a worker
  either way. `queries/oms_trace/ingest_amps.py` is the `oms-ingest-amps-<x>` PQ (Live Query -
  Merge Server) that persists the state stream to `OMS_RAW.Orders<X>` through the Derived Table
  Writer on the same `oms_ingest_<x>` DIS, so the lineage PQ is unchanged; `OMS_TRACE_SOURCE=amps`
  in the lineage PQ itself skips the DIS for a first UAT. `amps-python-client` must be in the
  worker's Python environment. The writer's identifiers are marked **confirm on 2026.01.054** like
  the Kafka ones; `enterprise.dtw_options` holds every value.

## Settings

Every knob of docs/design/03 §8 and 07 §8 is an environment variable (`OMS_TRACE_GAP_AGE_SEC`,
`OMS_TRACE_UNRESOLVED_ALERT_SEC`, `OMS_TRACE_FEED_STALE_SEC`, `OMS_TRACE_FEED_CATCHUP_SEC`,
`OMS_TRACE_AGE_CLOCK=recv|event`, `OMS_TRACE_DATE_WINDOW_DAYS`, `OMS_TRACE_UI_MAX_ROOTS`,
`OMS_TOPICS_{A,B,C}`, `OMS_TRACE_ONE_TABLE_PER_SYSTEM`, ...). A malformed value is a start-up
error that names the key. The AMPS source adds `OMS_AMPS_URI` (one or more, comma separated),
`OMS_AMPS_TOPICS_{A,B,C}` (default: the `OMS_TOPICS_*` lists), `OMS_AMPS_FORMAT=json|fix`,
`OMS_AMPS_FIELD_MAP` (`bodyField=RawColumn,...`), `OMS_AMPS_TAG_MAP` (`tag=RawColumn,...`; the
parent and upstream tags of a FIX body must be mapped), `OMS_AMPS_HEARTBEAT_SEC` (5),
`OMS_AMPS_CLIENT_NAME`, `OMS_AMPS_BATCH_SIZE`, `OMS_AMPS_TIMEOUT_MS`, `OMS_AMPS_MAX_PENDING`,
`OMS_AMPS_MODE=sow|bookmark` (the ingester PQ runs `bookmark`: a transaction-log subscription that
resumes from a file-backed store under `OMS_AMPS_BOOKMARK_DIR`, first start per
`OMS_AMPS_BOOKMARK_FIRST=EPOCH|NOW`) and, in `sow` mode, `OMS_AMPS_PERSIST_SNAPSHOT`. Alerting adds `OMS_TRACE_ALERT_URL`,
`OMS_TRACE_ALERT_AUTH_HEADER` (`Name: value`, from the secret store), `OMS_TRACE_MARKET_HOURS`
(`09:30-16:00` in `OMS_TRACE_TZ`; empty = always) and the `OMS_TRACE_ALERT_*` thresholds of
docs/design/03 §8 (page counts, persistence windows, growth factor, ring size).

## Status and what is not in this repository yet

Implemented and verified on the engine: the lineage DAG with executed- and open-quantity
reconciliation, per-channel feed health, the taxonomy, the roots and symbols rollups, the UI
slice and dashboard (with the alerts and break-events panels), the mock, files and AMPS SOW
sources (with the snapshot-aware feed-health override and the `SowDeleted` policy), the break
transitions, the alert rules with the HTTP sink, the pre-cutover check, the CI workflow.

Written for Enterprise but unverified without a cluster: the Kafka ingester PQ, the AMPS →
Derived Table Writer ingester PQ, the drift-events writer PQ, the schemas and routing
(identifiers marked confirm on 2026.01.054). Designed, not yet built (docs/design/10
milestones): execution-level identification (`exec_breaks`, phase P2b), the EOD summary,
edge-family sharding (contingency).
