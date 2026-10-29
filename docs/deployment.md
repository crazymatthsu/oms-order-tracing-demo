# Deploying to Deephaven Enterprise (Podman on-prem)

The runbook for docs/design/07 §9, with the scripts and files of this repository. One pass per
environment (UAT, then production); every step is idempotent and prints the `dhconfig` commands it
runs, so a flag that differs on the installed release is visible at once. Everything marked
**confirm on 2026.01.054** follows the Core+ documentation as read at design time (docs/design/05
§11): read the installed release's page once, adjust the one place named, and the rest follows.

| Step | What | Where it runs | Script / file |
|---|---|---|---|
| 0 | Settings for the cluster (no secrets) | your laptop or the infra node | `deploy/deploy.env` from `deploy/deploy.env.example` |
| 1 | Is the cluster what the design expects | anywhere `dhconfig` is reachable | `scripts/check_env.sh` |
| 2 | Schemas `OMS_RAW.Orders{A,B,C}`, `OMS_TRACE.DriftEvents` | infra node (or through the infra container) | `scripts/deploy_schemas.sh`, `schemas/*.schema` |
| 3 | Data Import Servers with private storage and claims, routing validated | a node with the DIS storage mounted | `scripts/deploy_dis.sh` |
| 4 | The `oms_trace` package (and the AMPS client) in every worker's Python | every query and merge node | `scripts/build_wheel.sh`, `scripts/install_worker_venv.sh` |
| 5 | The controller pulls `queries/oms_trace` from Git | infra node | `scripts/deploy_git_repo.sh` |
| 6 | ACLs for the groups of docs/design/07 §5 | ACL editor or `dhconfig acl` | by hand the first time; export into `pq/acl/` |
| 7 | The `oms-*` Persistent Queries | Core+ Python with the controller client, or the Query Monitor | `scripts/deploy_pqs.sh`, `pq/*.json` |
| 8 | Verify, then hand over | a Core+ console, the Query Monitor | docs/design/05 §10 checklist, docs/design/08 |

## 0. Settings

```bash
cp deploy/deploy.env.example deploy/deploy.env     # git-ignored
```

Fill in the connection URL, the service account and its key file path, the dispatcher names, the
DIS storage root, the Git URI and branch, the worker venv path, the node list, the topics and the
Kafka bootstrap (or the AMPS URI). Credentials stay out of this file: Kafka SASL properties reach
the ingester PQs as `KAFKA_PROP_*` variables from the host's secret store, the AMPS password is in
the URI the secret store holds, and the service account's private key is a file path.

If `dhconfig` is not on this host, let every call go through the infra container:

```bash
DHCONFIG="podman exec -i deephaven-infra dhconfig"
```

The scripts stream files (schemas, the properties fragment) into the container when they see
`podman exec` in `DHCONFIG`.

## 1. Environment check

```bash
bash scripts/check_env.sh          # DRY_RUN=1 prints the commands only
```

Reports the `dhconfig` version against `DH_EXPECTED_VERSION`, the storage root, the PQ scripts
and definitions (`oms_trace.pqdefs --check`), the controller's Git repository state and, from
this host, the reachability of Kafka or AMPS. A blocking finding exits 1. A different release is a
warning: re-run the identifier confirmation of docs/design/05 §11 before step 7.

## 2. Schemas

```bash
bash scripts/deploy_schemas.sh
```

`dhconfig schema import --file` for every file under `schemas/`, then `dhconfig schema list` for
both namespaces. Attribute names in the XML follow the schema reference as read at design time
(**confirm**); the column set is the contract of docs/design/03 §2 and §5.1. A later schema change
is additive only (docs/design/07 §10).

## 3. Data Import Servers and routing

```bash
bash scripts/deploy_dis.sh
```

One `dhconfig dis add` per raw table (`oms_ingest_a/b/c`, each claiming its `OMS_RAW.Orders<X>`)
and one for the derived results (`oms_trace`, claiming `OMS_TRACE`), private storage under
`DIS_STORAGE_ROOT` on the shared filesystem that merge and query nodes mount, then
`dhconfig dis list` and `dhconfig routing validate`. From a Core+ console, `db.live_table("OMS_RAW",
"OrdersA")` must return an empty table rather than an error before the ingesters start.

## 4. The Python package in the workers

The PQ scripts import `oms_trace`. Two routes, the first is the one for production:

1. **A wheel in the Core+ Python environment of every query and merge node.**
   ```bash
   bash scripts/build_wheel.sh                     # dist/oms_order_tracing-<version>-py3-none-any.whl, pip index from repos.env
   bash scripts/install_worker_venv.sh             # scp + pip install as irisadmin on every node in WORKER_NODES
   bash scripts/install_worker_venv.sh --local     # this node only
   ```
   `COREPLUS_VENV` is the venv the workers run from (**confirm the path on the installed release**;
   docs/design/07 §6: build worker environments from the release's frozen-requirements file). When
   `OMS_AMPS_URI` is set the script also installs `amps-python-client`, which the AMPS source and
   the AMPS ingester need (docs/design/11 §6). Re-run after every release of this repository that
   changes `src/`.
2. **`OMS_TRACE_SRC` on the PQ**, pointing at a checkout's `src` folder on a path every worker
   node sees (shared storage). Fine for a first UAT; the controller's own clone under
   `GIT_REPO_ROOT` lives on the infra node and is **not** visible to the workers, so do not point
   at it.

## 5. The controller's Git repository

Prerequisites in the infra container, as `irisadmin`: an SSH key pair without passphrase under
`/db/TempFiles/irisadmin/.ssh` (mode 600), its public key registered as a **read-only deploy key**
on this repository, and the Git host in `known_hosts`. Then:

```bash
bash scripts/deploy_git_repo.sh
```

Writes `deploy/iris-environment.oms_trace.prop` (the `iris.scripts.repos`,
`PersistentQueryController.scriptUpdateEnabled` and `iris.scripts.repo.oms_trace.*` properties:
URI, branch, `paths=queries/oms_trace`, a persistent `root` under the host-mounted volume,
`updateEnabled`, the groups) and imports it with `dhconfig properties import` (**confirm the
subcommand**; appending the fragment to the controller's `iris-environment.prop` is the
equivalent). Restart the controller, then:

```bash
dhconfig pq check-repo
```

The scripts appear in the Query Monitor's dropdown as `oms_trace/queries/oms_trace/<name>.py`. A
new commit is pulled on the controller's refresh cycle; a running PQ picks it up at its next
restart (daily, or `dhconfig pq restart --name <pq>`). Rolling back is reverting the commit and
restarting (docs/design/07 §10).

## 6. ACLs

Groups and rights per docs/design/07 §5: `oms-trace-svc` owns the PQs and writes `OMS_TRACE`
through the merge dispatcher, `oms-trace-ops` reads both namespaces and administers the PQs, the
desk groups view `oms-trace-ui`. Create them in the ACL editor (or `dhconfig acl`) the first time
and keep the export under `pq/acl/`. The PQ definitions carry the admin and viewer groups
(`DH_ADMIN_GROUPS`, `DH_VIEWER_GROUPS`).

## 7. Persistent queries

```bash
bash scripts/deploy_pqs.sh --dry-run              # the table to type into the Query Monitor
bash scripts/deploy_pqs.sh                        # create or update through the Core+ controller client
bash scripts/deploy_pqs.sh --only oms-lineage
bash scripts/deploy_pqs.sh --export               # dhconfig pq export into pq/export/, versioned
```

`pq/*.json` is the single source of truth (type, dispatcher, script, heap, daily restart, error
restarts, init timeout, dependencies, environment). `${VAR}` values resolve from `deploy/deploy.env`.
The first time, `--dry-run` and the Query Monitor is the safest path; `--apply` runs in a Core+
Python environment (`COREPLUS_PYTHON=/usr/illumon/dnd/venv/bin/python`) and uses the controller
client with the configuration field names of `oms_trace.pqdefs.CONTROLLER_FIELDS` (**confirm**).

Start order (docs/design/07 §7 and §9): the ingesters (`oms-ingest-a/b/c`, or the `oms-ingest-amps-*`
twins when a system publishes to AMPS, docs/design/11), check that today's partitions appear and
`db.live_table` ticks in a console; then `oms-lineage` and check `trace_stats`; then
`oms-drift-writer` (Dependent) and `oms-trace-ui` (Dependent). Daily restarts are staggered 00:05,
00:15, 00:20, 00:25 in `OMS_TRACE_TZ`. Create the Data Merge and Data Validation PQs per raw table
in the Query Monitor (type-defined, docs/design/07 §2) once a day has closed.

## 8. Verification and handover

Run the docs/design/05 §10 checklist: schemas listed, DISes claimed and routed, ingesters writing
today's partition, a SIGKILL'd ingester resuming without a gap, `OnReject.LOG` on a malformed
record, a midnight partition roll without restart, the nightly merge and validation. Then
`trace_stats`, `feed_health`, `alerts` and the dashboard on real feeds; `oms_trace.trace_check`
on a day of raw rows first (docs/design/09 §6). Hand over with docs/design/08 (alerts, runbooks,
forensic queries).

## What is confirmed where

| Item marked confirm on 2026.01.054 | One place to adjust |
|---|---|
| `dhconfig` subcommands and flags (`schema import`, `dis add`, `routing validate`, `properties import`, `pq check-repo/import/export/restart`) | the `run dh ...` lines of the scripts |
| Core+ Kafka ingester names (`kafka_table_writer_options`, `get_dis_by_name`, `OnReject`) | `queries/oms_trace/ingest.py`, `oms_trace.enterprise.ingester_options` |
| Derived Table Writer (`DataImportServerTools.getDisByNameWithStorage`, `DerivedTableWriter.ingestTable`, `Options`) | `queries/oms_trace/ingest_amps.py`, `queries/oms_trace/drift_writer.py`, `oms_trace.enterprise.dtw_options` / `drift_writer_options` |
| Core+ client (`SessionManager`, `private_key`, `connect_to_persistent_query`, `open_table`, `barrage_subscribe`, `controller_client`) | `queries/oms_trace/drift_writer.py`, `queries/oms_trace/ui.py`, `oms_trace.pqdefs.apply` |
| Controller configuration fields and scheduling strings | `oms_trace.pqdefs.CONTROLLER_FIELDS`, `scheduling_lines` |
| Worker venv path | `COREPLUS_VENV` in `deploy/deploy.env` |
| Schema XML attribute names | `schemas/*.schema` |

## Enterprise test environment: the checklist

Everything above was proven on the Community engine and with stubs; these items need the cluster.
Record the outcome in the last column and keep this table in the repository (one copy per
environment is fine). Items 1–2 are cluster access and spellings; 3–6 are the test plan and need
the feeds.

### 1. Identifiers to confirm on 2026.01.054 (blocks everything else)

| # | Item | Where to adjust | Outcome |
|---|---|---|---|
| 1.1 | `dhconfig` subcommands and flags: `version`, `schema import --file`, `schema list --namespace`, `dis add --name/--storage/--claim`, `dis list`, `routing validate`, `properties import --file`, `pq check-repo`, `pq export/import/restart` | the `run dh ...` lines in `scripts/*.sh` | |
| 1.2 | Core+ Kafka ingester: `kafka_table_writer_options`, `get_dis_by_name`, `dis.consume`, `OnReject.LOG`, the partition / internal-partition options | `queries/oms_trace/ingest.py`, `oms_trace.enterprise.ingester_options` | |
| 1.3 | Derived Table Writer: `DataImportServerTools.getDisByNameWithStorage`, `DerivedTableWriter.ingestTable(dis, namespace, table, columnPartition, internalPartition, table, options)`, the `Options` builder (`onReject`, `sequenceColumn`) | `queries/oms_trace/ingest_amps.py`, `queries/oms_trace/drift_writer.py`, `enterprise.dtw_options` / `drift_writer_options` | |
| 1.4 | Core+ client: `SessionManager`, `private_key`, `connect_to_persistent_query`, `open_table`, `barrage_subscribe`, `controller_client` | `queries/oms_trace/drift_writer.py`, `queries/oms_trace/ui.py`, `oms_trace.pqdefs.apply` | |
| 1.5 | Controller configuration fields and scheduling strings; the Query Monitor's type labels | `oms_trace.pqdefs.CONTROLLER_FIELDS`, `scheduling_lines`, `PQ_TYPES` | |
| 1.6 | The Core+ worker venv path (`COREPLUS_VENV`) and the Python version the workers run | `deploy/deploy.env` | |
| 1.7 | Schema XML attribute names (`storageType`, `defaultMergeFormat`, `columnType`, `java.time.Instant`, `Boolean`) | `schemas/*.schema` | |
| 1.8 | `amps-python-client` installs into the worker venv (wheel available on the pip index for the worker's Python) | `scripts/install_worker_venv.sh` | |

### 2. One pass of the kit on UAT

| # | Item | Evidence | Outcome |
|---|---|---|---|
| 2.1 | irisadmin's SSH deploy key accepted by the Git host from the infra container | `ssh -T` prints the greeting | |
| 2.2 | `scripts/check_env.sh` passes | exit 0 | |
| 2.3 | `scripts/deploy_schemas.sh`: both namespaces listed | `dhconfig schema list` | |
| 2.4 | `scripts/deploy_dis.sh`: four DISes, routing valid, `db.live_table("OMS_RAW","OrdersA")` returns an empty table | console | |
| 2.5 | `scripts/build_wheel.sh` + `install_worker_venv.sh`: `import oms_trace` works in a Core+ console on every node | console | |
| 2.6 | `scripts/deploy_git_repo.sh`, controller restarted, `dhconfig pq check-repo` passes; scripts visible in the Query Monitor dropdown | Query Monitor | |
| 2.7 | ACL groups created, export saved under `pq/acl/` | `pq/acl/` | |
| 2.8 | `scripts/deploy_pqs.sh --dry-run` matches what was entered; `--apply` creates or updates every PQ; `--export` saved under `pq/export/` | `pq/export/` | |
| 2.9 | Start order holds: ingesters, lineage, writer, UI; each PQ reaches Running within its init timeout | Query Monitor | |

### 3. Ingestion and persistence (docs/design/05 §10)

| # | Item | Evidence | Outcome |
|---|---|---|---|
| 3.1 | Each ingester writes today's partition; `db.live_table` ticks in a console | row counts rising | |
| 3.2 | A SIGKILL'd ingester resumes from its checkpoint: row counts and offset ranges continue with no gap and no duplicate (AC6) | before / after counts | |
| 3.3 | One malformed record on a UAT topic is logged and skipped (`OnReject.LOG`), the PQ keeps running | ingester log | |
| 3.4 | Midnight: a new `Date` partition appears without a restart; the lineage window follows (`OMS_TRACE_DATE_WINDOW_DAYS`) | partitions, `trace_stats` | |
| 3.5 | Data Merge for yesterday runs; `db.historical_table` returns the merged rows; validation passes; the intraday partition older than the window is deleted | merge / validation PQs | |
| 3.6 | AMPS path (when used): the first start of the day with `OMS_AMPS_PERSIST_SNAPSHOT=true` fills the partition; a restart within the day persists change rows only; a reconnect re-snapshot is `CATCHING_UP` then `CURRENT` | `amps_topics`, partition counts | |

### 4. Derived Table Writer semantics

| # | Item | Outcome |
|---|---|---|
| 4.1 | Whether the writer wants the partitioning column present in the table it is handed, or takes the column partition from the argument only | |
| 4.2 | Whether a Barrage copy of the lineage PQ's `break_events` still counts as a blink or add-only table for the writer | |
| 4.3 | AMPS bookmark mode (D17): a reconnect resumes from the store with no re-snapshot; a crash between publish and discard replays at most one flush, which `last_by(OrderId)` absorbs; the bookmark store on shared storage survives a PQ moving nodes | |
| 4.3b | Whether `sow_delete` and expiry events replay to a bookmark subscriber (60East); the journal's retention against the `Date` window | |
| 4.4 | `OMS_TRACE.DriftEvents` carries every alert row (`Level = ALERT`) and every transition the lineage emitted | |

### 5. The real feeds (before cutover, docs/design/09 §6)

| # | Item | Evidence | Outcome |
|---|---|---|---|
| 5.1 | `oms_trace.trace_check` on one day of raw rows from each system: no contract violations (null parent id = parent, children carry no `UpstreamRef`) | report | |
| 5.2 | The link tags per system (docs/design/11 D18): 5011 on A's destination orders, B's and C's `UpstreamRef` and `ParentOrderID` tags, and whether any system keys on 11 rather than 37 | `OMS_AMPS_TAG_MAP_{A,B,C}` in `deploy.env`; the ingester PQ refuses to start without them | |
| 5.3 | Resolution rate per hop ≥ 99.9 % once lag settles; `DestMinusCli` and `CParentsMinusBChildren` sit at zero | `trace_check`, `trace_stats` | |
| 5.4 | Each topic's longest silence on a normal day, to set `OMS_TRACE_FEED_STALE_SEC` (a quiet channel needs a producer heartbeat or a longer threshold) | `trace_check` topics | |
| 5.5 | Tag 60 comparable across systems (GMT) for `OMS_TRACE_AGE_CLOCK=event` and the `CATCHING_UP` rule | spot checks | |
| 5.6 | The alert sink endpoint receives a test alert; the auth header from the secret store works | `alerts.Posted = 200` | |

### 6. Sizing and recovery (docs/design/06)

| # | Item | Measured | Outcome |
|---|---|---|---|
| 6.1 | Lineage PQ heap after a full day at production volume (expected case 96 GB, doc 06 §1.3) | | |
| 6.2 | Recovery time after a lineage restart over the window; init timeout set to twice it | | |
| 6.3 | Update-graph cycle time at the market-close burst (10k msg/s per topic, up to 8 topics per system); p99 under 1 s | | |
| 6.4 | Barrage subscriptions of the UI and writer PQs stay small (no node tables) | | |
| 6.5 | Decision: the single worker holds, or the edge-family sharding of doc 06 §4 is needed | | |

### 7. Not yet built, exercised by the environment

| # | Item | Outcome |
|---|---|---|
| 7.1 | Data Merge and Data Validation PQs per raw table (type-defined, created in the Query Monitor) | |
| 7.2 | ACL exports under `pq/acl/` | |
| 7.3 | The end-of-day summary (`OMS_TRACE.DailyDriftSummary`) and the execution-level identification (P2b) with the `Execs` feeds and schemas | |
