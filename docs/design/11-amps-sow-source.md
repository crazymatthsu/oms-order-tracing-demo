# 11 — Latest order states from AMPS SOW topics

Decision record for switching the latest-order-state feeds from Kafka topics to **AMPS SOW
topics**, and for how the states then reach Deephaven Enterprise. It extends docs 02, 03, 05
and 08; where it changes an earlier statement it says so.

---

## 1. The question

Each OMS publishes the latest state of every order it owns. Docs 02–05 assumed Kafka topics
read by the Core+ Kafka ingester into a Data Import Server. The firm can instead publish the
same states into AMPS **State-of-the-World (SOW)** topics, one or more per system, keyed on the
order id. Two questions follow:

1. Does the design survive the switch?
2. Does an AMPS source mean the states are injected straight into the worker's tables, skipping
   the DIS?

## 2. Why the switch is a good fit

A SOW topic **is** the latest state per key. `sow_and_subscribe` delivers a snapshot of every
live record (`group_begin` … `sow` … `group_end`), then every change (`publish`), and tells you
when a record leaves the SOW (`oof`, `sow_delete`). Consequences for the design:

- Assumption A2 (full state per message) holds by construction: a SOW record is the order's
  complete state.
- Recovery is a re-snapshot, not a replay of a day's messages: a worker restart sees the exact
  current book within seconds. The `Date` window (N4) still bounds what the lineage holds, but
  the SOW's own expiry policy is what keeps or drops old orders.
- The HA client reconnects and re-issues the subscription; the new snapshot (a new *epoch*)
  re-sends every live record and, at its `group_end`, whatever was **not** re-sent was deleted
  while we were disconnected (no `oof` could reach us). The source drops those records' live
  status; without this they would linger as phantom open orders.
- The fix42 repository's doc 14 §12 module solved exactly this for Raptor / algo / router FIX
  order states and proved it against a real AMPS 5.3.5 broker, including the reconnect re-sync
  and the heartbeat lesson. This design follows its event model.

## 3. Transport is not persistence: the two paths

Enterprise ships Core+ ingesters for Kafka and Solace, not for AMPS. The AMPS client therefore
runs **inside a Deephaven worker** and publishes into an in-memory source table either way; the
DIS question is only about persistence and sharing.

| | **In-worker only** (skip the DIS) | **In-worker client + Derived Table Writer** (the production path) |
|---|---|---|
| Where the client runs | in the lineage PQ; the SOW mirror feeds the DAG directly | in one ingester PQ per system on the **merge dispatcher**; the state stream is persisted to `OMS_RAW.Orders<X>` through the Derived Table Writer, and the lineage PQ reads `db.live_table` exactly as designed |
| Recovery | re-snapshot from the SOW | DIS storage plus the re-snapshot |
| History, forensics | in AMPS (its transaction log), not in Deephaven | `OMS_RAW` intraday and historical partitions, nightly merge, as designed |
| Other readers (UI PQ, shards, reports) | each needs its own subscription, or Barrage from the lineage PQ | read the tables from storage |
| ACLs on raw data | none to apply | as designed (doc 07 §5) |
| DAG | unchanged | unchanged |
| Cost | simplest; for dev, the demo and a first UAT | one more PQ per system; the writer's replay semantics must be settled (§5) |

**Decision (D16).** Build the AMPS source once (`oms_trace.amps_sow`) and run it in both
shapes: in-worker only for the Community demo and early UAT (`OMS_TRACE_SOURCE=amps` in the
lineage PQ), and behind the Derived Table Writer for production (`queries/oms_trace/ingest_amps.py`),
so that replay, history, multi-reader access and ACLs are kept without touching the DAG.

## 4. What changes in the design

### 4.1 Deletes (new)

Kafka topics are append-only; a SOW record can leave. A vanished child makes its parent look
short (`MISSING_DOWNSTREAM`), a vanished parent makes its children `UNRESOLVED`. **Policy:** the
source never drops a record; on `oof` / `sow_delete` (or when a record is missing from a
reconnect snapshot) it re-emits the last known state with `SowDeleted = true`. The DAG keeps
using the quantities (a filled-and-expired order still explains its parent's fills); the flag is
available for display, for feed health and for the pre-cutover check. `SowDeleted` is a new
raw-contract column (doc 03 §2), `false` or null on Kafka-fed rows.

### 4.2 Feed health gets a better signal (refines doc 04 §5)

Per topic the source knows whether a snapshot is in progress (`group_begin` seen, `group_end`
not yet) and when the last message arrived. It publishes one row per topic (`amps_topics`:
`System`, `Topic`, `Epoch`, `SnapshotInProgress`, `Connected`, `LastRecvTs`, `Records`, `Live`,
`Deleted`, `Rejected`, `LastError`, `StatusTs`; a blink stream behind a `last_by`). The lineage's
feed health takes that table as an optional override (`build_lineage(..., feed_states=...)`):
silence still makes a topic `STALE`; then a disconnected client makes every one of its topics
`STALE` at once; then a replaying snapshot is `CATCHING_UP` whatever its event-time lag says;
otherwise the event-time rule of doc 04 §5 applies, as it does alone for feeds without a
snapshot marker (Kafka). `feed_health` gains the two override columns (null on Kafka feeds).

### 4.3 Ordering (downgrades A15)

A snapshot arrives in key order, not publish order, so parent-before-child does not hold
during a snapshot or after a reconnect. The DAG never depended on it (a child that arrives first
is `UNRESOLVED` and heals in the cycle its parent lands), so nothing changes in code; A15 becomes
"not relied upon".

A re-snapshot also stamps a fresh `RecvTs` on every re-sent record: with the default
`OMS_TRACE_AGE_CLOCK=recv` every open gap reads `TRANSIENT` for `gap_age_sec` after a reconnect
and then returns to its kind; `event` aging is unaffected. The topics are `CATCHING_UP` for the
duration of the snapshot either way (§4.2).

### 4.4 Topics and keys

- One SOW topic per drop-copy channel, as with Kafka (Q6): `OMS_AMPS_TOPICS_<X>` lists them per
  system (falling back to `OMS_TOPICS_<X>`). Each topic is keyed on the order id
  (`<Key>/OrderId</Key>`), so a record **is** an order's latest state. `send_keys` makes AMPS
  send its `SowKey` header with every message; the source uses it to match an `oof` to the
  record it saw, and the body's `OrderId` is the identity.
- Body formats: `json` (field names per the raw contract, remappable with `OMS_AMPS_FIELD_MAP`)
  or `fix` (tag=value bodies with a tag map, defaults: 37 → `OrderId`, 55 → `Symbol`, 54 →
  `Side`, 38 → `OrderQty`, 14 → `CumQty`, 151 → `LeavesQty`, 6 → `AvgPx`, 39 → `OrdStatus`,
  60 → `EventTs`; the cross-system and in-system parent tags are firm-specific and **must** be
  configured).
- `RecvTs` is stamped on arrival; `Topic` is the SOW topic; `KafkaPartition` / `KafkaOffset`
  are null (the AMPS bookmark is not carried).

### 4.5 Heartbeats (operational rule)

The client runs with a heartbeat (`OMS_AMPS_HEARTBEAT_SEC`, default 5). Without it a half-open
connection stalls the feed silently and nothing reconnects (the fix42 e2e test showed exactly
that); with it the connection drops within two intervals and the reconnect snapshot repairs the
book. Doc 08 gains the failure row.

## 5. The Derived Table Writer path, precisely

- One **Live Query - Merge Server** PQ per system (`oms-ingest-amps-<x>`,
  `queries/oms_trace/ingest_amps.py`) runs `amps_sow.start_source` for that system's topics
  (`AmpsSettings.for_system`) and hands the system's **blink** state table to
  `DerivedTableWriter.ingestTable(dis, namespace, tableName, columnPartition, internalPartition,
  table, options)` on the `oms_ingest_<x>` DIS of doc 05 §2 (`DataImportServerTools.getDisByNameWithStorage`).
  The writer takes a blink or add-only table; the column partition is the day's `Date` in
  `OMS_TRACE_TZ` and the PQ restarts daily like the Kafka ingester (`ingestPartitionedTable`
  over `partition_by("Date")` is the alternative for a multi-day worker). The parameter order
  and the `Options` builder (`sequenceColumn`, ...) are read from the Core+ javadoc at design
  time: **confirm on 2026.01.054**; `oms_trace.enterprise.dtw_options` holds every value.
- **Replay semantics.** A reconnect re-snapshot would append the whole SOW again. The source
  therefore tags every row with `SowStream`: `snapshot` (a `sow` record) or `change` (a
  `publish`, a delete, a reconnect drop). The ingester persists change rows always and snapshot
  rows only when `OMS_AMPS_PERSIST_SNAPSHOT` is `true` (the first start of a trading day, when
  the partition is empty). After a restart within the day, the DIS already holds the states and
  the SOW re-snapshot only rebuilds the in-worker mirror. Without a per-partition monotonic
  sequence column the writer cannot promise exactly-once across a restart; a consumer doing
  `last_by(OrderId)` is indifferent to duplicates either way.
- The lineage PQ is **unchanged**: `OMS_TRACE_SOURCE=enterprise`, `db.live_table` over
  `OMS_RAW.Orders<X>`, the same Date window.

## 6. What stays open

- The AMPS client library (`amps-python-client`) must be in the worker's Python environment:
  an addition to the release's frozen-requirements file or a custom worker image
  (`docker/deephaven-amps.Dockerfile` is the Community analogue the demo overlay builds).
- The body format and the parent/upstream tags of the real topics (Q15/Q16 answered the
  columns; the AMPS tags that carry them are to be read off a sample with `trace_check`).
- SOW expiry policy per topic: how long a filled or canceled order stays in the SOW decides how
  far back the lineage can see without the DIS.
- The DTW options (`transactionsEnabled`, `sequenceColumn`, chunking) on the installed release.

## 7. Verification

- Pure python (`tests/test_amps_sow.py`): the SOW fold (`amps_sow.TopicMirror`) on message
  doubles and on real `AMPS.Message` objects — snapshot, publish, `delta_publish`, `oof`,
  `sow_delete`, a reconnect snapshot dropping what was not re-sent, the `SowDeleted` policy,
  rejects, both body formats, the field and tag maps, the settings, the stub-client wiring.
- Embedded engine (`tests/test_dag_embedded.py`): the source's blink tables feed
  `dag.build_lineage` and the lineage equals the oracle on the scenario book delivered as SOW
  messages; a late B child published to the SOW heals its C parent; a disconnect turns the
  system's topics `STALE` and the re-snapshot `CATCHING_UP`, then `CURRENT`; an `oof` leaves a
  `SowDeleted` state behind.
- Real broker (when available, as the fix42 e2e does): the compose overlay with an AMPS image
  from `repos.env`, the three SOW topics, publish / delete / disconnect / restart steps.
