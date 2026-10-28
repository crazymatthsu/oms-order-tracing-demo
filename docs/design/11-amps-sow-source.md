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
  system (falling back to `OMS_TOPICS_<X>`). Each topic is keyed on the order id, tag 37
  (`<Key>/37</Key>` for the `fix` message type), so a record **is** an order's latest state.
  `send_keys` makes AMPS send its `SowKey` header with every message; the source uses it to match
  an `oof` to the record it saw, and the body's `OrderID` is the identity.
- **Bodies are FIX 4.2 execution reports** (`OMS_AMPS_FORMAT=fix`, the default; D18 in §5.2). The
  standard tags feed the raw columns of the same name in every system without configuration
  (`config.FIX42_TAGS`: 37 `OrderID`, 11 `ClOrdID`, 41 `OrigClOrdID`, 55 `Symbol`, 54 `Side`,
  38 `OrderQty`, 14 `CumQty`, 151 `LeavesQty`, 6 `AvgPx`, 39 `OrdStatus`, 150 `ExecType`, 35
  `MsgType`, 60 `TransactTime`, 34 `MsgSeqNum`, 75 `Date`). The two link columns are the firm's
  own tags and differ per system: `OMS_AMPS_TAG_MAP_<X>` (`tag=Column,...`) names them — A's
  destination order names its client order in 5011 by default (M4); B's `UpstreamRef` (the A
  destination id) and `ParentOrderID`, and C's, **must** be configured, and the settings refuse
  to load for a system that has topics but no tag for one of its link columns. `OMS_AMPS_TAG_MAP`
  applies to every system first (e.g. to re-point a standard tag); a system's own entry wins
  tag by tag, and a tag feeds one column. Header and trailer tags are ignored, the checksum is
  not verified, codes (`Side`, `OrdStatus`, `ExecType`) pass through untranslated (Q13).
- `json` bodies remain available (`OMS_AMPS_FORMAT=json`, field names per the raw contract,
  remappable with `OMS_AMPS_FIELD_MAP`) for a producer that is not FIX.
- `RecvTs` is stamped on arrival; `Topic` is the SOW topic; `KafkaPartition` / `KafkaOffset`
  are null (the AMPS bookmark is carried in `Bookmark` on the in-worker tables, not persisted).

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

### 5.1 Decision D17: the persistence path subscribes to the transaction log, not the SOW

A reconnect of `sow_and_subscribe` re-sends the whole SOW. For the in-worker mirror that is the
recovery mechanism; for the Derived Table Writer it is a duplicated book per reconnect and the
end of any exactly-once story, which the `snapshot` / `change` split above only patched over.
The SOW topics have a **transaction log**, so the ingester PQ uses a bookmark subscription
instead (`OMS_AMPS_MODE=bookmark`, the default in `queries/oms_trace/ingest_amps.py`):

- `subscribe` with the `bookmark` option and a stable sub id per topic (`<client>:<topic>`),
  replaying from the bookmark the client's file-backed store holds
  (`OMS_AMPS_BOOKMARK_DIR/<client>.mmap`, on shared storage so a PQ restarted on another node
  finds it). A topic with no stored bookmark starts at `EPOCH` or `NOW`
  (`OMS_AMPS_BOOKMARK_FIRST`); the first start of a trading day replays from the journal within
  its retention.
- Every message is a `publish`: a `change` row carrying its `Bookmark`. The row's bookmark is
  discarded once the row has been published to the blink table, so the writer sees each message
  once. The window between publish and discard is the at-least-once window: a crash inside it
  replays those rows, which `last_by(OrderId)` absorbs. The Core+ Kafka ingester does better
  (offset and table size committed together); nothing in the AMPS client offers that join.
- A reconnect resumes from the store: no snapshot, no `CATCHING_UP` beyond the real backlog,
  `SnapshotInProgress` never set. Deletes: `oof` is a live-subscription signal, and whether
  `sow_delete` and expiry replay from the journal is to be confirmed with 60East (§6); a missed
  `SowDeleted` row costs a flag, never a quantity.
- `OMS_AMPS_PERSIST_SNAPSHOT` stays for `sow` mode only.

The lineage PQ's own in-worker source keeps `sow_and_subscribe` (`OMS_AMPS_MODE=sow`): there the
re-snapshot is wanted. One topic, two subscription styles, two jobs.

### 5.2 Decision D18: FIX 4.2 field names in the raw tables, link tags configured per system

The drop copies on AMPS are FIX 4.2 execution reports, not JSON. Three choices follow:

1. **The raw tables use the standard FIX field names** for what a message carries (`OrderID`,
   `ClOrdID`, `TransactTime`, `MsgSeqNum`, ...; doc 03 §2 lists tag and name side by side), so a
   desk reading `OMS_RAW.OrdersB` sees the message it knows, and the DIS schema, the Kafka value
   spec and the parser's standard tag map are one definition (`config.RAW_COLUMNS`,
   `config.FIX42_TAGS`). The lineage's canonical columns (doc 03 §3) keep their own names
   (`OrderId`, `LinkId`, `Status`): they are the model, not the wire.
2. **The two link columns stay ours** (`UpstreamRef`, `ParentOrderID`): FIX 4.2 has no standard
   field for a parent or an upstream order, every firm uses its own tags, and the tags differ
   between the three systems. They are configuration per system (`OMS_AMPS_TAG_MAP_{A,B,C}`),
   validated at start-up, printed in the source's log line and carried by the mock publisher
   (`oms_trace.amps_publish`) so the demo's two ends agree without code changes.
3. **Codes are not translated.** `Side`, `OrdStatus` and `ExecType` hold the FIX codes; the
   lineage compares them for equality only, and a display mapping belongs in the UI if the desks
   want one (open, §6).

`json` stays as a second format for a non-FIX producer; the broker configuration, the URI's
message type and `OMS_AMPS_FORMAT` must agree.

## 6. What stays open

- The AMPS client library (`amps-python-client`) must be in the worker's Python environment:
  an addition to the release's frozen-requirements file or a custom worker image
  (`docker/deephaven-amps.Dockerfile` is the Community analogue the demo overlay builds).
- The parent/upstream tags of the real topics per system (Q15/Q16 answered the columns; the tags
  that carry them are to be read off a sample and put in `OMS_AMPS_TAG_MAP_{A,B,C}`), and
  whether any system keys its orders on `ClOrdID` (11) rather than `OrderID` (37) — a one-entry
  re-point of the standard map for that system.
- A display mapping for the FIX codes in `Side` / `OrdStatus` on the dashboard (D18).
- SOW expiry policy per topic: how long a filled or canceled order stays in the SOW decides how
  far back the lineage can see without the DIS.
- The DTW options (`transactionsEnabled`, `sequenceColumn`, chunking) on the installed release.
- Whether `sow_delete` and SOW expiry events replay to a bookmark subscriber from the transaction
  log (60East), and the journal's retention against the two-day `Date` window.

## 7. Verification

- Pure python (`tests/test_amps_sow.py`): the SOW fold (`amps_sow.TopicMirror`) on message
  doubles and on real `AMPS.Message` objects — snapshot, publish, `delta_publish`, `oof`,
  `sow_delete`, a reconnect snapshot dropping what was not re-sent, the `SowDeleted` policy,
  rejects, FIX 4.2 and JSON bodies (whole execution reports with header and trailer, the
  standard and per-system tag maps, the link-tag validation, the publisher's encoding round
  trip and checksum), the settings, the stub-client wiring.
- Embedded engine (`tests/test_dag_embedded.py`): the source's blink tables feed
  `dag.build_lineage` and the lineage equals the oracle on the scenario book delivered as SOW
  messages; a late B child published to the SOW heals its C parent; a disconnect turns the
  system's topics `STALE` and the re-snapshot `CATCHING_UP`, then `CURRENT`; an `oof` leaves a
  `SowDeleted` state behind.
- Real broker (when available, as the fix42 e2e does): the compose overlay with an AMPS image
  from `repos.env`, the three SOW topics, publish / delete / disconnect / restart steps.
