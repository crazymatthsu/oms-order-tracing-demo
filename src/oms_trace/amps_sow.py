"""AMPS SOW topics -> the raw order-state tables, inside the worker (docs/design/11).

One ``HAClient`` over every URI in ``OMS_AMPS_URI`` runs one ``sow_and_subscribe`` (options
``oof,send_keys``) per SOW topic of every system. Each topic's messages fold into a
:class:`TopicMirror`; the rows it emits land, per system, in a **blink** table with the raw
contract of docs/design/03 section 2 plus ``SowStream`` (``snapshot`` | ``change``) and
``SowEpoch``, so that

* the lineage DAG reads ``last_by(OrderId)`` over the blink table exactly as it reads a DIS
  table (``OMS_TRACE_SOURCE=amps``, :func:`oms_trace.app.build`; aggregations over a blink
  table see its whole history), and
* the Derived Table Writer PQ (``queries/oms_trace/ingest_amps.py``) persists the ``change``
  rows always and the ``snapshot`` rows only when ``OMS_AMPS_PERSIST_SNAPSHOT`` is true.

==========================  ====================================================================
AMPS message                what the mirror does
==========================  ====================================================================
``group_begin``             a new epoch: the snapshot of every live record starts
``sow``                     the record's state -> a ``snapshot`` row; the record is marked seen
``publish``                 the record's new full state -> a ``change`` row
``delta_publish``           the changed fields merged onto the last known state -> ``change``
``oof``, ``sow_delete``     the record left the SOW -> its last state again, ``SowDeleted = true``
``group_end``               the snapshot is complete: every record not re-sent in this epoch was
                            deleted while no ``oof`` could reach us -> ``SowDeleted`` rows
``ack``, ``heartbeat``, ... ignored
==========================  ====================================================================

A record is never dropped from the mirror (docs/design/11 section 4.1): a filled-and-expired
child still explains its parent's fills. ``RecvTs`` is stamped on arrival; ``Date`` is the
body's ``Date`` when it carries one, else the arrival date in ``OMS_TRACE_TZ``;
``KafkaPartition`` / ``KafkaOffset`` are null. A body that does not parse is rejected and
counted, like the Kafka ingester's ``OnReject.LOG``.

Per topic the source also publishes status rows (``amps_topics``: ``Epoch``,
``SnapshotInProgress``, ``Connected``, ...) that the DAG's feed health takes as an override
(docs/design/11 section 4.2): a replaying snapshot is ``CATCHING_UP``, a disconnected client
makes every one of its topics ``STALE``.

Threads::

    AMPS receive thread                 EventBuffer (per system)      update-graph thread
    -------------------                 ------------------------      -------------------
    handler -> TopicMirror ---offer---> [(row, stream, epoch)] ---drain---> on_flush -> publisher.add

Nothing at module scope imports ``deephaven`` or ``AMPS`` (``pip install amps-python-client``,
the ``amps`` extra): the settings, the body parsers and the fold are unit-tested on a bare
python with a stub client (``tests/test_amps_sow.py``).
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import traceback
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from oms_trace import config
from oms_trace.config import INGESTER_COLUMNS, RAW_COLUMN_KINDS, RAW_COLUMN_NAMES, SYSTEMS, Settings, SettingsError

Row = Dict[str, Any]

#: ``SowStream`` values.
SNAPSHOT = "snapshot"
CHANGE = "change"

URI_ENV = "OMS_AMPS_URI"
TOPICS_ENV = "OMS_AMPS_TOPICS_{system}"
CLIENT_NAME_ENV = "OMS_AMPS_CLIENT_NAME"
HEARTBEAT_ENV = "OMS_AMPS_HEARTBEAT_SEC"
FORMAT_ENV = "OMS_AMPS_FORMAT"
FIELD_MAP_ENV = "OMS_AMPS_FIELD_MAP"
TAG_MAP_ENV = "OMS_AMPS_TAG_MAP"
BATCH_SIZE_ENV = "OMS_AMPS_BATCH_SIZE"
TIMEOUT_ENV = "OMS_AMPS_TIMEOUT_MS"
MAX_PENDING_ENV = "OMS_AMPS_MAX_PENDING"
PERSIST_SNAPSHOT_ENV = "OMS_AMPS_PERSIST_SNAPSHOT"

DEFAULT_CLIENT_NAME = "dh-oms-trace"
#: Seconds between AMPS heartbeats (docs/design/11 section 4.5); 0 turns them off.
DEFAULT_HEARTBEAT_SEC = 5
DEFAULT_BATCH_SIZE = 1000
DEFAULT_TIMEOUT_MS = 10_000
DEFAULT_MAX_PENDING = 250_000
#: ``oof``: deletes and expiries arrive as ``oof``; ``send_keys``: every message carries its SowKey.
SUBSCRIBE_OPTIONS = "oof,send_keys"
#: FIX tag -> raw column, for ``OMS_AMPS_FORMAT=fix`` (docs/design/11 section 4.4). The parent
#: and upstream tags are firm-specific and come from ``OMS_AMPS_TAG_MAP``.
DEFAULT_TAG_MAP: Mapping[str, str] = {
    "37": "OrderId", "55": "Symbol", "54": "Side", "38": "OrderQty", "14": "CumQty", "151": "LeavesQty",
    "6": "AvgPx", "39": "OrdStatus", "60": "EventTs", "34": "SeqNo",
}
#: A status row per topic at most this often outside snapshot boundaries.
STATUS_INTERVAL_NS = config.SECOND_NANOS

_UPSERTS = ("sow", "publish", "delta_publish")
_DELETES = ("oof", "sow_delete")
_CREDENTIALS = re.compile(r"(://[^:/@]+):[^@/]+@")
_FIX_TIMESTAMPS = ("%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------------------------
# settings
# ---------------------------------------------------------------------------------------------


def _int_env(env: Mapping[str, str], key: str, default: int, minimum: int) -> int:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{key}={raw!r} is not an integer") from exc
    if value < minimum:
        raise SettingsError(f"{key}={value} must be >= {minimum}")
    return value


def _bool_env(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key, "").strip().lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    raise SettingsError(f"{key}={raw!r} must be true or false")


def _map_env(env: Mapping[str, str], key: str) -> Dict[str, str]:
    """``a=OrderId,b=Symbol`` -> dict; every target must be a raw column the body may carry."""
    out: Dict[str, str] = {}
    for part in re.split(r"[\s,]+", env.get(key, "").strip()):
        if not part:
            continue
        source, sep, column = part.partition("=")
        if not sep or not source or not column:
            raise SettingsError(f"{key}: {part!r} is not source=Column")
        if column not in RAW_COLUMN_KINDS:
            raise SettingsError(f"{key}: {column!r} is not a raw contract column ({', '.join(RAW_COLUMN_NAMES)})")
        if column in INGESTER_COLUMNS:
            raise SettingsError(f"{key}: {column!r} is stamped by the source, not read from the body")
        out[source] = column
    return out


@dataclass(frozen=True)
class AmpsSettings:
    """Every ``OMS_AMPS_*`` knob (docs/design/11 section 4.4 / 4.5 / 5)."""

    uris: Tuple[str, ...]
    topics: Mapping[str, Tuple[str, ...]]
    client_name: str = DEFAULT_CLIENT_NAME
    heartbeat_sec: int = DEFAULT_HEARTBEAT_SEC
    body_format: str = "json"  # json | fix
    field_map: Mapping[str, str] = None  # type: ignore[assignment]  # json field -> raw column
    tag_map: Mapping[str, str] = None  # type: ignore[assignment]  # FIX tag -> raw column
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_pending: int = DEFAULT_MAX_PENDING
    persist_snapshot: bool = False
    tz: str = "America/New_York"

    def __post_init__(self) -> None:
        if self.field_map is None:
            object.__setattr__(self, "field_map", {})
        if self.tag_map is None:
            object.__setattr__(self, "tag_map", dict(DEFAULT_TAG_MAP))
        if self.body_format not in ("json", "fix"):
            raise SettingsError(f"{FORMAT_ENV}={self.body_format!r} must be json or fix")
        if not any(self.topics.get(s) for s in SYSTEMS):
            raise SettingsError("no AMPS topics: set " + ", ".join(TOPICS_ENV.format(system=s) for s in SYSTEMS))

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None, settings: Optional[Settings] = None) -> "AmpsSettings":
        """Read ``OMS_AMPS_*``; the topics fall back to ``OMS_TOPICS_<X>`` and the zone to ``OMS_TRACE_TZ``."""
        env = os.environ if env is None else env
        settings = settings or config.load_settings(env)
        uris = tuple(u for u in re.split(r"[\s,]+", env.get(URI_ENV, "").strip()) if u)
        if not uris:
            raise SettingsError(f"{URI_ENV}: at least one AMPS URI is required, e.g. tcp://amps-host:9007/amps/json")
        topics: Dict[str, Tuple[str, ...]] = {}
        for system in SYSTEMS:
            raw = env.get(TOPICS_ENV.format(system=system), "")
            listed = tuple(t.strip() for t in raw.split(",") if t.strip())
            topics[system] = listed or tuple(settings.topics.get(system, ()))
        tag_map = dict(DEFAULT_TAG_MAP)
        tag_map.update(_map_env(env, TAG_MAP_ENV))
        return cls(
            uris=uris,
            topics=topics,
            client_name=env.get(CLIENT_NAME_ENV, "").strip() or DEFAULT_CLIENT_NAME,
            heartbeat_sec=_int_env(env, HEARTBEAT_ENV, DEFAULT_HEARTBEAT_SEC, 0),
            body_format=env.get(FORMAT_ENV, "").strip() or "json",
            field_map=_map_env(env, FIELD_MAP_ENV),
            tag_map=tag_map,
            batch_size=_int_env(env, BATCH_SIZE_ENV, DEFAULT_BATCH_SIZE, 1),
            timeout_ms=_int_env(env, TIMEOUT_ENV, DEFAULT_TIMEOUT_MS, 0),
            max_pending=_int_env(env, MAX_PENDING_ENV, DEFAULT_MAX_PENDING, 1),
            persist_snapshot=_bool_env(env, PERSIST_SNAPSHOT_ENV, False),
            tz=settings.tz,
        )

    def for_system(self, system: str) -> "AmpsSettings":
        """The same settings with only ``system``'s topics (one ingester PQ per system)."""
        return replace(self, topics={s: (tuple(self.topics.get(s, ())) if s == system else ()) for s in SYSTEMS},
                       client_name=f"{self.client_name}-{system.lower()}")

    def subscriptions(self) -> List[Tuple[str, str]]:
        """``(system, topic)`` for every configured topic, in system order."""
        return [(s, t) for s in SYSTEMS for t in self.topics.get(s, ())]

    def describe(self) -> str:
        """For the log: the URIs with any password masked."""
        uris = ", ".join(_CREDENTIALS.sub(r"\1:****@", u) for u in self.uris)
        heartbeat = f"heartbeat {self.heartbeat_sec} s" if self.heartbeat_sec else "no heartbeat"
        return f"{uris} as {self.client_name} ({self.body_format} bodies, batch {self.batch_size}, {heartbeat})"


# ---------------------------------------------------------------------------------------------
# bodies -> raw rows
# ---------------------------------------------------------------------------------------------


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def to_nanos(value: Any) -> Optional[int]:
    """A timestamp as epoch nanoseconds: an integer, an ISO-8601 string or FIX tag 60 (GMT)."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{value!r} is not a timestamp")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return int(text)
    for fmt in _FIX_TIMESTAMPS:
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            parsed = None
    if parsed is None:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * config.SECOND_NANOS + delta.microseconds * 1_000


def coerce(column: str, value: Any) -> Any:
    """One body value into the raw column's type; raises ``ValueError`` on garbage."""
    kind = RAW_COLUMN_KINDS[column]
    if value is None:
        return None
    if kind == "String":
        return _text(value)  # an empty string stays empty: the contract violation trace_check flags
    if kind in ("long", "int"):
        if isinstance(value, bool):
            raise ValueError(f"{column}={value!r} is not a number")
        if isinstance(value, int):
            return value
        try:
            number = float(_text(value).strip())
        except ValueError:
            raise ValueError(f"{column}={value!r} is not a number") from None
        if not number.is_integer():
            raise ValueError(f"{column}={value!r} is not a whole number")
        return int(number)
    if kind == "double":
        try:
            return float(value) if not isinstance(value, str) else float(value.strip())
        except ValueError:
            raise ValueError(f"{column}={value!r} is not a number") from None
    if kind == "Instant":
        return to_nanos(value)
    if kind == "boolean":
        if isinstance(value, bool):
            return value
        text = _text(value).strip().lower()
        if text in ("true", "1", "yes", "y"):
            return True
        if text in ("false", "0", "no", "n"):
            return False
        raise ValueError(f"{column}={value!r} is not a boolean")
    raise ValueError(f"unknown raw column kind {kind} for {column}")  # pragma: no cover


def parse_json(body: str, field_map: Mapping[str, str]) -> Dict[str, Any]:
    """A JSON object body -> ``{raw column: value}``; fields the map or the contract do not name are ignored."""
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise ValueError("the JSON body is not an object")
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        column = field_map.get(key, key)
        if column in RAW_COLUMN_KINDS and column not in INGESTER_COLUMNS:
            out[column] = value
    return out


def parse_fix(body: str, tag_map: Mapping[str, str]) -> Dict[str, Any]:
    """A ``tag=value`` body (SOH or ``|`` delimited) -> ``{raw column: value}`` through the tag map."""
    out: Dict[str, Any] = {}
    for part in re.split("[\x01|]", body):
        if not part.strip():
            continue
        tag, sep, value = part.partition("=")
        if not sep:
            raise ValueError(f"{part!r} is not tag=value")
        column = tag_map.get(tag.strip())
        if column is not None and column not in INGESTER_COLUMNS:
            out[column] = value
    return out


def make_parser(amps: AmpsSettings) -> Callable[[str], Dict[str, Any]]:
    if amps.body_format == "fix":
        return lambda body: parse_fix(body, amps.tag_map)
    return lambda body: parse_json(body, amps.field_map)


def raw_row(system: str, topic: str, fields: Mapping[str, Any], recv_ns: int, tz: ZoneInfo,
            deleted: bool = False, base: Optional[Row] = None) -> Row:
    """A raw contract row from parsed body fields, stamped on arrival (docs/design/11 section 4.4)."""
    row: Row = {name: None for name in RAW_COLUMN_NAMES}
    if base is not None:
        row.update({name: base.get(name) for name in RAW_COLUMN_NAMES})
    for column, value in fields.items():
        row[column] = coerce(column, value)
    if not row.get("OrderId"):
        raise ValueError("the body carries no OrderId")
    if not row.get("Date"):
        row["Date"] = datetime.fromtimestamp(recv_ns // config.SECOND_NANOS, tz).date().isoformat()
    row["RecvTs"] = recv_ns
    row["Topic"] = topic
    row["KafkaPartition"] = None
    row["KafkaOffset"] = None
    row["SowDeleted"] = deleted
    return row


# ---------------------------------------------------------------------------------------------
# the fold
# ---------------------------------------------------------------------------------------------


class TopicMirror:
    """The SOW fold for one ``(system, topic)``: AMPS messages in, raw rows with a stream out.

    Pure python; ``on_message`` takes anything with ``get_command``, ``get_sow_key`` and
    ``get_data`` (an ``AMPS.Message`` or a test double) and the arrival time.
    """

    def __init__(self, system: str, topic: str, parser: Callable[[str], Dict[str, Any]], tz: ZoneInfo) -> None:
        self.system = system
        self.topic = topic
        self._parse = parser
        self._tz = tz
        #: OrderId -> last known raw row (deleted ones stay, flagged).
        self.records: Dict[str, Row] = {}
        self._keys: Dict[str, str] = {}  # AMPS SowKey -> OrderId
        self._seen: set = set()  # OrderIds (re-)sent in the current snapshot
        self.epoch = 0
        self.in_snapshot = False
        self.last_recv_ns: Optional[int] = None
        self.counts: Dict[str, int] = {"sow": 0, "publish": 0, "delete": 0, "dropped": 0, "rejected": 0, "snapshots": 0}
        self.last_error: Optional[str] = None

    # -- events --------------------------------------------------------------------

    def on_message(self, message: Any, now_ns: int) -> List[Tuple[Row, str]]:
        command = _text(message.get_command())
        if command == "group_begin":
            self.epoch += 1
            self.in_snapshot = True
            self._seen = set()
            self.last_recv_ns = now_ns
            return []
        if command == "group_end":
            self.last_recv_ns = now_ns
            return self._end_snapshot(now_ns)
        if command in _UPSERTS:
            return self._upsert(message, command, now_ns)
        if command in _DELETES:
            return self._delete(message, now_ns)
        return []

    def _upsert(self, message: Any, command: str, now_ns: int) -> List[Tuple[Row, str]]:
        sow_key = _text(message.get_sow_key())
        try:
            fields = self._parse(_text(message.get_data()))
            base = None
            if command == "delta_publish":
                order_id = coerce("OrderId", fields.get("OrderId")) or self._keys.get(sow_key)
                base = self.records.get(order_id or "")
                if base is not None and base.get("SowDeleted"):
                    base = dict(base, SowDeleted=None)
            row = raw_row(self.system, self.topic, fields, now_ns, self._tz, base=base)
        except Exception as exc:  # noqa: BLE001 - OnReject.LOG: count, remember, carry on
            self.counts["rejected"] += 1
            self.last_error = f"{command}: {type(exc).__name__}: {exc}"
            return []
        order_id = row["OrderId"]
        self.records[order_id] = row
        if sow_key:
            self._keys[sow_key] = order_id
        if self.in_snapshot:
            self._seen.add(order_id)
        self.last_recv_ns = now_ns
        self.counts["sow" if command == "sow" else "publish"] += 1
        return [(row, SNAPSHOT if command == "sow" else CHANGE)]

    def _delete(self, message: Any, now_ns: int) -> List[Tuple[Row, str]]:
        sow_key = _text(message.get_sow_key())
        order_id = self._keys.get(sow_key) if sow_key else None
        body = _text(message.get_data())
        if order_id is None and body:
            try:
                order_id = coerce("OrderId", self._parse(body).get("OrderId"))
            except Exception as exc:  # noqa: BLE001
                self.counts["rejected"] += 1
                self.last_error = f"oof: {type(exc).__name__}: {exc}"
                return []
        if not order_id:
            return []
        known = self.records.get(order_id)
        if known is None:
            try:  # a record we never saw live: its last state is the oof body
                known = raw_row(self.system, self.topic, self._parse(body), now_ns, self._tz)
            except Exception as exc:  # noqa: BLE001
                self.counts["rejected"] += 1
                self.last_error = f"oof: {type(exc).__name__}: {exc}"
                return []
        elif known.get("SowDeleted"):
            return []  # already gone
        row = dict(known, RecvTs=now_ns, SowDeleted=True)
        self.records[order_id] = row
        self._seen.discard(order_id)
        self.last_recv_ns = now_ns
        self.counts["delete"] += 1
        return [(row, CHANGE)]

    def _end_snapshot(self, now_ns: int) -> List[Tuple[Row, str]]:
        out: List[Tuple[Row, str]] = []
        if self.in_snapshot:
            for order_id, row in self.records.items():
                if order_id not in self._seen and not row.get("SowDeleted"):
                    dropped = dict(row, RecvTs=now_ns, SowDeleted=True)
                    self.records[order_id] = dropped
                    out.append((dropped, CHANGE))
            self.counts["dropped"] += len(out)
            self.counts["snapshots"] += 1
        self.in_snapshot = False
        self._seen = set()
        return out

    # -- status --------------------------------------------------------------------

    def live(self) -> int:
        return sum(1 for r in self.records.values() if not r.get("SowDeleted"))

    def status(self, connected: bool, now_ns: int) -> Row:
        """One ``amps_topics`` row (docs/design/11 section 4.2)."""
        return {
            "System": self.system, "Topic": self.topic, "Epoch": self.epoch,
            "SnapshotInProgress": self.in_snapshot, "Connected": connected, "LastRecvTs": self.last_recv_ns,
            "Records": len(self.records), "Live": self.live(),
            "Deleted": self.counts["delete"] + self.counts["dropped"], "Rejected": self.counts["rejected"],
            "LastError": self.last_error, "StatusTs": now_ns,
        }


# ---------------------------------------------------------------------------------------------
# the hand-off and the source
# ---------------------------------------------------------------------------------------------


class EventBuffer:
    """Bounded hand-off from the AMPS receive thread to the update-graph thread.

    :meth:`offer` **blocks** when full rather than dropping: a lost delete would leave a phantom
    open order, and blocking the receive thread is ordinary TCP backpressure.
    """

    def __init__(self, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        self._max = max(1, int(max_pending))
        self._cv = threading.Condition()
        self._items: List[Any] = []
        self._closed = False
        self.offered = 0
        self.dropped = 0

    def offer(self, item: Any, timeout: Optional[float] = None) -> bool:
        with self._cv:
            if not self._closed and len(self._items) >= self._max:
                self._cv.wait_for(lambda: self._closed or len(self._items) < self._max, timeout)
            if self._closed or len(self._items) >= self._max:
                self.dropped += 1
                return False
            self._items.append(item)
            self.offered += 1
            return True

    def drain(self) -> List[Any]:
        with self._cv:
            items, self._items = self._items, []
            if items:
                self._cv.notify_all()
            return items

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    @property
    def pending(self) -> int:
        with self._cv:
            return len(self._items)


def default_client_factory(amps: AmpsSettings) -> Any:
    """An ``AMPS.HAClient`` over every configured URI: it reconnects and re-subscribes itself."""
    import AMPS

    client = AMPS.HAClient(amps.client_name)
    chooser = AMPS.DefaultServerChooser()
    for uri in amps.uris:
        chooser.add(uri)
    client.set_server_chooser(chooser)
    if amps.heartbeat_sec:
        client.set_heartbeat(amps.heartbeat_sec)
    return client


def default_command_factory(topic: str, amps: AmpsSettings) -> Any:
    """The ``sow_and_subscribe`` command for one topic."""
    import AMPS

    return (AMPS.Command("sow_and_subscribe").set_topic(topic).set_options(SUBSCRIBE_OPTIONS)
            .set_batch_size(amps.batch_size).set_timeout(amps.timeout_ms))


STATUS_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("System", "String"), ("Topic", "String"), ("Epoch", "int"), ("SnapshotInProgress", "boolean"),
    ("Connected", "boolean"), ("LastRecvTs", "Instant"), ("Records", "long"), ("Live", "long"),
    ("Deleted", "long"), ("Rejected", "long"), ("LastError", "String"), ("StatusTs", "Instant"),
)


class AmpsSowSource:
    """``sow_and_subscribe`` on every topic of every system -> one blink raw table per system.

    Keep the instance reachable while the tables are in use (:func:`start_source` does): it
    owns the AMPS client. ``clock`` is the arrival-time source (tests inject one).
    """

    def __init__(
        self,
        settings: Settings,
        amps: AmpsSettings,
        client_factory: Callable[[AmpsSettings], Any] = default_client_factory,
        command_factory: Callable[[str, AmpsSettings], Any] = default_command_factory,
        clock: Callable[[], int] = time.time_ns,
        retry_seconds: float = 5.0,
    ) -> None:
        self.settings = settings
        self.amps = amps
        self._client_factory = client_factory
        self._command_factory = command_factory
        self._clock = clock
        self._retry_seconds = retry_seconds
        parser = make_parser(amps)
        tz = ZoneInfo(amps.tz)
        self.mirrors: Dict[Tuple[str, str], TopicMirror] = {
            (s, t): TopicMirror(s, t, parser, tz) for s, t in amps.subscriptions()}
        self.buffers: Dict[str, EventBuffer] = {s: EventBuffer(amps.max_pending) for s in SYSTEMS}
        self.status_buffer = EventBuffer(amps.max_pending)
        self.tables: Dict[str, Any] = {}
        self._publishers: Dict[str, Any] = {}
        self._status_table: Any = None
        self._status_publisher: Any = None
        self._ctx: Any = None
        self._status_ts: Dict[Tuple[str, str], int] = {}
        self._client: Any = None
        self._sub_ids: List[Any] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.connected = False
        self.last_error: Optional[str] = None

    # -- tables (engine) --------------------------------------------------------------

    def build_tables(self) -> Dict[str, Any]:
        """One blink table per system plus the status table (no AMPS connection yet)."""
        from deephaven import dtypes
        from deephaven.execution_context import get_exec_ctx
        from deephaven.stream.table_publisher import table_publisher

        from oms_trace import sources

        self._ctx = get_exec_ctx()
        defs = dict(sources.raw_column_definitions())
        defs["SowStream"] = dtypes.string
        defs["SowEpoch"] = dtypes.int32
        for system in SYSTEMS:
            table, publisher = table_publisher(f"amps_{system.lower()}_states", defs,
                                               on_flush_callback=self._flusher(system), on_shutdown_callback=self.stop)
            self.tables[system] = table
            self._publishers[system] = publisher
        status_defs = {name: sources.dtype(kind) for name, kind in STATUS_COLUMNS}
        self._status_table, self._status_publisher = table_publisher(
            "amps_topics_events", status_defs, on_flush_callback=self._status_flush, on_shutdown_callback=self.stop)
        return self.tables

    def raw_tables(self) -> Dict[str, Any]:
        """``{"A": table, ...}`` for :func:`oms_trace.dag.build_lineage` (blink; raw contract + SowStream, SowEpoch)."""
        if not self.tables:
            self.build_tables()
        return dict(self.tables)

    def persist_table(self, system: str) -> Any:
        """The rows the Derived Table Writer persists: ``change`` rows, plus ``snapshot`` rows when configured."""
        table = self.raw_tables()[system]
        if not self.amps.persist_snapshot:
            table = table.where(f"SowStream == `{CHANGE}`")
        return table.view(list(RAW_COLUMN_NAMES))

    def topics_table(self) -> Any:
        """``amps_topics``: the latest status row per topic (docs/design/11 section 4.2)."""
        if self._status_table is None:
            self.build_tables()
        return self._status_table.last_by(["System", "Topic"])

    def _flusher(self, system: str) -> Callable[[Any], None]:
        def on_flush(publisher: Any) -> None:
            """Update-graph thread: publish everything buffered since the last cycle; never raises."""
            try:
                items = self.buffers[system].drain()
                if items:
                    with self._ctx:
                        publisher.add(self._batch(items))
            except Exception:  # noqa: BLE001 - a flush exception would stall the update graph
                traceback.print_exc()
        return on_flush

    def _status_flush(self, publisher: Any) -> None:
        try:
            items = self.status_buffer.drain()
            if items:
                with self._ctx:
                    publisher.add(self._status_batch(items))
        except Exception:  # noqa: BLE001
            traceback.print_exc()

    @staticmethod
    def _batch(items: Sequence[Tuple[Row, str, int]]) -> Any:
        from deephaven import new_table
        from deephaven.column import int_col, string_col

        from oms_trace import sources

        cols = sources.raw_columns([row for row, _, _ in items])
        cols.append(string_col("SowStream", [stream for _, stream, _ in items]))
        cols.append(int_col("SowEpoch", [epoch for _, _, epoch in items]))
        return new_table(cols)

    @staticmethod
    def _status_batch(items: Sequence[Row]) -> Any:
        from deephaven import new_table

        from oms_trace import sources

        return new_table(sources.typed_columns(STATUS_COLUMNS, items))

    # -- AMPS ---------------------------------------------------------------------------

    def handler(self, system: str, topic: str) -> Callable[[Any], None]:
        """The message handler for one topic (runs on the AMPS receive thread)."""
        key = (system, topic)
        mirror = self.mirrors[key]
        buffer = self.buffers[system]

        def on_message(message: Any) -> None:
            try:
                now = self._clock()
                command = _text(message.get_command())
                for row, stream in mirror.on_message(message, now):
                    buffer.offer((row, stream, mirror.epoch))
                boundary = command in ("group_begin", "group_end")
                if boundary or now - self._status_ts.get(key, 0) >= STATUS_INTERVAL_NS:
                    self._status_ts[key] = now
                    self.status_buffer.offer(mirror.status(self.connected, now))
                if command == "group_end":
                    print(f"[oms-trace] AMPS {topic} ({system}): snapshot {mirror.epoch} complete, "
                          f"{mirror.live()} live records, {mirror.counts['dropped']} dropped so far", flush=True)
            except Exception:  # noqa: BLE001 - a handler exception would kill the subscription
                traceback.print_exc()

        return on_message

    def connect(self) -> None:
        """Connect, log on and subscribe every topic (raises on failure)."""
        client = self._client_factory(self.amps)
        try:
            client.add_connection_state_listener(self._on_connection_state)
            client.connect_and_logon()
            for system, topic in self.amps.subscriptions():
                command = self._command_factory(topic, self.amps)
                self._sub_ids.append(client.execute_async(command, self.handler(system, topic)))
        except Exception:
            self._safe_close(client)
            raise
        self._client = client
        self._set_connected(True)
        print(f"[oms-trace] AMPS subscribed ({self.amps.describe()}): "
              + ", ".join(f"{t} -> {s}" for s, t in self.amps.subscriptions()), flush=True)

    def start(self, background: bool = True) -> Dict[str, Any]:
        """Build the tables and connect: in a background thread by default, retrying until it
        succeeds, so the worker comes up (with empty tables) even while AMPS is unreachable."""
        if not self.tables:
            self.build_tables()
        if not background:
            self.connect()
            return self.tables

        def run() -> None:
            while not self._stop.is_set():
                try:
                    self.connect()
                    return
                except Exception as exc:  # noqa: BLE001 - keep retrying; the reason is logged
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    print(f"[oms-trace] AMPS connect failed ({self.last_error}); retrying in "
                          f"{self._retry_seconds:g}s", flush=True)
                    self._stop.wait(self._retry_seconds)

        self._thread = threading.Thread(target=run, name="oms-trace-amps-connect", daemon=True)
        self._thread.start()
        return self.tables

    def stop(self) -> None:
        """Unsubscribe and close (best effort; never raises)."""
        self._stop.set()
        client, self._client = self._client, None
        for buffer in self.buffers.values():
            buffer.close()
        self.status_buffer.close()
        if client is None:
            return
        for sub_id in self._sub_ids:
            try:
                client.unsubscribe(sub_id)
            except Exception:  # noqa: BLE001 - shutdown must never raise
                traceback.print_exc()
        self._sub_ids = []
        self._safe_close(client)
        self.connected = False

    def status(self) -> List[Row]:
        """One row per topic (for the console; the ``amps_topics`` table is the live view)."""
        now = self._clock()
        return [mirror.status(self.connected, now) for _, mirror in sorted(self.mirrors.items())]

    @staticmethod
    def _safe_close(client: Any) -> None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()

    def _on_connection_state(self, state: Any) -> None:
        # The python client passes True when a connection is established (before logon and the
        # re-subscription, so a reconnect's snapshot follows) and False on a disconnect.
        self._set_connected(bool(state))

    def _set_connected(self, connected: bool) -> None:
        if connected == self.connected:
            return
        self.connected = connected
        print(f"[oms-trace] AMPS {'connected' if connected else 'disconnected'}: {self.amps.describe()}", flush=True)
        now = self._clock()
        for key, mirror in self.mirrors.items():
            self._status_ts[key] = now
            self.status_buffer.offer(mirror.status(connected, now))


#: Strong references: a collected source takes its AMPS client, and the streams, with it.
_ACTIVE: List[AmpsSowSource] = []


def start_source(settings: Settings, amps: Optional[AmpsSettings] = None, background: bool = True,
                 **kwargs: Any) -> AmpsSowSource:
    """Create, start and keep alive an :class:`AmpsSowSource`; its tables are ready at once.

    Idempotent: a PQ script re-run in a console gets the running source back. Two HA clients
    under one client name would displace each other's logon and reconnect forever, so a running
    source with the same name but other settings is stopped first.
    """
    amps = amps or AmpsSettings.from_env(settings=settings)
    for running in list(_ACTIVE):
        if running.amps.client_name != amps.client_name:
            continue
        if running.amps == amps and running.settings == settings:
            print(f"[oms-trace] AMPS source already running: {amps.describe()}", flush=True)
            return running
        running.stop()
        _ACTIVE.remove(running)
    source = AmpsSowSource(settings, amps, **kwargs)
    source.start(background=background)
    _ACTIVE.append(source)
    return source
