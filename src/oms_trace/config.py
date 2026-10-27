"""The fixed topology, the four edges, the break taxonomy and the settings (docs/design/01, 03).

Everything here is data: the six levels and their parent links, the edges with their scope and
expected cardinality, the ordered break kinds, the raw column contract, and ``Settings`` read
from the environment with validation (a malformed value is a start-up error, never a silent
default).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

SYSTEMS: Tuple[str, ...] = ("A", "B", "C")

#: Level name -> system. Six levels, five hops (docs/design/01 section 1). System A holds the
#: client-side order (``A_ROOT``, the root of everything) and the destination-side order it routes
#: to (``A_DEST``, tag 5011 = the client order's id, one per client order); B's parent references
#: the destination order.
LEVELS: Tuple[str, ...] = ("A_ROOT", "A_DEST", "B_PARENT", "B_CHILD", "C_PARENT", "C_CHILD")
LEVEL_SYSTEM: Dict[str, str] = {"A_ROOT": "A", "A_DEST": "A", "B_PARENT": "B", "B_CHILD": "B", "C_PARENT": "C", "C_CHILD": "C"}
#: Child level -> parent level.
LEVEL_PARENT: Dict[str, str] = {"A_DEST": "A_ROOT", "B_PARENT": "A_DEST", "B_CHILD": "B_PARENT", "C_PARENT": "B_CHILD",
                                "C_CHILD": "C_PARENT"}
#: Child level -> the raw column that carries its parent's id (cross-system or in-system).
LEVEL_LINK_COLUMN: Dict[str, str] = {
    "A_DEST": "ParentOrderId",     # tag 5011: the client order's id
    "B_PARENT": "UpstreamRef",     # the destination order's id
    "B_CHILD": "ParentOrderId",
    "C_PARENT": "UpstreamRef",
    "C_CHILD": "ParentOrderId",
}
#: System -> (parent level, child level): every system splits its book on the in-system parent id
#: column (null = parent), docs/design/03 section 3.
SYSTEM_LEVELS: Dict[str, Tuple[str, str]] = {"A": ("A_ROOT", "A_DEST"), "B": ("B_PARENT", "B_CHILD"), "C": ("C_PARENT", "C_CHILD")}
PARENT_SYSTEM: Dict[str, str] = {level: LEVEL_SYSTEM[parent] for level, parent in LEVEL_PARENT.items()}
#: Level -> depth in the tree (A_ROOT is 0).
LEVEL_DEPTH: Dict[str, int] = {name: i for i, name in enumerate(LEVELS)}
#: The street level: its quantities are the market-facing truth.
STREET_LEVEL = "C_CHILD"


@dataclass(frozen=True)
class Edge:
    """One hop: a parent level reconciled against the sum of its direct children."""

    name: str
    parent_level: str
    child_level: str
    scope: str  # CROSS (two systems) or INTERNAL (one system's parent and child books)
    expect_one: bool  # 1:1 hop: exactly one child per parent is expected

    @property
    def parent_system(self) -> str:
        return LEVEL_SYSTEM[self.parent_level]

    @property
    def child_system(self) -> str:
        return LEVEL_SYSTEM[self.child_level]


EDGES: Tuple[Edge, ...] = (
    Edge("Cli-Dest", "A_ROOT", "A_DEST", "INTERNAL", True),
    Edge("A-Bp", "A_DEST", "B_PARENT", "CROSS", True),
    Edge("Bp-Bc", "B_PARENT", "B_CHILD", "INTERNAL", False),
    Edge("Bc-Cp", "B_CHILD", "C_PARENT", "CROSS", True),
    Edge("Cp-Cc", "C_PARENT", "C_CHILD", "INTERNAL", False),
)
EDGE_BY_NAME: Dict[str, Edge] = {e.name: e for e in EDGES}

#: ``BreakKind`` values in evaluation order (first match wins), docs/design/03 section 6.
BREAK_KINDS: Tuple[str, ...] = (
    "FEED_STALE",
    "OVERFILL",
    "TRANSIENT",
    "CARDINALITY",
    "MISSING_CHILD",
    "MISSING_DOWNSTREAM",
    "UNBOOKED_UPSTREAM",
    "LEAVES_MISMATCH",
    "CHILD_STILL_OPEN",
    "OVER_ROUTED",
    "HELD",
    "NONE",
)
#: Kinds that are not drift: suppressed, too young, structural-only, informational, clean.
NON_DRIFT_KINDS: frozenset = frozenset({"FEED_STALE", "TRANSIENT", "MISSING_CHILD", "HELD", "NONE"})
#: Kinds on child rows (links).
LINK_STATES: Tuple[str, ...] = ("ROOT", "LINKED", "UNRESOLVED", "NO_LINK")
TOPIC_STATES: Tuple[str, ...] = ("CURRENT", "STALE", "CATCHING_UP")

#: The raw per-system table contract (docs/design/03 section 2): column -> Deephaven type.
RAW_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("Date", "String"),
    ("OrderId", "String"),
    ("UpstreamRef", "String"),
    ("ParentOrderId", "String"),
    ("Symbol", "String"),
    ("Side", "String"),
    ("OrderQty", "long"),
    ("CumQty", "long"),
    ("LeavesQty", "long"),
    ("AvgPx", "double"),
    ("OrdStatus", "String"),
    ("EventTs", "Instant"),
    ("RecvTs", "Instant"),
    ("SeqNo", "long"),
    ("Topic", "String"),
    ("KafkaPartition", "int"),
    ("KafkaOffset", "long"),
    # docs/design/11 section 4.1: the AMPS SOW source sets it to true when the record left the SOW
    # (oof / sow_delete / not re-sent by a reconnect snapshot); null on Kafka-fed rows.
    ("SowDeleted", "boolean"),
)
RAW_COLUMN_NAMES: Tuple[str, ...] = tuple(name for name, _ in RAW_COLUMNS)
RAW_COLUMN_KINDS: Dict[str, str] = dict(RAW_COLUMNS)
#: Columns the ingester (Kafka) or the source (AMPS) stamps; never read from a message body.
INGESTER_COLUMNS: Tuple[str, ...] = ("RecvTs", "Topic", "KafkaPartition", "KafkaOffset", "SowDeleted")

#: The canonical node columns every adapter view produces (docs/design/03 section 3).
CANONICAL_COLUMNS: Tuple[str, ...] = (
    "Level", "System", "OrderId", "LinkId", "Symbol", "Side", "OrderQty", "CumQty", "LeavesQty",
    "Status", "RecvTs", "Topic", "AgeTs", "Present",
)

SECOND_NANOS = 1_000_000_000


@dataclass(frozen=True)
class Settings:
    """Every knob of docs/design/03 section 8 and 07 section 8."""

    raw_namespace: str = "OMS_RAW"
    out_namespace: str = "OMS_TRACE"
    date_window_days: int = 2
    gap_age_sec: int = 30
    unresolved_alert_sec: int = 3600
    clock_sec: int = 30
    feed_stale_sec: int = 120
    feed_catchup_sec: int = 300
    age_clock: str = "recv"  # recv | event
    parent_id_column: str = "ParentOrderId"
    expect_one_edges: Tuple[str, ...] = ("Cli-Dest", "A-Bp", "Bc-Cp")
    one_table_per_system: bool = True
    topics: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    tz: str = "America/New_York"
    ui_max_roots: int = 20
    source: str = "mock"  # mock | files | enterprise | amps
    mock_seed: int = 42
    mock_families: int = 24
    #: "today" for the Date window; None means the wall clock (the PQ's daily restart rolls it).
    as_of: Optional[date] = None

    def dates(self) -> List[str]:
        """today-(days-1) .. today, plus tomorrow so midnight needs no restart (docs/design/04 §1)."""
        today = self.as_of or date.today()
        return [str(today + timedelta(days=d)) for d in range(-(self.date_window_days - 1), 2)]

    def edge_expects_one(self, edge: Edge) -> bool:
        return edge.name in self.expect_one_edges

    def age_column(self) -> str:
        return "EventTs" if self.age_clock == "event" else "RecvTs"

    def raw_table_names(self, kind: str, system: str) -> List[str]:
        """``Orders<X>`` (one table per system) or ``Orders<X>_<topic>`` per topic (docs/design/05 §4.3b)."""
        topics = tuple(self.topics.get(system, ()))
        if self.one_table_per_system or not topics:
            return [f"{kind}{system}"]
        return [f"{kind}{system}_{t.rsplit('.', 1)[-1]}" for t in topics]

    def with_(self, **changes: Any) -> "Settings":
        return replace(self, **changes)


class SettingsError(ValueError):
    """A malformed setting; the message names the key."""


def _int(env: Mapping[str, str], key: str, default: int, minimum: int = 0) -> int:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{key}={raw!r} is not an integer") from exc
    if value < minimum:
        raise SettingsError(f"{key}={value} must be >= {minimum}")
    return value


def _bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    if raw.lower() in ("true", "1", "yes"):
        return True
    if raw.lower() in ("false", "0", "no"):
        return False
    raise SettingsError(f"{key}={raw!r} must be true or false")


def _choice(env: Mapping[str, str], key: str, default: str, choices: Sequence[str]) -> str:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    if raw not in choices:
        raise SettingsError(f"{key}={raw!r} must be one of {list(choices)}")
    return raw


def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    """Read every ``OMS_TRACE_*`` / ``OMS_TOPICS_*`` key, validating each one."""
    env = os.environ if env is None else env
    edges_raw = env.get("OMS_TRACE_EXPECT_ONE_EDGES", "Cli-Dest,A-Bp,Bc-Cp")
    expect_one = tuple(e.strip() for e in edges_raw.split(",") if e.strip())
    unknown = [e for e in expect_one if e not in EDGE_BY_NAME]
    if unknown:
        raise SettingsError(f"OMS_TRACE_EXPECT_ONE_EDGES names unknown edges {unknown}; known: {list(EDGE_BY_NAME)}")
    topics: Dict[str, Tuple[str, ...]] = {}
    for system in SYSTEMS:
        raw = env.get(f"OMS_TOPICS_{system}", "")
        topics[system] = tuple(t.strip() for t in raw.split(",") if t.strip())
    as_of_raw = env.get("OMS_TRACE_AS_OF", "")
    as_of: Optional[date] = None
    if as_of_raw:
        try:
            as_of = date.fromisoformat(as_of_raw)
        except ValueError as exc:
            raise SettingsError(f"OMS_TRACE_AS_OF={as_of_raw!r} is not a yyyy-mm-dd date") from exc
    return Settings(
        raw_namespace=env.get("OMS_TRACE_RAW_NAMESPACE", "OMS_RAW") or "OMS_RAW",
        out_namespace=env.get("OMS_TRACE_OUT_NAMESPACE", "OMS_TRACE") or "OMS_TRACE",
        date_window_days=_int(env, "OMS_TRACE_DATE_WINDOW_DAYS", 2, minimum=1),
        gap_age_sec=_int(env, "OMS_TRACE_GAP_AGE_SEC", 30),
        unresolved_alert_sec=_int(env, "OMS_TRACE_UNRESOLVED_ALERT_SEC", 3600),
        clock_sec=_int(env, "OMS_TRACE_CLOCK_SEC", 30, minimum=1),
        feed_stale_sec=_int(env, "OMS_TRACE_FEED_STALE_SEC", 120, minimum=1),
        feed_catchup_sec=_int(env, "OMS_TRACE_FEED_CATCHUP_SEC", 300, minimum=1),
        age_clock=_choice(env, "OMS_TRACE_AGE_CLOCK", "recv", ("recv", "event")),
        parent_id_column=env.get("OMS_TRACE_PARENT_ID_COLUMN", "ParentOrderId") or "ParentOrderId",
        expect_one_edges=expect_one,
        one_table_per_system=_bool(env, "OMS_TRACE_ONE_TABLE_PER_SYSTEM", True),
        topics=topics,
        tz=env.get("OMS_TRACE_TZ", "America/New_York") or "America/New_York",
        ui_max_roots=_int(env, "OMS_TRACE_UI_MAX_ROOTS", 20, minimum=1),
        source=_choice(env, "OMS_TRACE_SOURCE", "mock", ("mock", "files", "enterprise", "amps")),
        mock_seed=_int(env, "OMS_TRACE_MOCK_SEED", 42),
        mock_families=_int(env, "OMS_TRACE_MOCK_FAMILIES", 24),
        as_of=as_of,
    )


def sec(nanos: int) -> int:
    """Nanoseconds -> whole seconds, truncating toward zero like Java's long division."""
    q = abs(nanos) // SECOND_NANOS
    return q if nanos >= 0 else -q


def level_rows_of(rows: Iterable[Mapping[str, Any]], level: str) -> List[Mapping[str, Any]]:
    return [r for r in rows if r.get("Level") == level]
