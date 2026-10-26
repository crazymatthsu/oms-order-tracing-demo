"""A five-level mock book and the scenario catalog (docs/design/09 section 2).

Every scenario starts from a consistent family (A -> Bp -> Bc* -> Cp* -> Cc*) and breaks one
thing on purpose. Ids are readable and unique: ``A-<name>``, ``Bp-<name>``, ``Bc-<name>-<i>``,
``Cp-<name>-<i>``, ``Cc-<name>-<i>-<j>``. Rows carry the raw contract of docs/design/03
section 2 with timestamps as epoch nanoseconds (UTC) relative to a fixed ``NOW``, so the oracle
and the engine see identical inputs and the tests are deterministic.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from oms_trace import config

SEC = config.SECOND_NANOS
MS = 1_000_000
MIN = 60 * SEC
HOUR = 3600 * SEC

#: 2026-10-26T15:00:00Z (11:00 New York) as epoch nanoseconds; TODAY is the Date of that instant in
#: America/New_York, which the AMPS source derives from RecvTs (docs/design/11 section 4.4).
NOW = 1_793_026_800 * SEC
TODAY = "2026-10-26"
YESTERDAY = "2026-10-25"

TOPICS: Dict[str, Tuple[str, ...]] = {
    "A": ("oms.a.t1", "oms.a.t2"),
    "B": ("oms.b.t1", "oms.b.t2", "oms.b.t3"),
    "C": ("oms.c.t1", "oms.c.t2", "oms.c.t3"),
}
#: Special channels used by exactly one scenario each (docs/design/09 section 2).
TOPIC_C_STALE = "oms.c.stale"      # silent for 10 minutes: STALE
TOPIC_B_CATCHUP = "oms.b.catchup"  # recent receipts, event time two hours behind: CATCHING_UP

SYMBOLS = ("AAPL", "MSFT", "NVDA", "AMZN", "JPM", "CSCO", "CAT", "GS")

Row = Dict[str, Any]


@dataclass
class Scenario:
    name: str
    root_id: str
    ids: Dict[str, str]
    note: str


@dataclass
class Book:
    rows: Dict[str, List[Row]] = field(default_factory=lambda: {"A": [], "B": [], "C": []})
    now: int = NOW
    scenarios: Dict[str, Scenario] = field(default_factory=dict)
    _offsets: Dict[str, int] = field(default_factory=dict)

    def add(self, system: str, row: Row) -> Row:
        topic = row["Topic"]
        row["KafkaOffset"] = self._offsets.get(topic, 0)
        self._offsets[topic] = row["KafkaOffset"] + 1
        self.rows[system].append(row)
        return row

    def all_rows(self) -> List[Row]:
        return [r for system in config.SYSTEMS for r in self.rows[system]]

    def find(self, system: str, order_id: str) -> List[Row]:
        return [r for r in self.rows[system] if r["OrderId"] == order_id]

    def last(self, system: str, order_id: str) -> Row:
        return self.find(system, order_id)[-1]


def _status(qty: int, cum: int, leaves: int, canceled: bool) -> str:
    if canceled:
        return "CANCELED"
    if cum >= qty:
        return "FILLED"
    return "PARTIALLY_FILLED" if cum > 0 else "NEW"


def raw_row(
    order_id: str,
    *,
    upstream: Optional[str],
    parent: Optional[str],
    symbol: str,
    side: str,
    qty: int,
    cum: int,
    recv: int,
    topic: str,
    leaves: Optional[int] = None,
    canceled: bool = False,
    status: Optional[str] = None,
    event: Optional[int] = None,
    seq: int = 1,
    date: str = TODAY,
    avg_px: Optional[float] = None,
) -> Row:
    """One raw state message (docs/design/03 section 2). ``parent`` None marks a parent order."""
    if leaves is None:
        leaves = 0 if canceled else max(qty - cum, 0)
    return {
        "Date": date,
        "OrderId": order_id,
        "UpstreamRef": upstream,
        "ParentOrderId": parent,
        "Symbol": symbol,
        "Side": side,
        "OrderQty": qty,
        "CumQty": cum,
        "LeavesQty": leaves,
        "AvgPx": avg_px if avg_px is not None else (100.0 if cum > 0 else None),
        "OrdStatus": status if status is not None else _status(qty, cum, leaves, canceled),
        "EventTs": recv - 20 * MS if event is None else event,
        "RecvTs": recv,
        "SeqNo": seq,
        "Topic": topic,
        "KafkaPartition": sum(ord(c) for c in order_id) % 4,
        "KafkaOffset": 0,  # assigned by Book.add
        "SowDeleted": None,  # set by the AMPS SOW source only (docs/design/11 section 4.1)
    }


def split(total: int, parts: int) -> List[int]:
    base, rem = divmod(total, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]


def _fills(quantities: Sequence[int], fill: float) -> List[int]:
    return [int(round(q * fill)) for q in quantities]


def family(
    book: Book,
    name: str,
    *,
    symbol: str = "AAPL",
    side: str = "BUY",
    qty: int = 1000,
    splits: Optional[Sequence[int]] = None,
    cc_splits: Optional[Sequence[Sequence[int]]] = None,
    fill: float = 1.0,
    recv: int = NOW - 10 * MIN,
    topics: Optional[Dict[str, Sequence[str]]] = None,
    note: str = "",
    date: str = TODAY,
) -> Scenario:
    """A consistent five-level family: every book agrees on executed and open quantity."""
    topics = topics or TOPICS
    bc_qty = list(splits) if splits is not None else split(qty, 2)
    assert sum(bc_qty) == qty, f"{name}: splits {bc_qty} do not sum to {qty}"
    cc_qty = [list(s) for s in cc_splits] if cc_splits is not None else [split(q, 2) for q in bc_qty]
    for i, (bq, ccq) in enumerate(zip(bc_qty, cc_qty)):
        assert sum(ccq) == bq, f"{name}: Cc splits {ccq} under Bc {i + 1} do not sum to {bq}"
    cc_cum = [_fills(ccq, fill) for ccq in cc_qty]
    bc_cum = [sum(c) for c in cc_cum]
    a_cum = sum(bc_cum)

    ids: Dict[str, str] = {"A": f"A-{name}", "Bp": f"Bp-{name}"}
    t = recv
    pick = lambda system, n: topics[system][n % len(topics[system])]  # noqa: E731
    book.add("A", raw_row(ids["A"], upstream=None, parent=None, symbol=symbol, side=side, qty=qty, cum=a_cum,
                          recv=t, topic=pick("A", len(book.scenarios)), date=date))
    t += MS
    book.add("B", raw_row(ids["Bp"], upstream=ids["A"], parent=None, symbol=symbol, side=side, qty=qty, cum=a_cum,
                          recv=t, topic=pick("B", len(book.scenarios)), date=date))
    for i, bq in enumerate(bc_qty, start=1):
        t += MS
        bc_id = f"Bc-{name}-{i}"
        ids[f"Bc{i}"] = bc_id
        book.add("B", raw_row(bc_id, upstream=None, parent=ids["Bp"], symbol=symbol, side=side, qty=bq,
                              cum=bc_cum[i - 1], recv=t, topic=pick("B", len(book.scenarios) + i), date=date))
    for i, bq in enumerate(bc_qty, start=1):
        t += MS
        cp_id = f"Cp-{name}-{i}"
        ids[f"Cp{i}"] = cp_id
        book.add("C", raw_row(cp_id, upstream=ids[f"Bc{i}"], parent=None, symbol=symbol, side=side, qty=bq,
                              cum=bc_cum[i - 1], recv=t, topic=pick("C", len(book.scenarios) + i), date=date))
        for j, (cq, ccum) in enumerate(zip(cc_qty[i - 1], cc_cum[i - 1]), start=1):
            t += MS
            cc_id = f"Cc-{name}-{i}-{j}"
            ids[f"Cc{i}{chr(96 + j)}"] = cc_id
            book.add("C", raw_row(cc_id, upstream=None, parent=cp_id, symbol=symbol, side=side, qty=cq, cum=ccum,
                                  recv=t, topic=pick("C", len(book.scenarios) + i + j), date=date))
    scenario = Scenario(name=name, root_id=ids["A"], ids=ids, note=note)
    book.scenarios[name] = scenario
    return scenario


def _set(book: Book, system: str, order_id: str, **values: Any) -> Row:
    """Mutate the latest state of an order in place (the book stays a valid message stream)."""
    row = book.last(system, order_id)
    row.update(values)
    if "CumQty" in values or "LeavesQty" in values or "OrderQty" in values:
        if "OrdStatus" not in values:
            row["OrdStatus"] = _status(row["OrderQty"], row["CumQty"], row["LeavesQty"], row["OrdStatus"] == "CANCELED")
    return row


def _remove(book: Book, system: str, order_id: str) -> None:
    book.rows[system] = [r for r in book.rows[system] if r["OrderId"] != order_id]


def _clean(book: Book, name: str, note: str, **kw: Any) -> Scenario:
    kw.setdefault("splits", [600, 400])
    kw.setdefault("cc_splits", [[300, 300], [400]])
    return family(book, name, note=note, **kw)


def scenario_catalog(book: Book) -> None:
    """Add every scenario of docs/design/09 section 2 that a static book can express."""
    _clean(book, "CLEAN", "clean_chain: all fills booked everywhere")
    family(book, "WORK", splits=[600, 400], cc_splits=[[300, 300], [400]], fill=0.4, note="working: partial, consistent")

    s = _clean(book, "MB", "missing_at_b: B's child book shows 300 of Bc2's 400")
    _set(book, "B", s.ids["Bc2"], CumQty=300, LeavesQty=100)

    s = _clean(book, "MC", "missing_at_c: Cp2 (and its child) show 300 of Bc2's 400")
    _set(book, "C", s.ids["Cp2"], CumQty=300, LeavesQty=100)
    _set(book, "C", s.ids["Cc2a"], CumQty=300, LeavesQty=100)

    s = _clean(book, "MCC", "missing_at_c_children: Cp2 shows 400, its child 300")
    _set(book, "C", s.ids["Cc2a"], CumQty=300, LeavesQty=100)

    s = _clean(book, "UA", "unbooked_at_a: A shows 900, everything below 1000")
    _set(book, "A", s.ids["A"], CumQty=900, LeavesQty=100)

    s = _clean(book, "BPL", "b_parent_lags: Bp shows 900, its children 1000")
    _set(book, "B", s.ids["Bp"], CumQty=900, LeavesQty=100)

    s = _clean(book, "TR", "transient: as missing_at_c, but C's state is 10 s old")
    _set(book, "C", s.ids["Cp2"], CumQty=300, LeavesQty=100, RecvTs=NOW - 10 * SEC, EventTs=NOW - 10 * SEC - 20 * MS)
    _set(book, "C", s.ids["Cc2a"], CumQty=300, LeavesQty=100, RecvTs=NOW - 10 * SEC, EventTs=NOW - 10 * SEC - 20 * MS)

    s = _clean(book, "BCM", "bc_missing (late_bc_hours / bc_never): Bc1 never arrived; Cp1 has waited 2 h", recv=NOW - 2 * HOUR)
    _remove(book, "B", s.ids["Bc1"])

    s = _clean(book, "BPM", "late_bp: the B parent has not arrived")
    _remove(book, "B", s.ids["Bp"])

    s = _clean(book, "AM", "late_a: the A root has not arrived")
    _remove(book, "A", s.ids["A"])

    s = _clean(book, "NL", "no_link: Cp1 carries no B reference")
    _set(book, "C", s.ids["Cp1"], UpstreamRef=None)

    s = _clean(book, "EP", "empty_parent_id: Bc1 carries an empty-string parent id (a child with no link, by contract)")
    _set(book, "B", s.ids["Bc1"], ParentOrderId="")

    s = _clean(book, "TB", "two_b_parents: a second B parent references the same A")
    dup = dict(book.last("B", s.ids["Bp"]))
    dup.update(OrderId=f"Bp-{s.name}-dup", RecvTs=dup["RecvTs"] + MS, EventTs=dup["EventTs"] + MS)
    book.add("B", dup)
    s.ids["BpDup"] = dup["OrderId"]

    s = family(book, "NRC", qty=1400, splits=[600, 400, 400], cc_splits=[[300, 300], [400], [400]], fill=1.0,
               note="b_child_never_reached_c: Bc3 (400, unfilled) has no C parent")
    _set(book, "B", s.ids["Bc3"], CumQty=0, LeavesQty=400)
    _set(book, "B", s.ids["Bp"], CumQty=1000, LeavesQty=400)
    _set(book, "A", s.ids["A"], CumQty=1000, LeavesQty=400)
    _remove(book, "C", s.ids["Cp3"])
    for key in ("Cc3a",):
        _remove(book, "C", s.ids[key])

    s = _clean(book, "MM", "mismatch: Cc1a is a SELL under a BUY C parent")
    _set(book, "C", s.ids["Cc1a"], Side="SELL")

    s = _clean(book, "OF", "overfill: Cp1 shows CumQty above its OrderQty")
    _set(book, "C", s.ids["Cp1"], CumQty=700, LeavesQty=0)

    s = _clean(book, "DUP", "duplicate_messages: Cc1a's state delivered twice")
    book.add("C", dict(book.last("C", s.ids["Cc1a"])))

    s = _clean(book, "OOO", "out_of_order_same_order: Cc2a's SeqNo 2 (400) arrives before SeqNo 1 (300)")
    _set(book, "C", s.ids["Cc2a"], SeqNo=2)
    stale = dict(book.last("C", s.ids["Cc2a"]))
    stale.update(SeqNo=1, CumQty=300, LeavesQty=100, OrdStatus="PARTIALLY_FILLED", RecvTs=stale["RecvTs"] + MS)
    book.add("C", stale)

    s = _clean(book, "MID", "midnight: Bc1's state is on yesterday's Date partition")
    _set(book, "B", s.ids["Bc1"], Date=YESTERDAY)

    s = family(book, "HELD", qty=800, splits=[600, 200], cc_splits=[[300, 300], [200]], fill=0.0,
               note="held: Bp has 200 open that is not routed to any B child")
    # A and Bp are 1000-share orders of which B sliced 800: 700 open at Bp, 500 open below it.
    _set(book, "A", s.ids["A"], OrderQty=1000, CumQty=300, LeavesQty=700)
    _set(book, "B", s.ids["Bp"], OrderQty=1000, CumQty=300, LeavesQty=700)
    _set(book, "B", s.ids["Bc1"], CumQty=200, LeavesQty=400)
    _set(book, "B", s.ids["Bc2"], CumQty=100, LeavesQty=100)
    _set(book, "C", s.ids["Cp1"], CumQty=200, LeavesQty=400)
    _set(book, "C", s.ids["Cc1a"], CumQty=100, LeavesQty=200)
    _set(book, "C", s.ids["Cc1b"], CumQty=100, LeavesQty=200)
    _set(book, "C", s.ids["Cp2"], CumQty=100, LeavesQty=100)
    _set(book, "C", s.ids["Cc2a"], CumQty=100, LeavesQty=100)

    s = _clean(book, "OR", "over_routed: Cp1 has 300 open, its C children 400")
    _set(book, "A", s.ids["A"], CumQty=700, LeavesQty=300)
    _set(book, "B", s.ids["Bp"], CumQty=700, LeavesQty=300)
    _set(book, "B", s.ids["Bc1"], CumQty=300, LeavesQty=300)
    _set(book, "C", s.ids["Cp1"], CumQty=300, LeavesQty=300)
    _set(book, "C", s.ids["Cc1a"], CumQty=150, LeavesQty=150)
    _set(book, "C", s.ids["Cc1b"], OrderQty=400, CumQty=150, LeavesQty=250)

    s = _clean(book, "CNP", "cancel_not_propagated: everything canceled, Cc1b still working 300")
    _set(book, "A", s.ids["A"], CumQty=700, LeavesQty=0, OrdStatus="CANCELED")
    _set(book, "B", s.ids["Bp"], CumQty=700, LeavesQty=0, OrdStatus="CANCELED")
    _set(book, "B", s.ids["Bc1"], CumQty=300, LeavesQty=0, OrdStatus="CANCELED")
    _set(book, "C", s.ids["Cp1"], CumQty=300, LeavesQty=0, OrdStatus="CANCELED")
    _set(book, "C", s.ids["Cc1a"], CumQty=300, LeavesQty=0)
    _set(book, "C", s.ids["Cc1b"], CumQty=0, LeavesQty=300, OrdStatus="NEW")

    s = _clean(book, "LMX", "leaves_mismatch_cross: A has 400 open, its B parent 300")
    _set(book, "A", s.ids["A"], CumQty=600, LeavesQty=400)
    _set(book, "B", s.ids["Bp"], CumQty=600, LeavesQty=300)
    for key in ("Bc1", "Bc2"):
        _set(book, "B", s.ids[key], CumQty=300, LeavesQty=book.last("B", s.ids[key])["OrderQty"] - 300)
    _set(book, "B", s.ids["Bc2"], CumQty=300, LeavesQty=100)
    _set(book, "C", s.ids["Cp1"], CumQty=300, LeavesQty=300)
    _set(book, "C", s.ids["Cc1a"], CumQty=300, LeavesQty=0)
    _set(book, "C", s.ids["Cc1b"], CumQty=0, LeavesQty=300)
    _set(book, "C", s.ids["Cp2"], CumQty=300, LeavesQty=100)
    _set(book, "C", s.ids["Cc2a"], CumQty=300, LeavesQty=100)

    stale_topics = {"A": TOPICS["A"], "B": TOPICS["B"], "C": (TOPIC_C_STALE,)}
    s = _clean(book, "FS", "feed_stale_suppresses: the C channel is silent; Cp2 is short", topics=stale_topics)
    _set(book, "C", s.ids["Cp2"], CumQty=300, LeavesQty=100)
    _set(book, "C", s.ids["Cc2a"], CumQty=300, LeavesQty=100)

    catchup_topics = {"A": TOPICS["A"], "B": (TOPIC_B_CATCHUP,), "C": TOPICS["C"]}
    s = _clean(book, "FC", "feed_catching_up: B's channel replays a backlog; Bc2 is short", topics=catchup_topics)
    for key in ("Bp", "Bc1", "Bc2"):
        _set(book, "B", s.ids[key], RecvTs=NOW - 30 * SEC, EventTs=NOW - 2 * HOUR)
    _set(book, "B", s.ids["Bc2"], CumQty=300, LeavesQty=100)

    family(book, "FAN", qty=30000, splits=[100] * 300, cc_splits=[[20] * 5] * 300, fill=1.0,
           note="large_fanout: 300 B children, 300 C parents, 1 500 C children")


def healthy_families(book: Book, families: int, seed: int) -> None:
    """Random consistent families spread over every regular topic, with recent receipts."""
    rng = random.Random(seed)
    for n in range(families):
        n_bc = rng.randint(1, 6)
        qty = 100 * rng.randint(1, 50)
        bc_q = split(qty, n_bc)
        cc_q = [split(q, rng.randint(1, 4)) for q in bc_q]
        fill = rng.choice((0.0, 0.3, 0.7, 1.0))
        recv = NOW - rng.randint(45, 30 * 60) * SEC
        family(book, f"H{n:02d}", symbol=rng.choice(SYMBOLS), side=rng.choice(("BUY", "SELL")), qty=qty,
               splits=bc_q, cc_splits=cc_q, fill=fill, recv=recv, note="healthy")
    # Every regular channel must have spoken within the stale threshold: a heartbeat family per topic.
    latest: Dict[str, int] = {}
    for row in book.all_rows():
        latest[row["Topic"]] = max(latest.get(row["Topic"], 0), row["RecvTs"])
    for system, topics in TOPICS.items():
        for topic in topics:
            if latest.get(topic, 0) < NOW - 90 * SEC:
                family(book, f"HB-{topic.replace('.', '-')}", qty=100, splits=[100], cc_splits=[[100]], fill=1.0,
                       recv=NOW - 45 * SEC, topics={s: ((topic,) if s == system else TOPICS[s]) for s in config.SYSTEMS},
                       note="heartbeat")


def generate(seed: int = 42, families: int = 24, scenarios: bool = True) -> Book:
    """The default book: the scenario catalog plus ``families`` random healthy families."""
    book = Book()
    if scenarios:
        scenario_catalog(book)
    healthy_families(book, families, seed)
    return book


def settings_for(book: Book, base: Optional[config.Settings] = None) -> config.Settings:
    """Settings whose Date window and topics match the book."""
    from datetime import date

    base = base or config.Settings()
    return base.with_(as_of=date.fromisoformat(TODAY),
                      topics={s: tuple(sorted({r["Topic"] for r in book.rows[s]})) for s in config.SYSTEMS})
