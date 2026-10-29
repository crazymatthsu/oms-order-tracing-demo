"""The pure-python oracle: every output table of docs/design/03 computed with dicts and loops.

``reconcile`` takes the raw rows per system (as the ingesters would have written them), the
settings and ``now``; it returns the same tables the Deephaven DAG (``oms_trace.dag``) builds,
keyed the way the embedded-engine test compares them. Nothing here imports Deephaven, so it
runs on any Python 3.10+ and pins the semantics the engine must reproduce row for row.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

from oms_trace import config
from oms_trace.config import EDGES, LEVELS, LEVEL_LINK_COLUMN, LEVEL_PARENT, LEVEL_SYSTEM, PARENT_SYSTEM, sec

Row = Dict[str, Any]


@dataclass
class Result:
    nodes: Dict[str, Dict[str, Row]] = field(default_factory=dict)          # level -> OrderId -> canonical node
    links: Dict[str, Dict[str, Row]] = field(default_factory=dict)          # child level -> OrderId -> linked row
    edges: Dict[str, Dict[str, Row]] = field(default_factory=dict)          # edge -> parent OrderId -> edge row
    feed_health: Dict[Tuple[str, str], Row] = field(default_factory=dict)   # (System, Topic) -> row
    system_health: Dict[str, int] = field(default_factory=dict)             # System -> max StaleInt
    gaps: Dict[Tuple[str, str], Row] = field(default_factory=dict)          # (Edge, OrderId) -> gap row
    unresolved_links: Dict[Tuple[str, str], Row] = field(default_factory=dict)  # (Level, OrderId)
    mismatch_links: Dict[Tuple[str, str], Row] = field(default_factory=dict)
    root_unresolved: Dict[Tuple[str, str], Row] = field(default_factory=dict)
    drift_by_a_order: Dict[str, Row] = field(default_factory=dict)          # RootId -> row
    drift_by_symbol: Dict[Tuple[str, str, str], Row] = field(default_factory=dict)  # (Symbol, Side, Edge)
    trace_stats: Dict[Tuple[str, str], int] = field(default_factory=dict)  # (Metric, Key) -> Value

    @property
    def all_links(self) -> Iterable[Row]:
        for level in LEVELS[1:]:
            yield from self.links[level].values()

    @property
    def drift_edges(self) -> Dict[Tuple[str, str], Row]:
        return {k: g for k, g in self.gaps.items() if g["IsDrift"]}

    @property
    def feed_suppressed(self) -> Dict[Tuple[str, str], Row]:
        return {k: g for k, g in self.gaps.items() if g["BreakKind"] == "FEED_STALE"}


def _s(value: Any) -> str:
    return "" if value is None else str(value)


def _q(value: Any) -> int:
    return 0 if value is None else int(value)


def last_states(rows: Iterable[Row], dates: Set[str]) -> Dict[str, Row]:
    """``last_by(OrderID)`` over the Date window: the latest row in arrival order per order."""
    latest: Dict[str, Row] = {}
    for row in rows:
        if row["Date"] in dates:
            latest[row["OrderID"]] = row
    return latest


def canonical(level: str, row: Row, settings: config.Settings) -> Row:
    link_col = LEVEL_LINK_COLUMN.get(level)
    link = "" if link_col is None else _s(row.get(link_col))
    age_col = settings.age_column()
    age_ts = row.get(age_col)
    if age_ts is None:
        age_ts = row["RecvTs"]
    return {
        "Level": level,
        "System": LEVEL_SYSTEM[level],
        "OrderId": row["OrderID"],
        "LinkId": link,
        "Symbol": _s(row.get("Symbol")),
        "Side": _s(row.get("Side")),
        "OrderQty": _q(row.get("OrderQty")),
        "CumQty": _q(row.get("CumQty")),
        "LeavesQty": _q(row.get("LeavesQty")),
        "Status": _s(row.get("OrdStatus")),
        "RecvTs": row["RecvTs"],
        "Topic": _s(row.get("Topic")),
        "AgeTs": age_ts,
        "Present": True,
    }


def split_roles(states: Mapping[str, Row], system: str, settings: config.Settings) -> Tuple[List[Row], List[Row]]:
    """Null parent id = parent, anything else (an empty string included) = child (docs/design/03 §3)."""
    parents, children = [], []
    for row in states.values():
        (parents if row.get(settings.parent_id_column) is None else children).append(row)
    return parents, children


def _link(children: Iterable[Row], parents: Mapping[str, Row], level: str) -> Dict[str, Row]:
    out: Dict[str, Row] = {}
    for child in children:
        parent = parents.get(child["LinkId"]) if child["LinkId"] else None
        row = dict(child)
        row.update({
            "ParentPresent": True if parent is not None else None,
            "ParentCumQty": parent["CumQty"] if parent else None,
            "ParentSymbol": parent["Symbol"] if parent else None,
            "ParentSide": parent["Side"] if parent else None,
            "ParentStatus": parent["Status"] if parent else None,
            "ParentRecvTs": parent["RecvTs"] if parent else None,
            "ParentRootId": parent["RootId"] if parent else None,
            "ParentSystem": PARENT_SYSTEM[level],
        })
        row["RootId"] = row["ParentRootId"]  # the root flows down the chain; unknown until every hop above is present
        row["ParentState"] = "NO_LINK" if child["LinkId"] == "" else ("UNRESOLVED" if parent is None else "LINKED")
        row["RootState"] = "UNRESOLVED" if row["RootId"] is None else "RESOLVED"
        row["UnresolvedSince"] = child["RecvTs"] if row["ParentState"] == "UNRESOLVED" else None
        row["Mismatch"] = bool(parent is not None and (child["Symbol"] != parent["Symbol"] or child["Side"] != parent["Side"]))
        out[child["OrderId"]] = row
    return out


def _edge(parents: Mapping[str, Row], children: Mapping[str, Row], edge: config.Edge, settings: config.Settings) -> Dict[str, Row]:
    rollup: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"ChildCount": 0, "ChildCumQty": 0, "ChildLeavesQty": 0,
                                                              "ChildLastTs": None, "ChildTopics": set()})
    for child in children.values():
        if child["LinkId"] == "":
            continue
        r = rollup[child["LinkId"]]
        r["ChildCount"] += 1
        r["ChildCumQty"] += child["CumQty"]
        r["ChildLeavesQty"] += child["LeavesQty"]
        r["ChildLastTs"] = child["AgeTs"] if r["ChildLastTs"] is None else max(r["ChildLastTs"], child["AgeTs"])
        r["ChildTopics"].add(child["Topic"])
    out: Dict[str, Row] = {}
    for parent in parents.values():
        r = rollup.get(parent["OrderId"], {"ChildCount": 0, "ChildCumQty": 0, "ChildLeavesQty": 0, "ChildLastTs": None, "ChildTopics": set()})
        row = dict(parent)
        row.update(r)
        row["ChildTopics"] = set(r["ChildTopics"])
        row["FillGap"] = parent["CumQty"] - r["ChildCumQty"]
        row["RouteGap"] = parent["LeavesQty"] - r["ChildLeavesQty"]
        row["LastChangeTs"] = parent["AgeTs"] if r["ChildLastTs"] is None or parent["AgeTs"] > r["ChildLastTs"] else r["ChildLastTs"]
        row["Edge"] = edge.name
        row["Scope"] = edge.scope
        row["ExpectOne"] = settings.edge_expects_one(edge)
        row["ChildSystem"] = edge.child_system
        out[parent["OrderId"]] = row
    return out


FeedStates = Mapping[Tuple[str, str], Mapping[str, Any]]


def feed_health(rows_by_system: Mapping[str, Iterable[Row]], dates: Set[str], settings: config.Settings, now: int,
                feed_states: Optional[FeedStates] = None) -> Dict[Tuple[str, str], Row]:
    """Per channel; ``feed_states`` ((System, Topic) -> Connected / SnapshotInProgress) is the
    AMPS source's override of docs/design/11 section 4.2."""
    out: Dict[Tuple[str, str], Row] = {}
    feed_states = feed_states or {}
    for system in config.SYSTEMS:
        last_recv: Dict[str, int] = {}
        last_event: Dict[str, Optional[int]] = {}
        for row in rows_by_system.get(system, ()):
            if row["Date"] not in dates:
                continue
            topic = _s(row.get("Topic"))
            last_recv[topic] = max(last_recv.get(topic, 0), row["RecvTs"])
            ev = row.get("TransactTime")
            if ev is not None:
                last_event[topic] = ev if last_event.get(topic) is None else max(last_event[topic], ev)
            else:
                last_event.setdefault(topic, None)
        for topic, recv in last_recv.items():
            silent = sec(now - recv)
            behind = sec(now - last_event[topic]) if last_event.get(topic) is not None else None
            fs = feed_states.get((system, topic), {})
            connected, snapshot = fs.get("Connected"), fs.get("SnapshotInProgress")
            if silent > settings.feed_stale_sec:
                state = "STALE"
            elif connected is False:
                state = "STALE"
            elif snapshot is True:
                state = "CATCHING_UP"
            elif behind is not None and behind > settings.feed_catchup_sec:
                state = "CATCHING_UP"
            else:
                state = "CURRENT"
            out[(system, topic)] = {
                "System": system, "Topic": topic, "LastRecvTs": recv, "LastEventTs": last_event.get(topic),
                "SilentSec": silent, "BehindSec": behind, "Connected": connected, "SnapshotInProgress": snapshot,
                "TopicState": state, "StaleInt": 0 if state == "CURRENT" else 1,
            }
    return out


def break_kind(g: Row, settings: config.Settings) -> str:
    if g["FeedStale"]:
        return "FEED_STALE"
    if g["CumQty"] > g["OrderQty"]:
        return "OVERFILL"
    if g["GapAgeSec"] < settings.gap_age_sec:
        return "TRANSIENT"
    if g["ExpectOne"] and g["ChildCount"] > 1:
        return "CARDINALITY"
    if g["ExpectOne"] and g["ChildCount"] == 0 and g["CumQty"] == 0:
        return "MISSING_CHILD"
    if g["FillGap"] > 0:
        return "MISSING_DOWNSTREAM"
    if g["FillGap"] < 0:
        return "UNBOOKED_UPSTREAM"
    if g["ExpectOne"] and g["RouteGap"] != 0:
        return "LEAVES_MISMATCH"
    if g["RouteGap"] < 0 and g["LeavesQty"] == 0:
        return "CHILD_STILL_OPEN"
    if g["RouteGap"] < 0:
        return "OVER_ROUTED"
    if g["RouteGap"] > 0:
        return "HELD"
    return "NONE"


def missed_by(g: Row) -> str:
    """A cross-system edge whose children are short names the child system; otherwise the parent's."""
    if g["Scope"] == "CROSS" and g["FillGap"] > 0:
        return g["ChildSystem"]
    return g["System"]


def reconcile(rows_by_system: Mapping[str, List[Row]], settings: config.Settings, now: int,
              feed_states: Optional[FeedStates] = None) -> Result:
    res = Result()
    dates = set(settings.dates())

    # 1. last state per order, role split (null parent id = parent), canonical nodes per level
    for system in config.SYSTEMS:
        states = last_states(rows_by_system.get(system, []), dates)
        parents, children = split_roles(states, system, settings)
        parent_level, child_level = config.SYSTEM_LEVELS[system]
        res.nodes[parent_level] = {r["OrderID"]: canonical(parent_level, r, settings) for r in parents}
        res.nodes[child_level] = {r["OrderID"]: canonical(child_level, r, settings) for r in children}
    for node in res.nodes["A_ROOT"].values():
        node.update(RootId=node["OrderId"], RootState="RESOLVED", ParentState="ROOT")

    # 2. links, top-down, carrying the root
    linked_parents: Dict[str, Mapping[str, Row]] = {"A_ROOT": res.nodes["A_ROOT"]}
    for level in LEVELS[1:]:
        parents = linked_parents[LEVEL_PARENT[level]]
        res.links[level] = _link(res.nodes[level].values(), parents, level)
        linked_parents[level] = res.links[level]

    # 3. edges
    for edge in EDGES:
        parents = linked_parents[edge.parent_level]
        res.edges[edge.name] = _edge(parents, res.links[edge.child_level], edge, settings)

    # 4. feed health
    res.feed_health = feed_health(rows_by_system, dates, settings, now, feed_states)
    # A system is "down" only when every one of its channels is non-current (min, not max): one
    # stale channel out of eight must not mask findings on the other seven (docs/design/04 §5).
    res.system_health = {}
    for (system, _), fh in res.feed_health.items():
        res.system_health[system] = min(res.system_health.get(system, 1), fh["StaleInt"])

    # 5. candidates -> gaps
    for edge in EDGES:
        for parent_id, e in res.edges[edge.name].items():
            if not (e["FillGap"] != 0 or e["RouteGap"] != 0 or (e["ExpectOne"] and e["ChildCount"] != 1)):
                continue
            g = dict(e)
            g["Now"] = now
            g["GapAgeSec"] = sec(now - e["LastChangeTs"])
            parent_feed = res.feed_health.get((e["System"], e["Topic"]))
            g["ParentFeed"] = parent_feed["TopicState"] if parent_feed else None
            child_states = [res.feed_health[(e["ChildSystem"], t)]["StaleInt"] for t in e["ChildTopics"]
                            if (e["ChildSystem"], t) in res.feed_health]
            if e["ChildCount"] == 0:
                g["ChildSideStale"] = res.system_health.get(e["ChildSystem"], 0) > 0
            else:
                g["ChildSideStale"] = bool(child_states) and max(child_states) > 0
            g["FeedStale"] = bool((g["ParentFeed"] is not None and g["ParentFeed"] != "CURRENT") or g["ChildSideStale"])
            g["BreakKind"] = break_kind(g, settings)
            g["ShortSide"] = "CHILD" if e["FillGap"] > 0 else "PARENT"
            g["OpenAt"] = "PARENT" if e["RouteGap"] > 0 else ("CHILD" if e["RouteGap"] < 0 else "")
            g["MissedBy"] = missed_by(g)
            g["IsDrift"] = g["BreakKind"] not in config.NON_DRIFT_KINDS
            res.gaps[(edge.name, parent_id)] = g

    # 6. unresolved, mismatch, root-unresolved
    for link in res.all_links:
        key = (link["Level"], link["OrderId"])
        if link["ParentState"] != "LINKED":
            u = dict(link)
            u["Now"] = now
            u["AgeSec"] = sec(now - link["AgeTs"])
            u["FeedStale"] = res.system_health.get(link["ParentSystem"], 0) > 0
            u["Alerted"] = (not u["FeedStale"]) and u["AgeSec"] >= settings.unresolved_alert_sec
            res.unresolved_links[key] = u
        if link["Mismatch"]:
            res.mismatch_links[key] = dict(link)
        if link["ParentState"] == "LINKED" and link["RootState"] == "UNRESOLVED":
            res.root_unresolved[key] = dict(link)

    # 7. drift by root
    street_cum: Dict[str, int] = defaultdict(int)
    street_leaves: Dict[str, int] = defaultdict(int)
    for cc in res.links["C_CHILD"].values():
        if cc["RootId"] is not None:
            street_cum[cc["RootId"]] += cc["CumQty"]
            street_leaves[cc["RootId"]] += cc["LeavesQty"]
    gap_sum: Dict[Tuple[str, str], int] = defaultdict(int)
    gap_breaks: Dict[Tuple[str, str], int] = defaultdict(int)
    held: Dict[str, int] = defaultdict(int)
    over: Dict[str, int] = defaultdict(int)
    for (edge_name, _), g in res.gaps.items():
        if g["RootId"] is None:
            continue
        if g["IsDrift"]:
            gap_sum[(edge_name, g["RootId"])] += g["FillGap"]
            gap_breaks[(edge_name, g["RootId"])] += 1
        # open-quantity sums over aged, feed-current rows only (HELD included, it is informational)
        if g["BreakKind"] not in ("TRANSIENT", "FEED_STALE", "MISSING_CHILD"):
            if g["RouteGap"] > 0:
                held[g["RootId"]] += g["RouteGap"]
            elif g["RouteGap"] < 0:
                over[g["RootId"]] += -g["RouteGap"]
    for root in res.nodes["A_ROOT"].values():
        rid = root["OrderId"]
        if not any((e.name, rid) in gap_breaks for e in EDGES):
            continue  # a root appears only when some edge below it is drift (HELD alone never is)
        row = {
            "RootId": rid, "Symbol": root["Symbol"], "Side": root["Side"],
            "ACumQty": root["CumQty"], "ALeavesQty": root["LeavesQty"],
            "StreetCumQty": street_cum.get(rid, 0), "StreetLeavesQty": street_leaves.get(rid, 0),
            "GapCliDest": gap_sum.get(("Cli-Dest", rid), 0),
            "GapABp": gap_sum.get(("A-Bp", rid), 0), "SumGapBpBc": gap_sum.get(("Bp-Bc", rid), 0),
            "SumGapBcCp": gap_sum.get(("Bc-Cp", rid), 0), "SumGapCpCc": gap_sum.get(("Cp-Cc", rid), 0),
            "HeldQty": held.get(rid, 0), "OverRoutedQty": over.get(rid, 0),
        }
        row["MissingAtDest"] = max(row["GapCliDest"], 0)
        row["UnbookedAtCli"] = max(-row["GapCliDest"], 0)
        row["MissingAtB"] = max(row["GapABp"], 0)
        row["UnbookedAtA"] = max(-row["GapABp"], 0)
        row["InternalB"] = row["SumGapBpBc"]
        row["MissingAtC"] = max(row["SumGapBcCp"], 0)
        row["UnbookedAtB"] = max(-row["SumGapBcCp"], 0)
        row["InternalC"] = row["SumGapCpCc"]
        row["Breaks"] = sum(gap_breaks.get((e.name, rid), 0) for e in EDGES)
        res.drift_by_a_order[rid] = row

    # 8. drift by symbol
    for g in res.drift_edges.values():
        key = (g["Symbol"], g["Side"], g["Edge"])
        row = res.drift_by_symbol.setdefault(key, {"Symbol": g["Symbol"], "Side": g["Side"], "Edge": g["Edge"], "Breaks": 0,
                                                    "ParentCumQty": 0, "ChildCumQty": 0, "MissingDownstream": 0,
                                                    "UnbookedUpstream": 0, "OverRouted": 0})
        row["Breaks"] += 1
        row["ParentCumQty"] += g["CumQty"]
        row["ChildCumQty"] += g["ChildCumQty"]
        row["MissingDownstream"] += max(g["FillGap"], 0)
        row["UnbookedUpstream"] += max(-g["FillGap"], 0)
        row["OverRouted"] += max(-g["RouteGap"], 0)

    # 9. stats (long format)
    st = res.trace_stats
    for level in LEVELS:
        st[("Nodes", level)] = len(res.nodes[level])
    for level in LEVELS[1:]:
        st[("Unresolved", level)] = sum(1 for l in res.links[level].values() if l["ParentState"] != "LINKED")
    for edge in EDGES:
        gs = [g for (e, _), g in res.gaps.items() if e == edge.name]
        st[("Gaps", edge.name)] = len(gs)
        st[("Drift", edge.name)] = sum(1 for g in gs if g["IsDrift"])
        st[("MissingDownstreamQty", edge.name)] = sum(max(g["FillGap"], 0) for g in gs if g["IsDrift"])
        st[("UnbookedUpstreamQty", edge.name)] = sum(max(-g["FillGap"], 0) for g in gs if g["IsDrift"])
    st[("CParentsMinusBChildren", "")] = len(res.nodes["C_PARENT"]) - len(res.nodes["B_CHILD"])
    st[("DestMinusCli", "")] = len(res.nodes["A_DEST"]) - len(res.nodes["A_ROOT"])
    for (system, topic), fh in res.feed_health.items():
        st[("TopicStale", f"{system}:{topic}")] = fh["StaleInt"]
    return res


def telescoping_holds(res: Result, root_id: str) -> Tuple[bool, bool]:
    """The two identities of docs/design/03 section 6 for one fully linked root."""
    root = res.nodes["A_ROOT"].get(root_id)
    if root is None:
        return False, False
    fill_sum = sum(e["FillGap"] for edge in EDGES for e in res.edges[edge.name].values() if e.get("RootId") == root_id)
    route_sum = sum(e["RouteGap"] for edge in EDGES for e in res.edges[edge.name].values() if e.get("RootId") == root_id)
    street_cum = sum(c["CumQty"] for c in res.links["C_CHILD"].values() if c["RootId"] == root_id)
    street_leaves = sum(c["LeavesQty"] for c in res.links["C_CHILD"].values() if c["RootId"] == root_id)
    return root["CumQty"] - street_cum == fill_sum, root["LeavesQty"] - street_leaves == route_sum
