"""The lineage and drift DAG (docs/design/04), on the Community Core engine.

``build_lineage`` takes one raw table per system (already merged over that system's topic
tables, see :mod:`oms_trace.sources` / :mod:`oms_trace.enterprise`), the settings and a clock,
and returns every table of docs/design/03 section 4. Everything is a join, a filter or an
aggregation: when one state ticks, only that node, its parent's rollup, that edge row and the
affected aggregates recompute. The python twin of every formula is :mod:`oms_trace.reference`.
"""

from __future__ import annotations

from typing import Dict, Mapping, Optional

from deephaven import agg, merge, new_table, time_table
from deephaven.column import string_col
from deephaven.table import Table

from oms_trace import config
from oms_trace.config import EDGES, LEVEL_LINK_COLUMN, LEVEL_SYSTEM, PARENT_SYSTEM, Settings

#: Every table ``build_lineage`` returns, in dependency order.
TABLE_NAMES = (
    "a_root", "b_parent", "b_child", "c_parent", "c_child",
    "links_bp", "links_bc", "links_cp", "links_cc",
    "edge_a_bp", "edge_bp_bc", "edge_bc_cp", "edge_cp_cc",
    "feed_health", "system_health",
    "gaps", "drift_edges", "feed_suppressed",
    "unresolved_links", "mismatch_links", "root_unresolved",
    "drift_by_a_order", "drift_by_symbol", "trace_stats",
)

LINK_COLUMNS = list(config.CANONICAL_COLUMNS) + [
    "ParentPresent", "ParentCumQty", "ParentSymbol", "ParentSide", "ParentStatus", "ParentRecvTs", "ParentRootId",
    "ParentSystem", "RootId", "ParentState", "RootState", "UnresolvedSince", "Mismatch",
]
EDGE_COLUMNS = LINK_COLUMNS + [
    "ChildCount", "ChildCumQty", "ChildLeavesQty", "ChildLastTs", "ChildTopics", "FillGap", "RouteGap", "LastChangeTs",
    "Edge", "Scope", "ExpectOne", "ChildSystem",
]
DRIFT_EDGE_COLUMNS = [
    "Edge", "Scope", "Level", "System", "OrderId", "RootId", "RootState", "Symbol", "Side", "Status", "OrderQty", "CumQty",
    "ChildCount", "ChildCumQty", "FillGap", "LeavesQty", "ChildLeavesQty", "RouteGap", "GapAgeSec", "BreakKind",
    "ShortSide", "MissedBy", "OpenAt", "ParentFeed", "ChildSideStale", "FeedStale", "IsDrift", "Topic", "ChildTopics",
]

_PARENT_COLS = [
    "ParentPresent = Present",
    "ParentCumQty = CumQty",
    "ParentSymbol = Symbol",
    "ParentSide = Side",
    "ParentStatus = Status",
    "ParentRecvTs = RecvTs",
    "ParentRootId = RootId",
]
_STATE_COLS = [
    "ParentState = LinkId == `` ? `NO_LINK` : (isNull(ParentPresent) ? `UNRESOLVED` : `LINKED`)",
    "RootState = isNull(RootId) ? `UNRESOLVED` : `RESOLVED`",
    "UnresolvedSince = ParentState == `UNRESOLVED` ? RecvTs : null",
    "Mismatch = !isNull(ParentPresent) && (Symbol != ParentSymbol || Side != ParentSide)",
]


def live_clock(settings: Settings) -> Table:
    """The production clock: one row, re-ticking every ``clock_sec`` seconds.

    A static first row makes ``Now`` available immediately; ``time_table`` emits its first row
    only after one period, and a null ``Now`` would read every gap as TRANSIENT until then.
    """
    import time

    from deephaven.column import datetime_col
    from deephaven.time import to_j_instant

    first = new_table([datetime_col("Now", [to_j_instant(time.time_ns())])])
    ticking = time_table(f"PT{settings.clock_sec}S").view(["Now = Timestamp"])
    return merge([first, ticking]).last_by()


def _dates_filter(settings: Settings) -> str:
    return "Date in " + ", ".join(f"`{d}`" for d in settings.dates())


def canonical_formulas(level: str, settings: Settings) -> list:
    """The adapter view of docs/design/03 section 3 for one level."""
    link_col = LEVEL_LINK_COLUMN.get(level)
    link = "``" if link_col is None else f"isNull({link_col}) ? `` : {link_col}"
    age = "isNull(EventTs) ? RecvTs : EventTs" if settings.age_clock == "event" else "RecvTs"
    return [
        f"Level = `{level}`",
        f"System = `{LEVEL_SYSTEM[level]}`",
        "OrderId",
        f"LinkId = {link}",
        "Symbol = isNull(Symbol) ? `` : Symbol",
        "Side = isNull(Side) ? `` : Side",
        "OrderQty = isNull(OrderQty) ? 0L : OrderQty",
        "CumQty = isNull(CumQty) ? 0L : CumQty",
        "LeavesQty = isNull(LeavesQty) ? 0L : LeavesQty",
        "Status = isNull(OrdStatus) ? `` : OrdStatus",
        "RecvTs",
        "Topic = isNull(Topic) ? `` : Topic",
        f"AgeTs = {age}",
        "Present = true",
    ]


def _link(children: Table, parents: Table, level: str, root_formula: str) -> Table:
    return (
        children.natural_join(parents, on=["LinkId=OrderId"], joins=_PARENT_COLS)
        .update_view([root_formula, f"ParentSystem = `{PARENT_SYSTEM[level]}`"] + _STATE_COLS)
        .view(LINK_COLUMNS)
    )


def _children_rollup(children: Table) -> Table:
    return children.where("LinkId != ``").agg_by(
        [
            agg.count_("ChildCount"),
            agg.sum_(["ChildCumQty = CumQty", "ChildLeavesQty = LeavesQty"]),
            agg.max_(["ChildLastTs = AgeTs"]),
            agg.distinct(["ChildTopics = Topic"]),
        ],
        by=["LinkId"],
    )


def _edge(parents: Table, children: Table, edge: config.Edge, settings: Settings) -> Table:
    expect_one = "true" if settings.edge_expects_one(edge) else "false"
    return (
        parents.natural_join(_children_rollup(children), on=["OrderId=LinkId"],
                             joins=["ChildCount", "ChildCumQty", "ChildLeavesQty", "ChildLastTs", "ChildTopics"])
        .update_view([
            "ChildCount = isNull(ChildCount) ? 0L : ChildCount",
            "ChildCumQty = isNull(ChildCumQty) ? 0L : ChildCumQty",
            "ChildLeavesQty = isNull(ChildLeavesQty) ? 0L : ChildLeavesQty",
            "FillGap = CumQty - ChildCumQty",
            "RouteGap = LeavesQty - ChildLeavesQty",
            "LastChangeTs = isNull(ChildLastTs) || epochNanos(AgeTs) > epochNanos(ChildLastTs) ? AgeTs : ChildLastTs",
            f"Edge = `{edge.name}`",
            f"Scope = `{edge.scope}`",
            f"ExpectOne = {expect_one}",
            f"ChildSystem = `{edge.child_system}`",
        ])
        .view(EDGE_COLUMNS)
    )


FEED_HEALTH_COLUMNS = ["System", "Topic", "LastRecvTs", "LastEventTs", "SilentSec", "BehindSec", "Connected",
                       "SnapshotInProgress", "TopicState", "StaleInt"]


def _topic_health(raw_window: Table, system: str, clock: Table, settings: Settings,
                  feed_states: Optional[Table] = None) -> Table:
    """Per channel: silence, event-time lag and, with ``feed_states``, the source's own word (docs/design/11 §4.2).

    ``feed_states`` has one row per (System, Topic) with ``Connected`` and ``SnapshotInProgress``
    (the AMPS source's ``amps_topics``). Silence still wins; a disconnected client makes the
    topic STALE, a replaying snapshot CATCHING_UP; otherwise the event-time rule of doc 04 §5.
    """
    health = (
        raw_window.agg_by([agg.max_(["LastRecvTs = RecvTs", "LastEventTs = EventTs"])], by=["Topic"])
        .update_view([f"System = `{system}`", "Topic = isNull(Topic) ? `` : Topic"])
        .natural_join(clock, on=[], joins=["Now"])
    )
    if feed_states is None:
        health = health.update_view(["Connected = (Boolean) null", "SnapshotInProgress = (Boolean) null"])
    else:
        health = health.natural_join(feed_states, on=["System", "Topic"], joins=["Connected", "SnapshotInProgress"])
    return (
        health.update_view([
            "SilentSec = (long) (diffNanos(LastRecvTs, Now) / SECOND)",   # `/` on longs is a double in the query language
            "BehindSec = isNull(LastEventTs) ? NULL_LONG : (long) (diffNanos(LastEventTs, Now) / SECOND)",
            f"TopicState = SilentSec > {settings.feed_stale_sec} ? `STALE`"
            " : (!isNull(Connected) && !Connected) ? `STALE`"
            " : (!isNull(SnapshotInProgress) && SnapshotInProgress) ? `CATCHING_UP`"
            f" : ((!isNull(BehindSec) && BehindSec > {settings.feed_catchup_sec}) ? `CATCHING_UP` : `CURRENT`)",
            "StaleInt = TopicState == `CURRENT` ? 0 : 1",
        ])
        .view(FEED_HEALTH_COLUMNS)
    )


def _one_row(metric: str, key: str) -> Table:
    return new_table([string_col("Metric", [metric]), string_col("Key", [key])])


def _count(table: Table, metric: str, key: str) -> Table:
    # count_by on an empty table yields no row at all; the anchor keeps the zero.
    return (_one_row(metric, key).natural_join(table.count_by("N"), on=[], joins=["N"])
            .update_view(["Value = isNull(N) ? 0L : N"]).view(["Metric", "Key", "Value"]))


def _sum(table: Table, expr: str, metric: str, key: str) -> Table:
    summed = table.update_view([f"S = {expr}"]).agg_by([agg.sum_(["S"])], by=[])
    return (_one_row(metric, key).natural_join(summed, on=[], joins=["S"])
            .update_view(["Value = isNull(S) ? 0L : S"]).view(["Metric", "Key", "Value"]))


def build_lineage(raw: Mapping[str, Table], settings: Settings, clock: Optional[Table] = None,
                  feed_states: Optional[Table] = None) -> Dict[str, Table]:
    """Build every table of docs/design/03 section 4 from one raw table per system.

    Args:
        raw: ``{"A": table, "B": table, "C": table}`` with the raw contract's columns (append-only
            or blink: every aggregation here sees a blink table's whole history).
        settings: the knobs (window, thresholds, clock choice).
        clock: a one-row table with an ``Now`` Instant; ``None`` means :func:`live_clock`.
        feed_states: optional, one row per (System, Topic) with ``Connected`` and
            ``SnapshotInProgress`` (the AMPS source's ``amps_topics``) overriding feed health.
    """
    clock = live_clock(settings) if clock is None else clock
    window = {s: raw[s].where(_dates_filter(settings)) for s in config.SYSTEMS}
    pid = settings.parent_id_column
    last = {s: window[s].last_by(["OrderId"]) for s in config.SYSTEMS}

    a_root = last["A"].view(canonical_formulas("A_ROOT", settings)).update_view(
        ["RootId = OrderId", "RootState = `RESOLVED`", "ParentState = `ROOT`"])
    b_parent = last["B"].where(f"isNull({pid})").view(canonical_formulas("B_PARENT", settings))
    b_child = last["B"].where(f"!isNull({pid})").view(canonical_formulas("B_CHILD", settings))
    c_parent = last["C"].where(f"isNull({pid})").view(canonical_formulas("C_PARENT", settings))
    c_child = last["C"].where(f"!isNull({pid})").view(canonical_formulas("C_CHILD", settings))

    # links, top-down, carrying the root; the A id is known on a B parent before A arrives
    links_bp = _link(b_parent, a_root, "B_PARENT", "RootId = LinkId == `` ? (String) null : LinkId")
    links_bc = _link(b_child, links_bp, "B_CHILD", "RootId = ParentRootId")
    links_cp = _link(c_parent, links_bc, "C_PARENT", "RootId = ParentRootId")
    links_cc = _link(c_child, links_cp, "C_CHILD", "RootId = ParentRootId")

    a_root_linked = a_root.view(LINK_COLUMNS[:len(config.CANONICAL_COLUMNS)] + [
        "ParentPresent = (Boolean) null", "ParentCumQty = NULL_LONG", "ParentSymbol = (String) null",
        "ParentSide = (String) null", "ParentStatus = (String) null", "ParentRecvTs = (java.time.Instant) null",
        "ParentRootId = (String) null", "ParentSystem = ``", "RootId", "ParentState", "RootState",
        "UnresolvedSince = (java.time.Instant) null", "Mismatch = false"])
    parents_of = {"A_ROOT": a_root_linked, "B_PARENT": links_bp, "B_CHILD": links_bc, "C_PARENT": links_cp}
    children_of = {"B_PARENT": links_bp, "B_CHILD": links_bc, "C_PARENT": links_cp, "C_CHILD": links_cc}
    edges = {e.name: _edge(parents_of[e.parent_level], children_of[e.child_level], e, settings) for e in EDGES}

    # feed health per channel (Q7): a system is down only when every channel is non-current
    feed_health = merge([_topic_health(window[s], s, clock, settings, feed_states) for s in config.SYSTEMS])
    system_health = feed_health.agg_by([agg.min_(["SystemStaleInt = StaleInt"])], by=["System"])

    candidates = merge(list(edges.values())).where("FillGap != 0 || RouteGap != 0 || (ExpectOne && ChildCount != 1)")
    child_feed = (
        candidates.view(["Edge", "OrderId", "ChildSystem", "ChildTopics"]).ungroup(["ChildTopics"])
        .natural_join(feed_health, on=["ChildSystem=System", "ChildTopics=Topic"], joins=["ChildFeedInt = StaleInt"])
        .agg_by([agg.max_(["ChildFeedStaleInt = ChildFeedInt"])], by=["Edge", "OrderId"])
    )
    gaps = (
        candidates
        .natural_join(clock, on=[], joins=["Now"])
        .natural_join(feed_health, on=["System", "Topic"], joins=["ParentFeed = TopicState"])
        .natural_join(child_feed, on=["Edge", "OrderId"], joins=["ChildFeedStaleInt"])
        .natural_join(system_health, on=["ChildSystem=System"], joins=["ChildSystemStaleInt = SystemStaleInt"])
        .update_view([
            "GapAgeSec = (long) (diffNanos(LastChangeTs, Now) / SECOND)",
            "ChildSideStale = ChildCount == 0 ? (!isNull(ChildSystemStaleInt) && ChildSystemStaleInt > 0)"
            " : (!isNull(ChildFeedStaleInt) && ChildFeedStaleInt > 0)",
            "FeedStale = (!isNull(ParentFeed) && ParentFeed != `CURRENT`) || ChildSideStale",
            "BreakKind = FeedStale ? `FEED_STALE`"
            " : CumQty > OrderQty ? `OVERFILL`"
            f" : GapAgeSec < {settings.gap_age_sec} ? `TRANSIENT`"
            " : (ExpectOne && ChildCount > 1) ? `CARDINALITY`"
            " : (ExpectOne && ChildCount == 0 && CumQty == 0) ? `MISSING_CHILD`"
            " : FillGap > 0 ? `MISSING_DOWNSTREAM` : FillGap < 0 ? `UNBOOKED_UPSTREAM`"
            " : (ExpectOne && RouteGap != 0) ? `LEAVES_MISMATCH`"
            " : (RouteGap < 0 && LeavesQty == 0) ? `CHILD_STILL_OPEN`"
            " : RouteGap < 0 ? `OVER_ROUTED` : RouteGap > 0 ? `HELD` : `NONE`",
            "ShortSide = FillGap > 0 ? `CHILD` : `PARENT`",
            "OpenAt = RouteGap > 0 ? `PARENT` : (RouteGap < 0 ? `CHILD` : ``)",
            "MissedBy = Scope == `INTERNAL` ? System"
            " : (FillGap > 0 ? (Edge == `A-Bp` ? `B` : `C`) : (Edge == `A-Bp` ? `A` : `B`))",
            "IsDrift = BreakKind != `FEED_STALE` && BreakKind != `TRANSIENT` && BreakKind != `MISSING_CHILD`"
            " && BreakKind != `HELD` && BreakKind != `NONE`",
        ])
    )
    drift_edges = gaps.where("IsDrift").view(DRIFT_EDGE_COLUMNS)
    feed_suppressed = gaps.where("BreakKind == `FEED_STALE`").view(DRIFT_EDGE_COLUMNS)

    all_links = merge([links_bp, links_bc, links_cp, links_cc])
    unresolved_links = (
        all_links.where("ParentState != `LINKED`")
        .natural_join(clock, on=[], joins=["Now"])
        .natural_join(system_health, on=["ParentSystem=System"], joins=["ParentSystemStaleInt = SystemStaleInt"])
        .update_view([
            "AgeSec = (long) (diffNanos(AgeTs, Now) / SECOND)",
            "FeedStale = !isNull(ParentSystemStaleInt) && ParentSystemStaleInt > 0",
            f"Alerted = !FeedStale && AgeSec >= {settings.unresolved_alert_sec}",
        ])
    )
    mismatch_links = all_links.where("Mismatch")
    root_unresolved = all_links.where("ParentState == `LINKED` && RootState == `UNRESOLVED`")

    # per root: one column per edge, open quantity held/over-routed, the street totals
    def gap_sum_by_root(edge_name: str, col: str) -> Table:
        return (gaps.where(f"Edge == `{edge_name}` && IsDrift && !isNull(RootId)")
                .agg_by([agg.sum_([f"{col} = FillGap"]), agg.count_(f"{col}Breaks")], by=["RootId"]))

    street_by_root = links_cc.where("!isNull(RootId)").agg_by(
        [agg.sum_(["StreetCumQty = CumQty", "StreetLeavesQty = LeavesQty"])], by=["RootId"])
    route_by_root = (
        gaps.where("RouteGap != 0 && !isNull(RootId) && BreakKind != `TRANSIENT` && BreakKind != `FEED_STALE`"
                   " && BreakKind != `MISSING_CHILD`")
        .update_view(["Held = RouteGap > 0 ? RouteGap : 0L", "OverRouted = RouteGap < 0 ? -RouteGap : 0L"])
        .agg_by([agg.sum_(["HeldQty = Held", "OverRoutedQty = OverRouted"])], by=["RootId"])
    )
    roots = a_root.view(["RootId", "Symbol", "Side", "ACumQty = CumQty", "ALeavesQty = LeavesQty"])
    drift_by_a_order = (
        roots
        .natural_join(gap_sum_by_root("A-Bp", "GapABp"), on=["RootId"], joins=["GapABp", "GapABpBreaks"])
        .natural_join(gap_sum_by_root("Bp-Bc", "SumGapBpBc"), on=["RootId"], joins=["SumGapBpBc", "SumGapBpBcBreaks"])
        .natural_join(gap_sum_by_root("Bc-Cp", "SumGapBcCp"), on=["RootId"], joins=["SumGapBcCp", "SumGapBcCpBreaks"])
        .natural_join(gap_sum_by_root("Cp-Cc", "SumGapCpCc"), on=["RootId"], joins=["SumGapCpCc", "SumGapCpCcBreaks"])
        .natural_join(street_by_root, on=["RootId"], joins=["StreetCumQty", "StreetLeavesQty"])
        .natural_join(route_by_root, on=["RootId"], joins=["HeldQty", "OverRoutedQty"])
        .where("!isNull(GapABpBreaks) || !isNull(SumGapBpBcBreaks) || !isNull(SumGapBcCpBreaks) || !isNull(SumGapCpCcBreaks)")
        .update_view([
            "GapABp = isNull(GapABp) ? 0L : GapABp",
            "SumGapBpBc = isNull(SumGapBpBc) ? 0L : SumGapBpBc",
            "SumGapBcCp = isNull(SumGapBcCp) ? 0L : SumGapBcCp",
            "SumGapCpCc = isNull(SumGapCpCc) ? 0L : SumGapCpCc",
            "StreetCumQty = isNull(StreetCumQty) ? 0L : StreetCumQty",
            "StreetLeavesQty = isNull(StreetLeavesQty) ? 0L : StreetLeavesQty",
            "HeldQty = isNull(HeldQty) ? 0L : HeldQty",
            "OverRoutedQty = isNull(OverRoutedQty) ? 0L : OverRoutedQty",
            "MissingAtB = GapABp > 0 ? GapABp : 0L",
            "UnbookedAtA = GapABp < 0 ? -GapABp : 0L",
            "InternalB = SumGapBpBc",
            "MissingAtC = SumGapBcCp > 0 ? SumGapBcCp : 0L",
            "UnbookedAtB = SumGapBcCp < 0 ? -SumGapBcCp : 0L",
            "InternalC = SumGapCpCc",
            "Breaks = (isNull(GapABpBreaks) ? 0L : GapABpBreaks) + (isNull(SumGapBpBcBreaks) ? 0L : SumGapBpBcBreaks)"
            " + (isNull(SumGapBcCpBreaks) ? 0L : SumGapBcCpBreaks) + (isNull(SumGapCpCcBreaks) ? 0L : SumGapCpCcBreaks)",
        ])
        .view(["RootId", "Symbol", "Side", "ACumQty", "ALeavesQty", "StreetCumQty", "StreetLeavesQty",
               "GapABp", "SumGapBpBc", "SumGapBcCp", "SumGapCpCc", "MissingAtB", "UnbookedAtA", "InternalB",
               "MissingAtC", "UnbookedAtB", "InternalC", "HeldQty", "OverRoutedQty", "Breaks"])
    )
    drift_by_symbol = (
        gaps.where("IsDrift")
        .update_view(["MissingDownstream = FillGap > 0 ? FillGap : 0L",
                      "UnbookedUpstream = FillGap < 0 ? -FillGap : 0L",
                      "OverRouted = RouteGap < 0 ? -RouteGap : 0L"])
        .agg_by([agg.count_("Breaks"),
                 agg.sum_(["ParentCumQty = CumQty", "ChildCumQty", "MissingDownstream", "UnbookedUpstream", "OverRouted"])],
                by=["Symbol", "Side", "Edge"])
    )

    # stats, long format: (Metric, Key, Value)
    levels = {"A_ROOT": a_root, "B_PARENT": b_parent, "B_CHILD": b_child, "C_PARENT": c_parent, "C_CHILD": c_child}
    stats = [_count(t, "Nodes", level) for level, t in levels.items()]
    stats += [_count(children_of[level].where("ParentState != `LINKED`"), "Unresolved", level) for level in children_of]
    for e in EDGES:
        eg = gaps.where(f"Edge == `{e.name}`")
        stats.append(_count(eg, "Gaps", e.name))
        stats.append(_count(eg.where("IsDrift"), "Drift", e.name))
        stats.append(_sum(eg.where("IsDrift"), "FillGap > 0 ? FillGap : 0L", "MissingDownstreamQty", e.name))
        stats.append(_sum(eg.where("IsDrift"), "FillGap < 0 ? -FillGap : 0L", "UnbookedUpstreamQty", e.name))
    stats.append(_one_row("CParentsMinusBChildren", "")
                 .natural_join(c_parent.count_by("Cp"), on=[], joins=["Cp"])
                 .natural_join(b_child.count_by("Bc"), on=[], joins=["Bc"])
                 .update_view(["Value = (isNull(Cp) ? 0L : Cp) - (isNull(Bc) ? 0L : Bc)"]).view(["Metric", "Key", "Value"]))
    stats.append(feed_health.view(["Metric = `TopicStale`", "Key = System + `:` + Topic", "Value = (long) StaleInt"]))
    trace_stats = merge(stats)

    tables: Dict[str, Table] = {
        "a_root": a_root, "b_parent": b_parent, "b_child": b_child, "c_parent": c_parent, "c_child": c_child,
        "links_bp": links_bp, "links_bc": links_bc, "links_cp": links_cp, "links_cc": links_cc,
        "edge_a_bp": edges["A-Bp"], "edge_bp_bc": edges["Bp-Bc"], "edge_bc_cp": edges["Bc-Cp"], "edge_cp_cc": edges["Cp-Cc"],
        "feed_health": feed_health, "system_health": system_health,
        "gaps": gaps, "drift_edges": drift_edges, "feed_suppressed": feed_suppressed,
        "unresolved_links": unresolved_links, "mismatch_links": mismatch_links, "root_unresolved": root_unresolved,
        "drift_by_a_order": drift_by_a_order, "drift_by_symbol": drift_by_symbol, "trace_stats": trace_stats,
    }
    return {name: tables[name] for name in TABLE_NAMES}
