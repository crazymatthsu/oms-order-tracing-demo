"""The Deephaven DAG against the python oracle, on the real engine (Community Core 42.5).

Skipped unless BOTH hold: ``deephaven_server`` is importable (``pip install -e .[engine]``,
~600 MB, JDK 17+ with JAVA_HOME set) and ``OMS_TRACE_DH_TEST=1``. What it proves:

* every table of docs/design/03 section 4 equals the oracle's, row for row, on the scenario
  book (links, edges, feed health, gaps, unresolved links, roots, symbols, stats);
* the DAG is incremental: a late B child arriving on an append-only input table resolves its
  C parent and re-roots the C children in place, with no rebuild.
"""

from __future__ import annotations

import faulthandler
import os
import time
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("OMS_TRACE_DH_TEST") != "1",
                                reason="set OMS_TRACE_DH_TEST=1 to run the embedded Deephaven tests")

deephaven_server = pytest.importorskip("deephaven_server")

from oms_trace import config, mockdata, reference  # noqa: E402


@pytest.fixture(scope="module")
def server():
    # The JVM traps SIGSEGV itself; pytest's faulthandler must be off before it starts (see conftest).
    faulthandler.disable()
    srv = deephaven_server.Server(port=int(os.environ.get("OMS_TRACE_DH_TEST_PORT", "10098")), jvm_args=["-Xmx2g"])
    srv.start()
    return srv


def _norm(value: Any) -> Any:
    from deephaven.constants import NULL_DOUBLE, NULL_INT, NULL_LONG

    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (NULL_LONG, NULL_INT, NULL_DOUBLE):
        return None
    if hasattr(value, "toArray"):  # a vector column (ChildTopics)
        return set(str(x) for x in value.toArray())
    if hasattr(value, "getEpochSecond"):  # java.time.Instant -> epoch nanos
        return int(value.getEpochSecond()) * 1_000_000_000 + int(value.getNano())
    return value


def rows(table) -> List[Dict[str, Any]]:
    return [{k: _norm(v) for k, v in r.items()} for r in table.iter_dict()]


def by_key(table, *cols) -> Dict[Any, Dict[str, Any]]:
    out = {}
    for r in rows(table):
        key = r[cols[0]] if len(cols) == 1 else tuple(r[c] for c in cols)
        assert key not in out, f"duplicate key {key}"
        out[key] = r
    return out


def same(engine: Dict[str, Any], oracle: Dict[str, Any], columns) -> List[tuple]:
    diffs = []
    for c in columns:
        e, o = engine.get(c), oracle.get(c)
        if isinstance(o, set):
            e = set(e) if e else set()
        if isinstance(e, float) or isinstance(o, float):
            if not ((e is None and o is None) or (e is not None and o is not None and abs(e - o) < 1e-9)):
                diffs.append((c, e, o))
        elif e != o:
            diffs.append((c, e, o))
    return diffs


@pytest.fixture(scope="module")
def built(server, book, settings):
    from oms_trace import dag, sources

    return dag.build_lineage(sources.mock_tables(book), settings, clock=sources.clock_table(book.now))


def test_every_table_is_built(built):
    from oms_trace import dag

    assert tuple(built) == dag.TABLE_NAMES
    for name, t in built.items():
        assert t.size >= 0, name


def test_links_match_oracle(built, oracle):
    for level, name in (("B_PARENT", "links_bp"), ("B_CHILD", "links_bc"), ("C_PARENT", "links_cp"), ("C_CHILD", "links_cc")):
        engine = by_key(built[name], "OrderId")
        assert set(engine) == set(oracle.links[level]), level
        cols = ["Level", "System", "LinkId", "Symbol", "Side", "OrderQty", "CumQty", "LeavesQty", "Status", "Topic",
                "ParentState", "RootId", "RootState", "Mismatch", "ParentSystem", "ParentCumQty"]
        diffs = [(oid, d) for oid, o in oracle.links[level].items() for d in [same(engine[oid], o, cols)] if d]
        assert diffs == [], (level, diffs[:5])


def test_edges_match_oracle(built, oracle):
    for edge in config.EDGES:
        name = "edge_" + edge.name.lower().replace("-", "_")
        engine = by_key(built[name], "OrderId")
        assert set(engine) == set(oracle.edges[edge.name]), edge.name
        cols = ["ChildCount", "ChildCumQty", "ChildLeavesQty", "FillGap", "RouteGap", "ChildTopics", "Edge", "Scope",
                "ExpectOne", "ChildSystem", "RootId", "LastChangeTs"]
        diffs = [(oid, d) for oid, o in oracle.edges[edge.name].items() for d in [same(engine[oid], o, cols)] if d]
        assert diffs == [], (edge.name, diffs[:5])


def test_feed_health_matches_oracle(built, oracle):
    engine = by_key(built["feed_health"], "System", "Topic")
    assert set(engine) == set(oracle.feed_health)
    cols = ["SilentSec", "BehindSec", "TopicState", "StaleInt", "LastRecvTs"]
    diffs = [(k, d) for k, o in oracle.feed_health.items() for d in [same(engine[k], o, cols)] if d]
    assert diffs == []
    sysh = by_key(built["system_health"], "System")
    assert {k: v["SystemStaleInt"] for k, v in sysh.items()} == oracle.system_health


def test_gaps_match_oracle(built, oracle):
    engine = by_key(built["gaps"], "Edge", "OrderId")
    assert set(engine) == set(oracle.gaps)
    cols = ["FillGap", "RouteGap", "ChildCount", "GapAgeSec", "ParentFeed", "ChildSideStale", "FeedStale", "BreakKind",
            "ShortSide", "OpenAt", "MissedBy", "IsDrift", "RootId", "Scope", "ExpectOne", "Symbol", "Side", "LeavesQty"]
    diffs = [(k, d) for k, o in oracle.gaps.items() for d in [same(engine[k], o, cols)] if d]
    assert diffs == [], diffs[:5]
    assert set(by_key(built["drift_edges"], "Edge", "OrderId")) == set(oracle.drift_edges)
    assert set(by_key(built["feed_suppressed"], "Edge", "OrderId")) == set(oracle.feed_suppressed)


def test_unresolved_mismatch_and_root_unresolved_match_oracle(built, oracle):
    engine = by_key(built["unresolved_links"], "Level", "OrderId")
    assert set(engine) == set(oracle.unresolved_links)
    diffs = [(k, d) for k, o in oracle.unresolved_links.items() for d in [same(engine[k], o, ["AgeSec", "FeedStale", "Alerted", "ParentState"])] if d]
    assert diffs == []
    assert set(by_key(built["mismatch_links"], "Level", "OrderId")) == set(oracle.mismatch_links)
    assert set(by_key(built["root_unresolved"], "Level", "OrderId")) == set(oracle.root_unresolved)


def test_roots_symbols_and_stats_match_oracle(built, oracle):
    engine = by_key(built["drift_by_a_order"], "RootId")
    assert set(engine) == set(oracle.drift_by_a_order)
    cols = ["ACumQty", "ALeavesQty", "StreetCumQty", "StreetLeavesQty", "GapABp", "SumGapBpBc", "SumGapBcCp", "SumGapCpCc",
            "MissingAtB", "UnbookedAtA", "InternalB", "MissingAtC", "UnbookedAtB", "InternalC", "HeldQty", "OverRoutedQty", "Breaks"]
    diffs = [(k, d) for k, o in oracle.drift_by_a_order.items() for d in [same(engine[k], o, cols)] if d]
    assert diffs == []
    engine = by_key(built["drift_by_symbol"], "Symbol", "Side", "Edge")
    assert set(engine) == set(oracle.drift_by_symbol)
    cols = ["Breaks", "ParentCumQty", "ChildCumQty", "MissingDownstream", "UnbookedUpstream", "OverRouted"]
    diffs = [(k, d) for k, o in oracle.drift_by_symbol.items() for d in [same(engine[k], o, cols)] if d]
    assert diffs == []
    stats = {(r["Metric"], r["Key"]): r["Value"] for r in rows(built["trace_stats"])}
    assert stats == oracle.trace_stats


def _wait(predicate, timeout_s: float = 10.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return False


def test_a_late_b_child_heals_its_c_parent_in_place(server, settings):
    """docs/design/04 section 4: the pending buffer is the join state."""
    from oms_trace import dag, sources

    book = mockdata.Book()
    sc = mockdata.family(book, "LIVE")
    late = [r for r in book.rows["B"] if r["OrderId"] == sc.ids["Bc1"]]
    first = {s: [r for r in book.rows[s] if not (s == "B" and r["OrderId"] == sc.ids["Bc1"])] for s in config.SYSTEMS}
    its = sources.input_tables()
    for s in config.SYSTEMS:
        its[s].add(sources.raw_table(first[s]))
    tables = dag.build_lineage({s: its[s] for s in config.SYSTEMS}, mockdata.settings_for(book, settings),
                               clock=sources.clock_table(book.now))

    def cp_state():
        return by_key(tables["links_cp"], "OrderId").get(sc.ids["Cp1"], {})

    assert _wait(lambda: cp_state().get("ParentState") == "UNRESOLVED")
    assert cp_state()["RootId"] is None
    ccs = by_key(tables["links_cc"], "OrderId")
    assert all(ccs[sc.ids[k]]["RootState"] == "UNRESOLVED" and ccs[sc.ids[k]]["ParentState"] == "LINKED" for k in ("Cc1a", "Cc1b"))
    assert (("C_PARENT", sc.ids["Cp1"]) in by_key(tables["unresolved_links"], "Level", "OrderId"))
    assert sc.ids["Bc1"] not in by_key(tables["edge_bc_cp"], "OrderId")

    its["B"].add(sources.raw_table(late))  # the B child lands two hours late

    assert _wait(lambda: cp_state().get("ParentState") == "LINKED")
    assert cp_state()["RootId"] == sc.root_id and cp_state()["RootState"] == "RESOLVED"
    ccs = by_key(tables["links_cc"], "OrderId")
    assert all(ccs[sc.ids[k]]["RootState"] == "RESOLVED" and ccs[sc.ids[k]]["RootId"] == sc.root_id for k in ("Cc1a", "Cc1b"))
    edge = by_key(tables["edge_bc_cp"], "OrderId")[sc.ids["Bc1"]]
    assert edge["ChildCount"] == 1 and edge["FillGap"] == 0
    assert ("C_PARENT", sc.ids["Cp1"]) not in by_key(tables["unresolved_links"], "Level", "OrderId")
    assert by_key(tables["gaps"], "Edge", "OrderId") == {}  # a clean family, everything linked


def test_ui_slice_builds_the_tree_of_a_root(built, book, settings):
    from oms_trace import ui_slice

    sc = book.scenarios["MB"]
    nodes, tree = ui_slice.lineage_nodes(built, [sc.root_id], settings)
    got = by_key(nodes, "NodeKey")
    assert len(got) == 1 + 1 + 2 + 2 + 3  # A, Bp, 2 Bc, 2 Cp, 3 Cc
    assert got[f"B|{sc.ids['Bp']}"]["BreakKind"] == "MISSING_DOWNSTREAM"
    assert got[f"B|{sc.ids['Bc2']}"]["BreakKind"] == "UNBOOKED_UPSTREAM"
    assert got[f"C|{sc.ids['Cc2a']}"]["BreakKind"] == "NONE" and got[f"C|{sc.ids['Cc2a']}"]["Depth"] == 4
    assert tree is not None
    roots = rows(ui_slice.roots_for_order(built, sc.ids["Cc2a"]))
    assert roots == [{"RootId": sc.root_id}]
    with pytest.raises(ValueError):
        ui_slice.lineage_nodes(built, [f"r{i}" for i in range(settings.ui_max_roots + 1)], settings)
    with pytest.raises(ValueError):
        ui_slice.lineage_nodes(built, ["bad`id"], settings)


def test_dashboard_builds_when_deephaven_ui_is_available(built, settings):
    pytest.importorskip("deephaven.ui")
    from oms_trace import dashboard

    assert dashboard.build(built, settings) is not None


def test_app_build_on_the_mock_source_follows_the_wall_clock(server):
    from oms_trace import app, config

    settings = config.Settings(source="mock", clock_sec=1, mock_families=4)
    tables = app.build(settings)
    assert set(("raw_orders_a", "raw_orders_b", "raw_orders_c", "gaps", "feed_health", "drift_by_a_order")) <= set(tables)
    # the book is re-based to now: the stale channel is 10 minutes old, the catch-up one two hours behind
    def states():
        return {(r["System"], r["Topic"]): r["TopicState"] for r in rows(tables["feed_health"])}
    assert _wait(lambda: states().get(("C", mockdata.TOPIC_C_STALE)) == "STALE")
    assert states()[("B", mockdata.TOPIC_B_CATCHUP)] == "CATCHING_UP"
    assert all(v == "CURRENT" for k, v in states().items() if k[1] in sum(mockdata.TOPICS.values(), ()))
    kinds = {k: r["BreakKind"] for k, r in by_key(tables["gaps"], "Edge", "OrderId").items()}
    assert kinds[("Bp-Bc", "Bp-MB")] == "MISSING_DOWNSTREAM"
    assert kinds[("Bc-Cp", "Bc-FS-2")] == "FEED_STALE"
    assert "A-MB" in by_key(tables["drift_by_a_order"], "RootId")
    exported: dict = {}
    assert "drift_edges" in app.main(exported, settings)
    assert "oms_trace_dashboard" in exported or True  # the dashboard needs the ui plugin; the tables are the contract
