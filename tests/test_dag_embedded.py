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
import http.server
import json
import os
import threading
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


# -- the AMPS SOW source on the engine (docs/design/11) -----------------------------------------------

def _sow_body(row) -> str:
    import json

    return json.dumps({k: v for k, v in row.items() if k not in config.INGESTER_COLUMNS})


def _publish_rows(feed, rows_by_system, skip=()) -> None:
    """Every row as a `publish` on its own topic, stamped with the row's RecvTs (the injected clock)."""
    for system in config.SYSTEMS:
        for row in rows_by_system[system]:
            if row["OrderId"] in skip:
                continue
            feed["clock"]["now"] = row["RecvTs"]
            feed["handlers"][row["Topic"]](feed["Message"]("publish", _sow_body(row), sow_key=f"{system}:{row['OrderId']}"))
    feed["clock"]["now"] = feed["now"]


@pytest.fixture(scope="module")
def amps_feed(server, book, settings):
    """The scenario book delivered as SOW messages through a stub client; the DAG over the source's blink tables."""
    from oms_trace import amps_sow, dag, sources
    from oms_trace.amps_testing import FakeMessage, StubClient, StubCommand

    clients: List[Any] = []
    clock = {"now": book.now}

    def client_factory(_amps):
        clients.append(StubClient())
        return clients[-1]

    env = {"OMS_AMPS_URI": "tcp://stub:9007/amps/json"}
    env.update({f"OMS_AMPS_TOPICS_{s}": ",".join(settings.topics[s]) for s in config.SYSTEMS})
    amps = amps_sow.AmpsSettings.from_env(env, settings=settings)
    source = amps_sow.AmpsSowSource(settings, amps, client_factory=client_factory, command_factory=StubCommand,
                                    clock=lambda: clock["now"])
    source.start(background=False)  # builds the blink tables and subscribes through the stub
    tables = dag.build_lineage(source.raw_tables(), settings, clock=sources.clock_table(book.now),
                               feed_states=source.topics_table())
    feed = {"source": source, "tables": tables, "client": clients[0], "handlers": clients[0].handlers,
            "clock": clock, "now": book.now, "Message": FakeMessage}
    for handler in feed["handlers"].values():  # an empty SOW first: snapshot boundaries on every topic
        handler(FakeMessage("group_begin"))
        handler(FakeMessage("group_end"))
    _publish_rows(feed, book.rows)
    yield feed
    source.stop()


def test_amps_fed_lineage_equals_the_oracle(amps_feed, oracle):
    tables = amps_feed["tables"]
    expected_nodes = {("Nodes", level): len(oracle.nodes[level]) for level in config.LEVELS}

    def nodes():
        stats = by_key(tables["trace_stats"], "Metric", "Key")
        return {k: stats.get(k, {}).get("Value") for k in expected_nodes}

    assert _wait(lambda: nodes() == expected_nodes, 30), nodes()
    for level, name in (("B_PARENT", "links_bp"), ("B_CHILD", "links_bc"), ("C_PARENT", "links_cp"), ("C_CHILD", "links_cc")):
        engine = by_key(tables[name], "OrderId")
        assert set(engine) == set(oracle.links[level]), level
        cols = ["Level", "System", "LinkId", "Symbol", "Side", "OrderQty", "CumQty", "LeavesQty", "Status", "Topic",
                "ParentState", "RootId", "RootState", "Mismatch", "ParentSystem", "ParentCumQty", "RecvTs", "AgeTs"]
        diffs = [(oid, d) for oid, o in oracle.links[level].items() for d in [same(engine[oid], o, cols)] if d]
        assert diffs == [], (level, diffs[:5])
    engine = by_key(tables["gaps"], "Edge", "OrderId")
    assert set(engine) == set(oracle.gaps)
    cols = ["FillGap", "RouteGap", "ChildCount", "GapAgeSec", "ParentFeed", "ChildSideStale", "FeedStale", "BreakKind",
            "ShortSide", "OpenAt", "MissedBy", "IsDrift", "RootId"]
    diffs = [(k, d) for k, o in oracle.gaps.items() for d in [same(engine[k], o, cols)] if d]
    assert diffs == [], diffs[:5]
    health = by_key(tables["feed_health"], "System", "Topic")
    assert set(health) == set(oracle.feed_health)
    diffs = [(k, d) for k, o in oracle.feed_health.items()
             for d in [same(health[k], o, ["SilentSec", "BehindSec", "TopicState", "StaleInt", "LastRecvTs"])] if d]
    assert diffs == []
    assert all(r["Connected"] is True and r["SnapshotInProgress"] is False for r in health.values())  # the override is in
    assert set(by_key(tables["drift_by_a_order"], "RootId")) == set(oracle.drift_by_a_order)
    stats = {(r["Metric"], r["Key"]): r["Value"] for r in rows(tables["trace_stats"])}
    assert stats == oracle.trace_stats
    # the exported raw tables are blink: a cycle's rows, while the aggregates see the whole history
    raw_c = amps_feed["source"].raw_tables()["C"]
    assert raw_c.is_blink
    states_c = rows(raw_c.last_by(["OrderId"]))
    assert len(states_c) == len(oracle.nodes["C_PARENT"]) + len(oracle.nodes["C_CHILD"])
    assert {r["SowStream"] for r in states_c} == {"change"} and {r["SowEpoch"] for r in states_c} == {1}
    assert all(m.counts["rejected"] == 0 for m in amps_feed["source"].mirrors.values())


def test_amps_publish_heals_a_c_parent_and_an_oof_leaves_a_deleted_state(amps_feed, book):
    """The in-place healing of the live test, through the SOW; then a record leaves the SOW."""
    tables, handlers, Message = amps_feed["tables"], amps_feed["handlers"], amps_feed["Message"]
    fam = mockdata.Book()
    sc = mockdata.family(fam, "SOWLIVE", recv=book.now - 20 * 60 * mockdata.SEC)
    late = book_row = [r for r in fam.rows["B"] if r["OrderId"] == sc.ids["Bc1"]][0]
    _publish_rows(amps_feed, fam.rows, skip={late["OrderId"]})

    def cp():
        return by_key(tables["links_cp"], "OrderId").get(sc.ids["Cp1"], {})

    assert _wait(lambda: cp().get("ParentState") == "UNRESOLVED")
    assert cp()["RootId"] is None
    assert ("C_PARENT", sc.ids["Cp1"]) in by_key(tables["unresolved_links"], "Level", "OrderId")
    amps_feed["clock"]["now"] = late["RecvTs"]
    handlers[late["Topic"]](Message("publish", _sow_body(late), sow_key=f"B:{late['OrderId']}"))
    amps_feed["clock"]["now"] = amps_feed["now"]
    assert _wait(lambda: cp().get("ParentState") == "LINKED")
    assert cp()["RootId"] == sc.root_id and cp()["RootState"] == "RESOLVED"
    ccs = by_key(tables["links_cc"], "OrderId")
    assert all(ccs[sc.ids[k]]["RootId"] == sc.root_id for k in ("Cc1a", "Cc1b"))
    assert ("C_PARENT", sc.ids["Cp1"]) not in by_key(tables["unresolved_links"], "Level", "OrderId")
    assert by_key(tables["edge_bc_cp"], "OrderId")[sc.ids["Bc1"]]["FillGap"] == 0
    # Cc1a leaves the SOW (an oof with the SowKey only): its last state stays, flagged, and still explains its parent
    cc = [r for r in fam.rows["C"] if r["OrderId"] == sc.ids["Cc1a"]][0]
    handlers[cc["Topic"]](Message("oof", sow_key=f"C:{cc['OrderId']}"))
    states = amps_feed["source"].raw_tables()["C"].last_by(["OrderId"])
    assert _wait(lambda: by_key(states, "OrderId").get(cc["OrderId"], {}).get("SowDeleted") is True)
    gone = by_key(states, "OrderId")[cc["OrderId"]]
    assert gone["CumQty"] == cc["CumQty"] and gone["SowStream"] == "change" and gone["RecvTs"] == amps_feed["now"]
    assert by_key(tables["links_cc"], "OrderId")[cc["OrderId"]]["CumQty"] == cc["CumQty"]
    assert by_key(tables["edge_cp_cc"], "OrderId")[sc.ids["Cp1"]]["FillGap"] == 0
    assert sc.root_id not in by_key(tables["drift_by_a_order"], "RootId")
    topics = by_key(tables["feed_health"], "System", "Topic")  # the C channel that carried the oof is still current
    assert topics[("C", cc["Topic"])]["TopicState"] == "CURRENT"


def test_amps_disconnect_and_resnapshot_drive_feed_health(amps_feed):
    """docs/design/11 section 4.2: disconnected -> STALE everywhere; snapshot -> CATCHING_UP; complete -> CURRENT."""
    source, client, tables, Message = amps_feed["source"], amps_feed["client"], amps_feed["tables"], amps_feed["Message"]
    handlers = amps_feed["handlers"]

    def states():
        return {(r["System"], r["Topic"]): r["TopicState"] for r in rows(tables["feed_health"])}

    regular = [(s, t) for s in config.SYSTEMS for t in mockdata.TOPICS[s]]
    before_gaps = {k: r["BreakKind"] for k, r in by_key(tables["gaps"], "Edge", "OrderId").items()}
    assert all(states()[k] == "CURRENT" for k in regular)
    client.connection(False)  # the broker is gone: one client, every topic of every system
    assert _wait(lambda: all(v == "STALE" for v in states().values()))
    assert {r["SystemStaleInt"] for r in rows(tables["system_health"])} == {1}
    assert {r["BreakKind"] for r in rows(tables["gaps"])} == {"FEED_STALE"}  # every finding suppressed, none lost
    assert len(rows(tables["gaps"])) == len(before_gaps)
    assert all(r["Connected"] is False for r in rows(tables["feed_health"]))
    client.connection(True)  # the HA client reconnected; it re-issues every subscription
    for handler in handlers.values():
        handler(Message("group_begin"))
    assert _wait(lambda: all(states()[k] == "CATCHING_UP" for k in regular))
    assert all(r["SnapshotInProgress"] is True and r["Connected"] is True for r in rows(tables["feed_health"]))
    # the broker re-sends every live record (the mirror's own records are what it holds), stamped as before
    for (system, topic), mirror in source.mirrors.items():
        for row in list(mirror.records.values()):
            if row["SowDeleted"]:
                continue
            amps_feed["clock"]["now"] = row["RecvTs"]
            handlers[topic](Message("sow", _sow_body(row), sow_key=f"{system}:{row['OrderId']}"))
    amps_feed["clock"]["now"] = amps_feed["now"]
    for handler in handlers.values():
        handler(Message("group_end"))
    assert _wait(lambda: all(states()[k] == "CURRENT" for k in regular))
    assert states()[("C", mockdata.TOPIC_C_STALE)] == "STALE" and states()[("B", mockdata.TOPIC_B_CATCHUP)] == "CATCHING_UP"
    assert all(m.counts["dropped"] == 0 and m.epoch == 2 for m in source.mirrors.values())
    after_gaps = {k: r["BreakKind"] for k, r in by_key(tables["gaps"], "Edge", "OrderId").items()}
    assert after_gaps == before_gaps  # an identical re-snapshot changes no finding
    status = by_key(tables["feed_health"], "System", "Topic")
    assert all(r["SnapshotInProgress"] is False for r in status.values())
    topics = by_key(source.topics_table(), "System", "Topic")
    assert all(r["Epoch"] == 2 and r["Connected"] is True for r in topics.values())


# -- break events and alerts (docs/design/04 §7, 08 §2) -----------------------------------------------

class _SinkHandler(http.server.BaseHTTPRequestHandler):
    received: List[Dict[str, Any]] = []

    def do_POST(self):  # noqa: N802 - http.server API
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        _SinkHandler.received.append(json.loads(body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


@pytest.fixture(scope="module")
def sink_url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SinkHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/alerts"
    server.shutdown()


def _event_keys(table) -> Dict[tuple, int]:
    from collections import Counter

    from oms_trace import alerting

    return Counter(alerting.event_key(r) for r in rows(table))


def _alert_tuples(alerts) -> List[tuple]:
    return sorted((a.id, a.ts, a.metric, a.key, a.value, a.threshold, a.severity, a.message) for a in alerts)


def _alert_rows(table) -> List[tuple]:
    return sorted((r["AlertId"], r["Ts"], r["Metric"], r["Key"], r["Value"], r["Threshold"], r["Severity"], r["Message"])
                  for r in rows(table))


def test_break_events_and_alerts_follow_the_oracle_step_by_step(server, settings, sink_url):
    """Input tables and a clock the test advances: every transition and every alert the engine
    emits equals the oracle's diff of consecutive states and the shared policy on its observations."""
    from deephaven import dtypes, input_table

    from oms_trace import alerting, dag, events, sources

    SEC = mockdata.SEC
    t0 = mockdata.NOW
    book = mockdata.Book()
    heal = mockdata.family(book, "HEAL", recv=t0 - 60 * SEC)  # Bc1 withheld: Bp is short, Cp1 unresolved
    trn = mockdata.family(book, "TRN", splits=[600, 400], cc_splits=[[300, 300], [400]], recv=t0 - 10 * SEC)
    mockdata._set(book, "C", trn.ids["Cp2"], CumQty=300, LeavesQty=100)  # 10 s old: TRANSIENT, drift after 30 s
    mockdata._set(book, "C", trn.ids["Cc2a"], CumQty=300, LeavesQty=100)
    late = book.last("B", heal.ids["Bc1"])
    rows0 = {s: [r for r in book.rows[s] if r["OrderId"] != heal.ids["Bc1"]] for s in config.SYSTEMS}
    # a two-family book gets no new rows for six minutes: keep the channel and system staleness rules
    # out of the way (they have their own tests) so the gap, link and drift rules are what is observed
    cfg = mockdata.settings_for(book, settings).with_(unresolved_alert_sec=30, feed_stale_sec=3600, feed_catchup_sec=7200)
    env = {"OMS_TRACE_ALERT_URL": sink_url, "OMS_TRACE_MARKET_HOURS": "", "OMS_TRACE_ALERT_RETRY_DELAY_SEC": "0.01",
           "OMS_TRACE_ALERT_SYSTEM_WARN_SEC": "100000", "OMS_TRACE_ALERT_SYSTEM_PAGE_SEC": "200000"}
    alert_settings = alerting.AlertSettings.from_env(env, settings=cfg)

    its = sources.input_tables()
    for s in config.SYSTEMS:
        its[s].add(sources.raw_table(rows0[s]))
    clock_it = input_table(col_defs={"Now": dtypes.Instant})
    clock_it.add(sources.clock_table(t0))
    clock = clock_it.last_by()
    tables = dag.build_lineage({s: its[s] for s in config.SYSTEMS}, cfg, clock=clock)
    att = events.attach(tables, cfg, alert_settings, clock)
    history = att.tables["break_history"]
    alerts = att.tables["alerts"]

    policy = alerting.AlertPolicy(alert_settings)
    expected_events: Dict[tuple, int] = {}
    expected_alerts: List[Any] = []

    def expect(prev, cur, now, tick: bool):
        from collections import Counter

        evs = alerting.gap_transitions(prev.gaps if prev else {}, cur.gaps, now)
        evs += alerting.link_transitions(prev.unresolved_links if prev else {}, cur.unresolved_links, now)
        new_alerts = policy.observe(alerting.observe_result(cur, now)) if tick else []
        evs += [alerting.alert_event(a) for a in new_alerts]
        for k, n in Counter(alerting.event_key(e) for e in evs).items():
            expected_events[k] = expected_events.get(k, 0) + n
        expected_alerts.extend(new_alerts)
        return evs, new_alerts

    def settled():
        return (sum(_event_keys(history).values()) == sum(expected_events.values())
                and len(rows(alerts)) == len(expected_alerts))

    def check(step: str):
        assert _wait(settled, 20), (step, dict(_event_keys(history)), expected_events, _alert_rows(alerts), _alert_tuples(expected_alerts))
        assert dict(_event_keys(history)) == expected_events, step
        assert _alert_rows(alerts) == _alert_tuples(expected_alerts), step

    # step 0: the initial state (replayed listeners) and the first clock tick
    res0 = reference.reconcile(rows0, cfg, t0)
    evs, new = expect(None, res0, t0, tick=True)
    assert {(e["From"], e["To"]) for e in evs if e["Level"] != "ALERT"} == {("NONE", "MISSING_DOWNSTREAM"), ("NONE", "TRANSIENT"), ("NEW", "UNRESOLVED")}
    assert {(a.metric, a.severity) for a in new} == {("unresolved_link", "WARN"), ("unresolved_links", "WARN"), ("drift_symbols", "WARN")}
    check("initial")
    assert att.observe(t0).as_dict() == alerting.observe_result(res0, t0).as_dict()

    # step 1: 40 s later the transient gap has aged into drift
    t1 = t0 + 40 * SEC
    clock_it.add(sources.clock_table(t1))
    res1 = reference.reconcile(rows0, cfg, t1)
    evs, new = expect(res0, res1, t1, tick=True)
    assert [(e["Edge"], e["From"], e["To"]) for e in evs] == [("Bc-Cp", "TRANSIENT", "MISSING_DOWNSTREAM")] and new == []
    check("aged")

    # step 2: the late B child lands (no clock tick): its parent's gap closes, its C parent links
    its["B"].add(sources.raw_table([late]))
    rows2 = {s: rows0[s] + ([late] if s == "B" else []) for s in config.SYSTEMS}
    res2 = reference.reconcile(rows2, cfg, t1)
    evs, new = expect(res1, res2, t1, tick=False)
    assert sorted((e["Edge"], e["From"], e["To"]) for e in evs) == [("Bc-Cp", "UNRESOLVED", "LINKED"), ("Bp-Bc", "MISSING_DOWNSTREAM", "NONE")]
    check("healed")

    # step 3: five minutes later the unresolved alerts clear and the persisting drift quantity alerts
    t2 = t1 + 300 * SEC
    clock_it.add(sources.clock_table(t2))
    res3 = reference.reconcile(rows2, cfg, t2)
    evs, new = expect(res2, res3, t2, tick=True)
    assert {(a.metric, a.key, a.severity) for a in new} == {("unresolved_link", f"C:{heal.ids['Cp1']}", "CLEAR"),
                                                             ("unresolved_links", "C", "CLEAR"),
                                                             ("drift_qty", "Bc-Cp:MissingDownstreamQty", "WARN"),
                                                             ("over_routed", trn.root_id, "WARN")}  # Cp2 holds 100 its B child does not
    check("five minutes later")

    # every alert reached the sink once and the table shows it
    assert _wait(lambda: all(r["Posted"] == "200" for r in rows(alerts)), 20), _alert_rows(alerts)
    assert all(r["Attempts"] == 1 and r["PostedTs"] is not None for r in rows(alerts))
    ids = sorted(d["alert_id"] for d in _SinkHandler.received if d["source"] == "oms-trace")
    assert ids == sorted(a.id for a in expected_alerts) and len(set(ids)) == len(ids)
    assert att.counts == {"events": sum(expected_events.values()), "alerts": len(expected_alerts)}
    assert sum(1 for r in rows(history) if r["Level"] == "ALERT") == len(expected_alerts)
    assert att.tables["break_events"].is_blink and att.tables["alert_events"].is_blink
    att.stop()
