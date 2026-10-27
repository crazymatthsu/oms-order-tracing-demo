"""Transitions, the alert policy and the HTTP sink on a bare python (docs/design/03 §4.5, 08 §2)."""

from __future__ import annotations

import http.server
import json
import threading
import time
from typing import Any, Dict, List, Tuple

import pytest

from oms_trace import alerting, config, mockdata, reference
from oms_trace.alerting import Alert, AlertPolicy, AlertSettings, Observation, Reading

SEC = config.SECOND_NANOS
MIN = 60 * SEC
NOW = mockdata.NOW  # 2026-10-26T15:00Z, 11:00 New York: market hours


def settings(**kw) -> AlertSettings:
    base = AlertSettings.from_env({})
    return base.__class__(**{**base.__dict__, **kw})


def obs(now: int, **kw) -> Observation:
    return Observation(now=now, **kw)


def kinds(alerts: List[Alert]) -> List[Tuple[str, str, str]]:
    return [(a.metric, a.key, a.severity) for a in alerts]


# -- settings ------------------------------------------------------------------------------------


def test_settings_defaults_overrides_hours_and_headers():
    s = AlertSettings.from_env({})
    assert s.url == "" and s.retries == 1 and s.unresolved_alert_sec == 3600 and s.ratio_page == -100
    assert s.market_hours == "09:30-16:00" and s.tz == "America/New_York" and s.symbols_page == 10
    assert s.in_market_hours(NOW) and not s.in_market_hours(NOW - 12 * 3600 * SEC)  # 23:00 the evening before
    assert settings(market_hours="").in_market_hours(NOW - 12 * 3600 * SEC)
    s = AlertSettings.from_env({"OMS_TRACE_ALERT_URL": " http://sink/alerts ", "OMS_TRACE_ALERT_AUTH_HEADER": "Authorization: Bearer x",
                                "OMS_TRACE_ALERT_RATIO_PAGE": "50", "OMS_TRACE_UNRESOLVED_ALERT_SEC": "600",
                                "OMS_TRACE_MARKET_HOURS": "08:00-17:30", "OMS_TRACE_TZ": "Europe/London"})
    assert s.url == "http://sink/alerts" and s.headers() == {"Content-Type": "application/json", "Authorization": "Bearer x"}
    assert s.ratio_page == -50 and s.unresolved_alert_sec == 600 and s.tz == "Europe/London"
    assert s.in_market_hours(NOW)  # 15:00 London
    for bad in ({"OMS_TRACE_MARKET_HOURS": "9-17"}, {"OMS_TRACE_MARKET_HOURS": "16:00-09:30"}, {"OMS_TRACE_ALERT_RETRIES": "x"},
                {"OMS_TRACE_ALERT_AUTH_HEADER": "nocolon"}, {"OMS_TRACE_ALERT_SYMBOLS_PAGE": "0"}):
        with pytest.raises(config.SettingsError):
            AlertSettings.from_env(bad)


# -- transitions ---------------------------------------------------------------------------------


def gap(edge: str, oid: str, kind: str, fill: int = 100, age: int = 60, root: str = "A-1") -> Dict[str, Any]:
    level = {"Cli-Dest": "A_ROOT", "A-Bp": "A_DEST", "Bp-Bc": "B_PARENT", "Bc-Cp": "B_CHILD", "Cp-Cc": "C_PARENT"}[edge]
    return {"Edge": edge, "OrderId": oid, "Level": level, "System": config.LEVEL_SYSTEM[level], "RootId": root,
            "BreakKind": kind, "FillGap": fill, "GapAgeSec": age}


def test_gap_and_link_transitions_between_two_states():
    prev = {("Bp-Bc", "Bp-1"): gap("Bp-Bc", "Bp-1", "TRANSIENT", age=10), ("Cp-Cc", "Cp-1"): gap("Cp-Cc", "Cp-1", "OVER_ROUTED")}
    cur = {("Bp-Bc", "Bp-1"): gap("Bp-Bc", "Bp-1", "MISSING_DOWNSTREAM", age=45), ("A-Bp", "A-1"): gap("A-Bp", "A-1", "CARDINALITY")}
    events = alerting.gap_transitions(prev, cur, NOW)
    assert sorted((e["Edge"], e["OrderId"], e["From"], e["To"], e["GapAgeSec"]) for e in events) == [
        ("A-Bp", "A-1", "NONE", "CARDINALITY", 60),
        ("Bp-Bc", "Bp-1", "TRANSIENT", "MISSING_DOWNSTREAM", 45),
        ("Cp-Cc", "Cp-1", "OVER_ROUTED", "NONE", 60),
    ]
    assert all(e["Ts"] == NOW and e["Level"] and e["System"] for e in events)
    assert alerting.gap_transitions(cur, cur, NOW) == []
    u = {"Level": "C_PARENT", "System": "C", "OrderId": "Cp-1", "RootId": None, "ParentState": "UNRESOLVED", "AgeSec": 7200}
    n = {"Level": "B_CHILD", "System": "B", "OrderId": "Bc-9", "RootId": None, "ParentState": "NO_LINK", "AgeSec": 5}
    events = alerting.link_transitions({}, {("C_PARENT", "Cp-1"): u, ("B_CHILD", "Bc-9"): n}, NOW)
    assert sorted((e["Edge"], e["OrderId"], e["From"], e["To"], e["FillGap"], e["GapAgeSec"]) for e in events) == [
        ("Bc-Cp", "Cp-1", "NEW", "UNRESOLVED", None, 7200), ("Bp-Bc", "Bc-9", "NEW", "NO_LINK", None, 5)]
    events = alerting.link_transitions({("C_PARENT", "Cp-1"): u}, {}, NOW + SEC)
    assert [(e["From"], e["To"], e["Ts"]) for e in events] == [("UNRESOLVED", "LINKED", NOW + SEC)]


def test_transitions_on_a_healing_family():
    """The late-B-child story of the engine tests, as the oracle sees it between two states."""
    fam = mockdata.Book()
    sc = mockdata.family(fam, "T", recv=mockdata.NOW - 60 * SEC)  # a minute old: aged past TRANSIENT, channels current
    cfg = mockdata.settings_for(fam)
    before = {s: [r for r in fam.rows[s] if r["OrderId"] != sc.ids["Bc1"]] for s in config.SYSTEMS}
    prev = reference.reconcile(before, cfg, fam.now)
    cur = reference.reconcile(fam.rows, cfg, fam.now)
    links = alerting.link_transitions(prev.unresolved_links, cur.unresolved_links, fam.now)
    assert [(e["Level"], e["OrderId"], e["From"], e["To"], e["Edge"]) for e in links] == [
        ("C_PARENT", sc.ids["Cp1"], "UNRESOLVED", "LINKED", "Bc-Cp")]
    gaps = alerting.gap_transitions(prev.gaps, cur.gaps, fam.now)
    assert [(e["Edge"], e["OrderId"], e["From"], e["To"], e["FillGap"]) for e in gaps] == [
        ("Bp-Bc", sc.ids["Bp"], "MISSING_DOWNSTREAM", "NONE", 500)]
    first = alerting.gap_transitions({}, prev.gaps, fam.now) + alerting.link_transitions({}, prev.unresolved_links, fam.now)
    assert len(first) == len(prev.gaps) + len(prev.unresolved_links) == 2
    assert len({alerting.event_key(e) for e in first}) == 2


# -- observations ---------------------------------------------------------------------------------


def test_observation_from_the_scenario_book(oracle):
    o = alerting.observe_result(oracle, NOW)
    age = oracle.unresolved_links[("C_PARENT", "Cp-BCM-1")]["AgeSec"]
    assert o.unresolved == {("C", "Cp-BCM-1"): age} and 7190 <= age <= 7200  # the only unresolved node past the alert age
    assert o.c_parents_minus_b_children == oracle.trace_stats[("CParentsMinusBChildren", "")]
    state, silent = o.channels[("C", mockdata.TOPIC_C_STALE)]
    assert state == "STALE" and 590 <= silent <= 600
    assert o.channels[("B", mockdata.TOPIC_B_CATCHUP)][0] == "CATCHING_UP"
    assert all(state == "CURRENT" for (s, t), (state, _) in o.channels.items() if t in mockdata.TOPICS[s])
    assert o.suppressed_by_topic[("C", mockdata.TOPIC_C_STALE)] >= 1
    assert o.over_routed_by_root["A-OR"] == 100 and o.over_routed_by_root["A-CNP"] == 300
    assert o.drift_qty[("Bp-Bc", "MissingDownstreamQty")] == oracle.trace_stats[("MissingDownstreamQty", "Bp-Bc")] > 0
    assert all(v > 0 for v in o.drift_qty.values()) and "AAPL" in o.symbol_breaks
    d = o.as_dict()
    assert set(d) == {"now", "unresolved", "ratio", "channels", "suppressed", "over_routed", "drift_qty", "symbols"}


# -- the policy ---------------------------------------------------------------------------------------


def test_unresolved_rules_dedupe_summary_page_and_clear():
    p = AlertPolicy(settings(market_hours=""))
    t0 = NOW
    a = p.observe(obs(t0, unresolved={("C", "Cp-1"): 3600}))
    assert kinds(a) == [("unresolved_link", "C:Cp-1", "WARN"), ("unresolved_links", "C", "WARN")]
    assert a[0].value == 3600 and a[0].threshold == 3600 and a[1].value == 1 and a[0].system == "C"
    assert [x.id for x in a] == [1, 2]
    assert p.observe(obs(t0 + 5 * MIN, unresolved={("C", "Cp-1"): 3900})) == []  # one alert per crossing
    a = p.observe(obs(t0 + 15 * MIN, unresolved={("C", "Cp-1"): 4500}))
    assert kinds(a) == [("unresolved_links", "C", "WARN")] and a[0].message.endswith("(summary)")
    many = {("C", f"Cp-{i}"): 3700 for i in range(101)}
    a = p.observe(obs(t0 + 16 * MIN, unresolved=many))
    assert ("unresolved_links", "C", "PAGE") in kinds(a) and sum(1 for k in kinds(a) if k[0] == "unresolved_link") == 100
    page = [x for x in a if x.metric == "unresolved_links"][0]
    assert page.value == 101 and page.previous == "WARN"
    a = p.observe(obs(t0 + 17 * MIN, unresolved={("C", "Cp-0"): 4 * 3600 + 1}))  # one very old node pages too
    assert kinds(a).count(("unresolved_link", "C:Cp-1", "CLEAR")) == 1 and ("unresolved_links", "C", "PAGE") not in kinds(a)
    assert p.open_alerts()[("unresolved_links", "C")] == "PAGE"
    a = p.observe(obs(t0 + 18 * MIN))
    assert kinds(a) == [("unresolved_link", "C:Cp-0", "CLEAR"), ("unresolved_links", "C", "CLEAR")]
    assert a[-1].previous == "PAGE" and p.open_alerts() == {}
    assert p.observe(obs(t0 + 19 * MIN)) == []


def test_ratio_rule_persists_then_warns_pages_and_clears():
    p = AlertPolicy(settings(market_hours=""))
    t0 = NOW
    assert p.observe(obs(t0, c_parents_minus_b_children=5)) == []
    assert p.observe(obs(t0 + 599 * SEC, c_parents_minus_b_children=7)) == []
    a = p.observe(obs(t0 + 600 * SEC, c_parents_minus_b_children=7))
    assert kinds(a) == [("c_parents_vs_b_children", "fleet", "WARN")] and a[0].value == 7
    a = p.observe(obs(t0 + 700 * SEC, c_parents_minus_b_children=-150))
    assert kinds(a) == [("c_parents_vs_b_children", "fleet", "PAGE")] and "never reaching C" in a[0].message
    assert kinds(p.observe(obs(t0 + 800 * SEC, c_parents_minus_b_children=-3))) == []  # stays paged until it clears
    assert kinds(p.observe(obs(t0 + 900 * SEC))) == [("c_parents_vs_b_children", "fleet", "CLEAR")]
    # a flicker shorter than the persistence never alerts, and the clock restarts on each return to zero
    assert p.observe(obs(t0 + 1000 * SEC, c_parents_minus_b_children=1)) == []
    assert p.observe(obs(t0 + 1300 * SEC)) == []
    assert p.observe(obs(t0 + 1400 * SEC, c_parents_minus_b_children=1)) == []
    assert p.observe(obs(t0 + 1900 * SEC, c_parents_minus_b_children=1)) == []
    assert kinds(p.observe(obs(t0 + 2000 * SEC, c_parents_minus_b_children=1))) == [("c_parents_vs_b_children", "fleet", "WARN")]


def test_channel_and_system_rules_follow_market_hours_and_state_changes():
    p = AlertPolicy(settings())
    t0 = NOW
    ch = {("C", "oms.c.t1"): ("STALE", 130), ("C", "oms.c.t2"): ("CURRENT", 5), ("A", "oms.a.t1"): ("CURRENT", 2)}
    a = p.observe(obs(t0, channels=ch, suppressed_by_topic={("C", "oms.c.t1"): 4}))
    assert kinds(a) == [("channel_health", "C:oms.c.t1", "WARN")] and "4 gaps suppressed" in a[0].message and a[0].value == 130
    assert p.observe(obs(t0 + MIN, channels={**ch, ("C", "oms.c.t1"): ("STALE", 190)})) == []
    a = p.observe(obs(t0 + 10 * MIN, channels={**ch, ("C", "oms.c.t1"): ("STALE", 730)}))
    assert kinds(a) == [("channel_health", "C:oms.c.t1", "PAGE")] and a[0].threshold == 600
    a = p.observe(obs(t0 + 11 * MIN, channels={**ch, ("C", "oms.c.t1"): ("CATCHING_UP", 3)}))
    assert kinds(a) == [("channel_health", "C:oms.c.t1", "WARN")] and "CATCHING_UP for 0 s" in a[0].message  # a new state re-alerts
    a = p.observe(obs(t0 + 72 * MIN, channels={**ch, ("C", "oms.c.t1"): ("CATCHING_UP", 3)}))
    assert kinds(a) == [("channel_health", "C:oms.c.t1", "PAGE")] and a[0].value == 61 * 60  # catching up for 61 min
    assert kinds(p.observe(obs(t0 + 73 * MIN, channels=ch | {("C", "oms.c.t1"): ("CURRENT", 1)}))) == [
        ("channel_health", "C:oms.c.t1", "CLEAR")]
    # a whole system silent: every channel quiet
    down = {("A", "oms.a.t1"): ("CURRENT", 70), ("A", "oms.a.t2"): ("CURRENT", 65), ("B", "oms.b.t1"): ("CURRENT", 1)}
    a = p.observe(obs(t0 + 74 * MIN, channels=down))
    assert kinds(a) == [("stale_system", "A", "WARN")] and a[0].value == 65
    a = p.observe(obs(t0 + 80 * MIN, channels={**down, ("A", "oms.a.t1"): ("STALE", 430), ("A", "oms.a.t2"): ("STALE", 425)}))
    assert set(kinds(a)) == {("channel_health", "A:oms.a.t1", "WARN"), ("channel_health", "A:oms.a.t2", "WARN"),
                             ("stale_system", "A", "PAGE")}
    # outside market hours the staleness rules go quiet (and clear what was open)
    night = NOW + 10 * 3600 * SEC  # 21:00 New York
    a = p.observe(obs(night, channels={**down, ("A", "oms.a.t1"): ("STALE", 900), ("A", "oms.a.t2"): ("STALE", 900)}))
    assert set(kinds(a)) == {("channel_health", "A:oms.a.t1", "CLEAR"), ("channel_health", "A:oms.a.t2", "CLEAR"),
                             ("stale_system", "A", "CLEAR")}
    assert p.observe(obs(night + MIN, channels={("A", "oms.a.t1"): ("STALE", 960)})) == []


def test_drift_rules_persist_grow_page_and_count_symbols():
    p = AlertPolicy(settings(market_hours="", over_routed_page_qty=500, drift_page_qty=0))
    t0 = NOW
    assert p.observe(obs(t0, over_routed_by_root={"A-1": 100}, drift_qty={("Bp-Bc", "MissingDownstreamQty"): 100})) == []
    a = p.observe(obs(t0 + 300 * SEC, over_routed_by_root={"A-1": 100}, drift_qty={("Bp-Bc", "MissingDownstreamQty"): 100}))
    assert set(kinds(a)) == {("over_routed", "A-1", "WARN"), ("drift_qty", "Bp-Bc:MissingDownstreamQty", "WARN")}
    assert [x for x in a if x.metric == "drift_qty"][0].message == "Bp-Bc: 100 shares MissingDownstream"
    assert p.observe(obs(t0 + 400 * SEC, over_routed_by_root={"A-1": 100}, drift_qty={("Bp-Bc", "MissingDownstreamQty"): 150})) == []
    a = p.observe(obs(t0 + 500 * SEC, over_routed_by_root={"A-1": 600}, drift_qty={("Bp-Bc", "MissingDownstreamQty"): 200}))
    assert set(kinds(a)) == {("over_routed", "A-1", "PAGE"), ("drift_qty", "Bp-Bc:MissingDownstreamQty", "WARN")}
    assert [x for x in a if x.metric == "drift_qty"][0].message.endswith("(grew)")
    assert p.observe(obs(t0 + 600 * SEC, over_routed_by_root={"A-1": 600}, drift_qty={("Bp-Bc", "MissingDownstreamQty"): 390})) == []
    a = p.observe(obs(t0 + 700 * SEC))
    assert set(kinds(a)) == {("over_routed", "A-1", "CLEAR"), ("drift_qty", "Bp-Bc:MissingDownstreamQty", "CLEAR")}
    # symbols: any new symbol warns at once; more than ten pages the fleet
    a = p.observe(obs(t0 + 800 * SEC, symbol_breaks={"AAPL": 2, "MSFT": 1}))
    assert set(kinds(a)) == {("drift_symbols", "AAPL", "WARN"), ("drift_symbols", "MSFT", "WARN")}
    eleven = {f"S{i}": 1 for i in range(11)}
    a = p.observe(obs(t0 + 900 * SEC, symbol_breaks={"AAPL": 2, **eleven}))
    assert ("drift_symbol_count", "fleet", "PAGE") in kinds(a) and ("drift_symbols", "MSFT", "CLEAR") in kinds(a)
    assert sum(1 for k in kinds(a) if k == ("drift_symbols", "S0", "WARN")) == 1
    a = p.observe(obs(t0 + 1000 * SEC))
    assert len(a) == 13 and all(x.severity == "CLEAR" for x in a)


def test_alert_event_row_and_json_document():
    a = Alert(7, NOW, "channel_health", "C:oms.c.t1", 730, 600, "PAGE", "C oms.c.t1 STALE for 730 s", "C", "WARN")
    e = alerting.alert_event(a)
    assert e == {"Ts": NOW, "Level": "ALERT", "System": "C", "OrderId": "C:oms.c.t1", "RootId": None, "Edge": "channel_health",
                 "From": "WARN", "To": "PAGE", "FillGap": 730, "GapAgeSec": 600}
    doc = json.loads(a.json())
    assert doc == {"source": "oms-trace", "alert_id": 7, "ts": "2026-10-26T15:00:00Z", "metric": "channel_health",
                   "key": "C:oms.c.t1", "value": 730, "threshold": 600, "severity": "PAGE", "message": "C oms.c.t1 STALE for 730 s"}
    assert set(a.row()) == {name for name, _ in alerting.ALERT_COLUMNS}


# -- the sink -------------------------------------------------------------------------------------------


class _Handler(http.server.BaseHTTPRequestHandler):
    received: List[Tuple[str, Dict[str, str], bytes]] = []

    def do_POST(self):  # noqa: N802 - http.server API
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        _Handler.received.append((self.path, dict(self.headers), body))
        if self.path == "/slow":
            time.sleep(0.5)
        status = 503 if self.path == "/fail" else 200
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_):
        pass


@pytest.fixture(scope="module")
def http_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def _wait(predicate, timeout_s: float = 5.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_http_sink_posts_json_with_headers_retries_once_and_reports(http_server):
    results: List[Tuple[int, str, int]] = []
    s = settings(url=http_server + "/ok", auth_header="Authorization: Bearer t0k", retry_delay_sec=0.01)
    sink = alerting.HttpSink(s, on_result=lambda i, posted, attempts, ts: results.append((i, posted, attempts)))
    alert = Alert(1, NOW, "drift_qty", "Bp-Bc:MissingDownstreamQty", 100, 0, "WARN", "Bp-Bc: 100 shares MissingDownstream")
    assert sink.post(alert)
    assert _wait(lambda: results == [(1, "200", 1)])
    path, headers, body = _Handler.received[-1]
    assert path == "/ok" and headers["Authorization"] == "Bearer t0k" and headers["Content-Type"] == "application/json"
    assert json.loads(body)["key"] == "Bp-Bc:MissingDownstreamQty" and sink.counts["posted"] == 1
    sink.stop()
    # a failing endpoint: retried once, the table keeps the record
    results.clear()
    sink = alerting.HttpSink(settings(url=http_server + "/fail", retry_delay_sec=0.01),
                             on_result=lambda i, posted, attempts, ts: results.append((i, posted, attempts)))
    sink.post(Alert(2, NOW, "m", "k", 1, 0, "WARN", "x"))
    assert _wait(lambda: results == [(2, "HTTP 503", 2)]) and sink.counts["failed"] == 1 and sink.last_error == "HTTP 503"
    sink.stop()
    # nobody listening
    results.clear()
    sink = alerting.HttpSink(settings(url="http://127.0.0.1:9/nothing", retry_delay_sec=0.01, timeout_sec=0.5),
                             on_result=lambda i, posted, attempts, ts: results.append((i, posted, attempts)))
    sink.post(Alert(3, NOW, "m", "k", 1, 0, "WARN", "x"))
    assert _wait(lambda: len(results) == 1) and results[0][1].startswith("ERR") and results[0][2] == 2
    sink.stop()


def test_http_sink_drops_when_the_queue_is_full(http_server):
    results: List[Tuple[int, str, int]] = []
    sink = alerting.HttpSink(settings(url=http_server + "/slow", max_pending=1, retry_delay_sec=0.01),
                             on_result=lambda i, posted, attempts, ts: results.append((i, posted, attempts)))
    outcomes = [sink.post(Alert(i, NOW, "m", "k", 1, 0, "WARN", "x")) for i in (1, 2, 3)]
    assert outcomes.count(False) >= 1 and sink.counts["dropped"] >= 1
    assert any(r[1] == "DROPPED" for r in results)
    assert _wait(lambda: sum(1 for r in results if r[1] == "200") == outcomes.count(True), 5.0)
    sink.stop()
    with pytest.raises(ValueError):
        alerting.HttpSink(settings(url=""))
