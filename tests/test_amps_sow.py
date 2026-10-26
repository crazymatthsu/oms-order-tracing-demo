"""The AMPS SOW source on a bare python: settings, body parsers, the fold, the stub-client wiring.

The fold is driven with message doubles that expose the three getters the mirror reads
(``get_command``, ``get_sow_key``, ``get_data``) and, when ``amps-python-client`` is installed,
with real ``AMPS.Message`` objects too, so the getters' names and return types are the client's.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pytest

from oms_trace import amps_sow, config, mockdata
from oms_trace.amps_testing import FakeMessage, StubClient, StubCommand
from oms_trace.amps_sow import CHANGE, SNAPSHOT, AmpsSettings, TopicMirror

SEC = config.SECOND_NANOS
NY = ZoneInfo("America/New_York")
BASE_ENV = {"OMS_AMPS_URI": "tcp://amps-a:9007/amps/json, tcp://user:secret@amps-b:9007/amps/json",
            "OMS_AMPS_TOPICS_A": "oms.a.t1", "OMS_AMPS_TOPICS_B": "oms.b.t1, oms.b.t2", "OMS_AMPS_TOPICS_C": "oms.c.t1"}


def real_message(command: str, data: Optional[str] = None, sow_key: str = "") -> Any:
    AMPS = pytest.importorskip("AMPS")
    m = AMPS.Message()
    m.set_command(command)
    if sow_key:
        m.set_sow_key(sow_key)
    if data is not None:
        m.set_data(data)
    return m


MESSAGE_FACTORIES = [pytest.param(FakeMessage, id="double"), pytest.param(real_message, id="AMPS.Message")]


def body(order_id: str, *, qty: int = 1000, cum: int = 0, parent: Optional[str] = None, upstream: Optional[str] = None,
         date_: Optional[str] = None, **extra: Any) -> str:
    import json

    fields: Dict[str, Any] = {"OrderId": order_id, "Symbol": "AAPL", "Side": "BUY", "OrderQty": qty, "CumQty": cum,
                              "LeavesQty": qty - cum, "OrdStatus": "NEW" if cum == 0 else "PARTIALLY_FILLED",
                              "EventTs": "2026-10-26T14:59:59.980Z", "SeqNo": 1,
                              "ParentOrderId": parent, "UpstreamRef": upstream}
    if date_:
        fields["Date"] = date_
    fields.update(extra)
    return json.dumps(fields)


# -- settings -----------------------------------------------------------------------------------


def test_settings_from_env_defaults_fallbacks_and_masking():
    a = AmpsSettings.from_env(BASE_ENV)
    assert a.uris == ("tcp://amps-a:9007/amps/json", "tcp://user:secret@amps-b:9007/amps/json")
    assert a.topics == {"A": ("oms.a.t1",), "B": ("oms.b.t1", "oms.b.t2"), "C": ("oms.c.t1",)}
    assert a.subscriptions() == [("A", "oms.a.t1"), ("B", "oms.b.t1"), ("B", "oms.b.t2"), ("C", "oms.c.t1")]
    assert a.client_name == "dh-oms-trace" and a.heartbeat_sec == 5 and a.body_format == "json"
    assert a.tag_map["37"] == "OrderId" and a.field_map == {} and a.persist_snapshot is False
    assert a.tz == "America/New_York"
    assert "secret" not in a.describe() and "****" in a.describe()
    # the topics fall back to the Kafka topic lists, the zone follows OMS_TRACE_TZ
    env = {"OMS_AMPS_URI": "tcp://h:9007/amps/json", "OMS_TOPICS_B": "k.b.1", "OMS_TRACE_TZ": "Europe/London"}
    b = AmpsSettings.from_env(env)
    assert b.topics == {"A": (), "B": ("k.b.1",), "C": ()} and b.tz == "Europe/London"
    only_b = a.for_system("B")
    assert only_b.topics == {"A": (), "B": ("oms.b.t1", "oms.b.t2"), "C": ()} and only_b.client_name == "dh-oms-trace-b"


def test_settings_maps_and_validation():
    env = dict(BASE_ENV, OMS_AMPS_FORMAT="fix", OMS_AMPS_TAG_MAP="26009=ParentOrderId, 26010=UpstreamRef",
               OMS_AMPS_FIELD_MAP="ClOrdID=OrderId", OMS_AMPS_HEARTBEAT_SEC="0", OMS_AMPS_PERSIST_SNAPSHOT="true")
    a = AmpsSettings.from_env(env)
    assert a.body_format == "fix" and a.heartbeat_sec == 0 and a.persist_snapshot is True
    assert a.tag_map["26009"] == "ParentOrderId" and a.tag_map["26010"] == "UpstreamRef" and a.tag_map["14"] == "CumQty"
    assert a.field_map == {"ClOrdID": "OrderId"}
    for bad in ({"OMS_AMPS_URI": ""}, {"OMS_AMPS_FORMAT": "xml"}, {"OMS_AMPS_HEARTBEAT_SEC": "-1"},
                {"OMS_AMPS_FIELD_MAP": "x=NoSuchColumn"}, {"OMS_AMPS_TAG_MAP": "99=RecvTs"},
                {"OMS_AMPS_TAG_MAP": "garbage"}, {"OMS_AMPS_PERSIST_SNAPSHOT": "maybe"}, {"OMS_AMPS_BATCH_SIZE": "0"}):
        with pytest.raises(config.SettingsError):
            AmpsSettings.from_env(dict(BASE_ENV, **bad))
    with pytest.raises(config.SettingsError):  # no topic anywhere
        AmpsSettings.from_env({"OMS_AMPS_URI": "tcp://h:9007/amps/json"})
    assert config.load_settings({"OMS_TRACE_SOURCE": "amps"}).source == "amps"


# -- bodies -------------------------------------------------------------------------------------


def test_json_body_to_raw_row_stamps_arrival_and_derives_the_date():
    a = AmpsSettings.from_env(dict(BASE_ENV, OMS_AMPS_FIELD_MAP="ClOrdID=OrderId,Qty=OrderQty"))
    parse = amps_sow.make_parser(a)
    fields = parse('{"ClOrdID": "B-1", "Qty": "500", "CumQty": 200.0, "LeavesQty": 300, "Side": "BUY", "Symbol": "MSFT",'
                   ' "EventTs": 1792861199980000000, "ParentOrderId": null, "UpstreamRef": "A-1", "Topic": "spoofed",'
                   ' "KafkaOffset": 7, "Ignored": 1}')
    recv = mockdata.NOW  # 2026-10-26T15:00Z = 11:00 New York
    row = amps_sow.raw_row("B", "oms.b.t1", fields, recv, NY)
    assert set(row) == set(config.RAW_COLUMN_NAMES)
    assert row["OrderId"] == "B-1" and row["OrderQty"] == 500 and row["CumQty"] == 200 and row["LeavesQty"] == 300
    assert row["UpstreamRef"] == "A-1" and row["ParentOrderId"] is None
    assert row["EventTs"] == 1792861199980000000 and row["RecvTs"] == recv
    assert row["Topic"] == "oms.b.t1" and row["KafkaOffset"] is None and row["KafkaPartition"] is None
    assert row["Date"] == "2026-10-26" and row["SowDeleted"] is False
    # 03:00Z is still the previous trading date in New York; a Date in the body wins over the clock
    night = mockdata.NOW - 12 * 3600 * SEC
    assert amps_sow.raw_row("B", "t", fields, night, NY)["Date"] == "2026-10-25"
    assert amps_sow.raw_row("B", "t", dict(fields, Date="2026-10-24"), night, NY)["Date"] == "2026-10-24"
    with pytest.raises(ValueError):
        amps_sow.raw_row("B", "t", {"Symbol": "X"}, recv, NY)  # no OrderId
    with pytest.raises(ValueError):
        amps_sow.raw_row("B", "t", {"OrderId": "B-2", "CumQty": "12.5"}, recv, NY)  # not a whole number
    with pytest.raises(ValueError):
        parse("[1, 2]")


def test_fix_body_through_the_tag_map_and_tag60_timestamps():
    a = AmpsSettings.from_env(dict(BASE_ENV, OMS_AMPS_FORMAT="fix", OMS_AMPS_TAG_MAP="26009=ParentOrderId,26010=UpstreamRef"))
    parse = amps_sow.make_parser(a)
    soh = "\x01"
    fields = parse(soh.join(["37=Cc-1", "55=NVDA", "54=1", "38=400", "14=150", "151=250", "6=101.25", "39=1",
                             "60=20261026-14:59:59.980", "26009=Cp-1", "34=17", "9999=ignored"]))
    row = amps_sow.raw_row("C", "oms.c.t1", fields, mockdata.NOW, NY)
    assert row["OrderId"] == "Cc-1" and row["ParentOrderId"] == "Cp-1" and row["UpstreamRef"] is None
    assert row["OrderQty"] == 400 and row["CumQty"] == 150 and row["LeavesQty"] == 250 and row["AvgPx"] == 101.25
    assert row["Side"] == "1" and row["OrdStatus"] == "1" and row["SeqNo"] == 17  # codes pass through untranslated (Q13)
    assert row["EventTs"] == mockdata.NOW - 20 * 1_000_000
    assert parse("37=X|14=1")["CumQty"] == "1"  # pipe delimited is accepted too
    with pytest.raises(ValueError):
        parse("37=X|garbage")
    assert amps_sow.to_nanos("20261026-14:59:59") == mockdata.NOW - SEC
    assert amps_sow.to_nanos("2026-10-26T15:00:00+00:00") == mockdata.NOW
    assert amps_sow.to_nanos("2026-10-26 11:00:00-04:00") == mockdata.NOW
    assert amps_sow.to_nanos(str(mockdata.NOW)) == mockdata.NOW and amps_sow.to_nanos(None) is None


# -- the fold -----------------------------------------------------------------------------------


def mirror(fmt: str = "json") -> TopicMirror:
    a = AmpsSettings.from_env(dict(BASE_ENV, OMS_AMPS_FORMAT=fmt))
    return TopicMirror("B", "oms.b.t1", amps_sow.make_parser(a), NY)


@pytest.mark.parametrize("msg", MESSAGE_FACTORIES)
def test_snapshot_publish_delete_and_reconnect_fold(msg):
    m = mirror()
    t = mockdata.NOW
    assert m.on_message(msg("group_begin"), t) == [] and m.epoch == 1 and m.in_snapshot
    out = m.on_message(msg("sow", body("Bp-1", upstream="A-1"), sow_key="k1"), t + 1)
    out += m.on_message(msg("sow", body("Bc-1", parent="Bp-1", cum=100), sow_key="k2"), t + 2)
    assert [(r["OrderId"], s) for r, s in out] == [("Bp-1", SNAPSHOT), ("Bc-1", SNAPSHOT)]
    assert m.on_message(msg("group_end"), t + 3) == [] and not m.in_snapshot and m.counts["snapshots"] == 1
    # a new full state is a change row; the mirror keeps the latest state per order
    out = m.on_message(msg("publish", body("Bc-1", parent="Bp-1", cum=400), sow_key="k2"), t + 4)
    assert [(r["OrderId"], r["CumQty"], s) for r, s in out] == [("Bc-1", 400, CHANGE)]
    assert m.records["Bc-1"]["CumQty"] == 400 and m.records["Bc-1"]["RecvTs"] == t + 4
    # a third child arrives, then the first child leaves the SOW (oof carries the SowKey only)
    m.on_message(msg("publish", body("Bc-3", parent="Bp-1", cum=50), sow_key="k3"), t + 5)
    out = m.on_message(msg("oof", sow_key="k2"), t + 6)
    assert len(out) == 1 and out[0][1] == CHANGE
    gone = out[0][0]
    assert gone["OrderId"] == "Bc-1" and gone["SowDeleted"] is True and gone["CumQty"] == 400 and gone["RecvTs"] == t + 6
    assert m.on_message(msg("oof", sow_key="k2"), t + 7) == []  # already gone: nothing more
    assert m.counts["delete"] == 1 and m.live() == 2 and len(m.records) == 3
    # a sow_delete without a SowKey but with the record's body
    out = m.on_message(msg("sow_delete", body("Bc-3", parent="Bp-1", cum=50)), t + 8)
    assert [(r["OrderId"], r["SowDeleted"]) for r, _ in out] == [("Bc-3", True)]
    # reconnect: the new snapshot re-sends Bp-1 only; Bc-2 (published meanwhile) was deleted while away
    m.on_message(msg("publish", body("Bc-2", parent="Bp-1", cum=10), sow_key="k4"), t + 9)
    assert m.live() == 2
    assert m.on_message(msg("group_begin"), t + 10) == [] and m.epoch == 2 and m.in_snapshot
    out = m.on_message(msg("sow", body("Bp-1", upstream="A-1", cum=410), sow_key="k1"), t + 11)
    assert [(r["OrderId"], s) for r, s in out] == [("Bp-1", SNAPSHOT)]
    out = m.on_message(msg("group_end"), t + 12)
    assert [(r["OrderId"], r["SowDeleted"], r["CumQty"], s) for r, s in out] == [("Bc-2", True, 10, CHANGE)]
    assert m.counts["dropped"] == 1 and m.counts["snapshots"] == 2 and m.live() == 1
    # the already-deleted records are not re-emitted by the drop, and ignored commands do nothing
    assert m.on_message(msg("ack"), t + 13) == [] and m.on_message(msg("heartbeat"), t + 14) == []
    status = m.status(True, t + 15)
    assert status["Epoch"] == 2 and status["SnapshotInProgress"] is False and status["Connected"] is True
    assert status["Records"] == 4 and status["Live"] == 1 and status["Deleted"] == 3 and status["Rejected"] == 0
    assert status["LastRecvTs"] == t + 12


def test_delta_publish_merges_and_garbage_is_rejected_not_fatal():
    m = mirror()
    t = mockdata.NOW
    m.on_message(FakeMessage("group_begin"), t)
    m.on_message(FakeMessage("sow", body("Bc-1", parent="Bp-1", qty=1000, cum=100), sow_key="k2"), t + 1)
    m.on_message(FakeMessage("group_end"), t + 2)
    out = m.on_message(FakeMessage("delta_publish", '{"OrderId": "Bc-1", "CumQty": 300, "LeavesQty": 700}', sow_key="k2"), t + 3)
    row = out[0][0]
    assert row["CumQty"] == 300 and row["LeavesQty"] == 700 and row["OrderQty"] == 1000 and row["ParentOrderId"] == "Bp-1"
    assert row["Symbol"] == "AAPL" and row["RecvTs"] == t + 3 and row["SowDeleted"] is False
    # garbage: a non-JSON body, a body without an order id, a non-numeric quantity
    for bad in ("not json", '{"Symbol": "X"}', '{"OrderId": "Bc-9", "CumQty": "lots"}'):
        assert m.on_message(FakeMessage("publish", bad, sow_key="k9"), t + 4) == []
    assert m.counts["rejected"] == 3 and "CumQty" in (m.last_error or "")
    assert m.live() == 1
    # an oof for a record never seen live still records its last state as deleted
    out = m.on_message(FakeMessage("oof", body("Bc-7", parent="Bp-1", cum=5), sow_key="k7"), t + 5)
    assert [(r["OrderId"], r["SowDeleted"]) for r, _ in out] == [("Bc-7", True)]
    assert m.on_message(FakeMessage("oof", sow_key="unknown-key"), t + 6) == []


def test_fix_bodies_fold_the_same_way():
    m = mirror("fix")
    t = mockdata.NOW
    m.on_message(FakeMessage("group_begin"), t)
    out = m.on_message(FakeMessage("sow", "37=Bp-1|38=1000|14=0|151=1000|55=AAPL|54=1|39=0", sow_key="k1"), t + 1)
    assert out[0][0]["OrderId"] == "Bp-1" and out[0][0]["ParentOrderId"] is None  # no parent tag mapped: a parent
    assert m.on_message(FakeMessage("group_end"), t + 2) == []


# -- the source around a stub client ---------------------------------------------------------------


def stub_source(env: Optional[Dict[str, str]] = None, clock_start: int = mockdata.NOW):
    clients: List[StubClient] = []
    ticks = {"now": clock_start}

    def client_factory(amps: AmpsSettings) -> StubClient:
        clients.append(StubClient())
        return clients[-1]

    def clock() -> int:
        ticks["now"] += 1_000_000
        return ticks["now"]

    settings = config.load_settings({})
    amps = AmpsSettings.from_env(dict(BASE_ENV, **(env or {})), settings=settings)
    source = amps_sow.AmpsSowSource(settings, amps, client_factory=client_factory, command_factory=StubCommand, clock=clock)
    return source, clients


def test_source_subscribes_every_topic_routes_rows_per_system_and_tracks_the_connection():
    source, clients = stub_source()
    source.connect()  # no tables: the buffers are the contract on a bare python
    client = clients[0]
    assert set(client.handlers) == {"oms.a.t1", "oms.b.t1", "oms.b.t2", "oms.c.t1"} and source.connected
    # connecting published one status row per topic, all Connected
    status = source.status_buffer.drain()
    assert sorted((r["System"], r["Topic"], r["Connected"]) for r in status) == [
        ("A", "oms.a.t1", True), ("B", "oms.b.t1", True), ("B", "oms.b.t2", True), ("C", "oms.c.t1", True)]
    b1, b2, a1 = client.handlers["oms.b.t1"], client.handlers["oms.b.t2"], client.handlers["oms.a.t1"]
    for h in (a1, b1, b2):
        h(FakeMessage("group_begin"))
    a1(FakeMessage("sow", body("A-1"), sow_key="a1"))
    b1(FakeMessage("sow", body("Bp-1", upstream="A-1"), sow_key="b1"))
    b2(FakeMessage("sow", body("Bc-1", parent="Bp-1"), sow_key="b2"))
    for h in (a1, b1, b2):
        h(FakeMessage("group_end"))
    b2(FakeMessage("publish", body("Bc-1", parent="Bp-1", cum=10), sow_key="b2"))
    rows_b = source.buffers["B"].drain()
    assert [(r["OrderId"], r["Topic"], s, e) for r, s, e in rows_b] == [
        ("Bp-1", "oms.b.t1", SNAPSHOT, 1), ("Bc-1", "oms.b.t2", SNAPSHOT, 1), ("Bc-1", "oms.b.t2", CHANGE, 1)]
    assert [(r["OrderId"], s) for r, s, _ in source.buffers["A"].drain()] == [("A-1", SNAPSHOT)]
    assert source.buffers["C"].drain() == []
    # snapshot boundaries publish status rows: in progress, then complete
    status = source.status_buffer.drain()
    b1_rows = [r for r in status if r["Topic"] == "oms.b.t1"]
    assert [r["SnapshotInProgress"] for r in b1_rows] == [True, False] and b1_rows[-1]["Epoch"] == 1
    # the broker goes away: every topic is reported disconnected at once; the mirror keeps its records
    client.listeners[0](False)
    assert not source.connected
    status = source.status_buffer.drain()
    assert len(status) == 4 and all(r["Connected"] is False for r in status)
    assert source.mirrors[("B", "oms.b.t2")].records["Bc-1"]["CumQty"] == 10
    # the HA client reconnects and re-snapshots: Bc-1 was deleted while away
    client.listeners[0](True)
    b2(FakeMessage("group_begin"))
    b2(FakeMessage("group_end"))
    dropped = source.buffers["B"].drain()
    assert [(r["OrderId"], r["SowDeleted"], s, e) for r, s, e in dropped] == [("Bc-1", True, CHANGE, 2)]
    assert [r["Topic"] for r in source.status()] == ["oms.a.t1", "oms.b.t1", "oms.b.t2", "oms.c.t1"]
    assert all(r["Connected"] for r in source.status())
    source.stop()
    assert client.closed and len(client.unsubscribed) == 4 and not source.connected
    assert source.buffers["B"].offer(("x", CHANGE, 1)) is False  # closed buffers refuse


def test_event_buffer_is_bounded_and_drains_in_order():
    buf = amps_sow.EventBuffer(max_pending=2)
    assert buf.offer(1) and buf.offer(2)
    assert buf.offer(3, timeout=0.01) is False and buf.dropped == 1
    assert buf.drain() == [1, 2] and buf.pending == 0 and buf.drain() == []
    assert buf.offer(4) and buf.offered == 3


def test_start_source_is_idempotent_per_client_name(monkeypatch):
    monkeypatch.setattr(amps_sow, "_ACTIVE", [])
    monkeypatch.setattr(amps_sow.AmpsSowSource, "build_tables", lambda self: {})
    settings = config.load_settings({})
    amps = AmpsSettings.from_env(BASE_ENV, settings=settings)
    first = amps_sow.start_source(settings, amps, background=False, client_factory=lambda a: StubClient(),
                                  command_factory=StubCommand)
    again = amps_sow.start_source(settings, amps, background=False, client_factory=lambda a: StubClient(),
                                  command_factory=StubCommand)
    assert again is first and first.connected
    other = amps_sow.start_source(settings, amps.for_system("B").__class__(**{**amps.__dict__, "heartbeat_sec": 9}),
                                  background=False, client_factory=lambda a: StubClient(), command_factory=StubCommand)
    assert other is not first and not first.connected and amps_sow._ACTIVE == [other]
    other.stop()


def test_date_window_of_the_source_follows_the_arrival_date():
    # the source stamps Date in OMS_TRACE_TZ, the DAG windows on settings.dates(): both use the same zone
    recv = mockdata.NOW
    row = amps_sow.raw_row("A", "t", {"OrderId": "A-1"}, recv, ZoneInfo("Asia/Tokyo"))
    assert row["Date"] == "2026-10-27"  # 15:00Z is already the 27th in Tokyo
    assert date.fromisoformat(row["Date"]).isoformat() in config.Settings(as_of=date(2026, 10, 26)).dates()


def test_publisher_bodies_round_trip_through_the_source(book):
    """What amps_publish sends, the source reads back as the same contract values (minus the stamped columns)."""
    from oms_trace import amps_publish

    a = AmpsSettings.from_env(BASE_ENV)
    parse = amps_sow.make_parser(a)
    items = list(amps_publish.messages(book.rows))
    assert len(items) == len(book.all_rows()) and items[0][0] == book.rows["A"][0]["Topic"]
    for row, (topic, body_text) in zip(book.all_rows(), items):
        back = amps_sow.raw_row(row["Topic"][4].upper(), topic, parse(body_text), row["RecvTs"], NY)
        for col in config.RAW_COLUMN_NAMES:
            if col in ("KafkaPartition", "KafkaOffset", "SowDeleted"):
                continue
            assert back[col] == row[col], (row["OrderId"], col, back[col], row[col])
        assert back["SowDeleted"] is False and back["KafkaOffset"] is None
