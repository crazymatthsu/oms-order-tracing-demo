"""Break events and alerts on the engine (docs/design/04 §7, 08 §2): listeners feeding blink tables.

``attach(tables, settings, alert_settings, clock)`` adds to a built lineage:

=================  ===============================================================================
``break_events``   blink: one row per node whose ``BreakKind`` (a listener on ``gaps``) or
                   ``ParentState`` (a listener on ``unresolved_links``) changed this cycle, plus
                   every alert row as a ``Level = ALERT`` row (doc 03 §4.5)
``break_history``  the last ``ring`` of those rows, for the UI and the tests
``alert_events``   blink: one row per threshold crossing; :class:`oms_trace.alerting.AlertPolicy`
                   runs on every clock tick (a listener on the clock, with the tables it reads
                   declared as dependencies)
``alerts``         the last ``ring`` alert rows joined with the sink's results (``Posted``,
                   ``PostedTs``, ``Attempts``; ``NO_SINK`` when ``OMS_TRACE_ALERT_URL`` is empty)
=================  ===============================================================================

The listeners run on the update-graph thread and only publish; the policy is the pure module's.
Keep the returned :class:`Attachment` alive (``app.build`` does): it owns the listener handles,
the publishers and the sink thread.
"""

from __future__ import annotations

import time
import traceback
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

import numpy as np

from oms_trace import alerting, config
from oms_trace.alerting import Alert, AlertPolicy, AlertSettings, Observation

GAP_COLS = ["Now", "Level", "System", "OrderId", "RootId", "Edge", "BreakKind", "FillGap", "GapAgeSec"]
LINK_COLS = ["Now", "Level", "System", "OrderId", "RootId", "ParentState", "AgeSec"]
#: The engine hands a listener String columns as numpy ``str_`` arrays (``np.array(j_array, np.str_)``), which
#: turns a Java null into the text ``'None'``. RootId is the only nullable String read here; an order id
#: spelled "None" is not a thing.
NULLABLE_STRINGS = frozenset({"RootId"})


def _cell(value: Any) -> Any:
    """One numpy cell from a listener update as a plain python value (nulls -> None)."""
    from deephaven.constants import NULL_DOUBLE, NULL_INT, NULL_LONG, NULL_SHORT

    if value is None:
        return None
    if isinstance(value, np.datetime64):
        return None if np.isnat(value) else int(value.astype("datetime64[ns]").astype("int64"))
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        v = int(value)
        return None if v in (NULL_LONG, NULL_INT, NULL_SHORT) else v
    if isinstance(value, np.floating):
        v = float(value)
        return None if v == NULL_DOUBLE or np.isnan(v) else v
    if isinstance(value, np.generic):
        return value.item()
    if hasattr(value, "getEpochSecond"):  # a java.time.Instant object array
        return int(value.getEpochSecond()) * config.SECOND_NANOS + int(value.getNano())
    return value


def _rows(columns: Mapping[str, np.ndarray]) -> List[Dict[str, Any]]:
    if not columns:
        return []
    n = len(next(iter(columns.values())))
    out = []
    for i in range(n):
        row = {name: _cell(array[i]) for name, array in columns.items()}
        for name in NULLABLE_STRINGS:
            if row.get(name) == "None":
                row[name] = None
        out.append(row)
    return out


def _table_rows(table: Any) -> List[Dict[str, Any]]:
    """Read a (small) dependency table inside a listener: the auto-locking context yields on the UG thread."""
    return [{k: _cell(v) if isinstance(v, np.generic) else (_cell(v) if hasattr(v, "getEpochSecond") else v)
             for k, v in r.items()} for r in table.iter_dict()]


class Attachment:
    """Owns the publishers, the listeners, the policy and the sink; ``tables`` has the four outputs."""

    def __init__(self, tables: Mapping[str, Any], settings: config.Settings, alert_settings: AlertSettings,
                 clock: Any, sink_factory: Optional[Callable[[AlertSettings, Callable], Any]] = None) -> None:
        from deephaven import new_table, ring_table
        from deephaven.execution_context import get_exec_ctx
        from deephaven.stream.table_publisher import table_publisher
        from deephaven.table_listener import listen

        from oms_trace import sources

        self.settings = settings
        self.alert_settings = alert_settings
        self.policy = AlertPolicy(alert_settings)
        self._ctx = get_exec_ctx()
        self._new_table = new_table
        self._typed_columns = sources.typed_columns
        self.counts = {"events": 0, "alerts": 0}

        defs = {name: sources.dtype(kind) for name, kind in alerting.EVENT_COLUMNS}
        self._events_table, self._events_pub = table_publisher("break_events", defs, on_shutdown_callback=self.stop)
        adefs = {name: sources.dtype(kind) for name, kind in alerting.ALERT_COLUMNS}
        self._alerts_table, self._alerts_pub = table_publisher("alert_events", adefs, on_shutdown_callback=self.stop)
        pdefs = {name: sources.dtype(kind) for name, kind in alerting.POST_COLUMNS}
        self._posts_table, self._posts_pub = table_publisher("alert_posts", pdefs, on_shutdown_callback=self.stop)

        self.sink = None
        if alert_settings.url:
            factory = sink_factory or (lambda s, on_result: alerting.HttpSink(s, on_result=on_result))
            self.sink = factory(alert_settings, self._on_post_result)

        # the observation tables, prepared beforehand and declared as dependencies (listen() docs)
        from deephaven import merge

        fh = tables["feed_health"]
        suppressed = tables["feed_suppressed"].update_view(["ChildSystem = (Edge == `A-Bp` || Edge == `Bp-Bc`) ? `B` : `C`"])
        # a suppressed gap counts against every non-current channel involved (alerting.observe_result is the twin)
        parent_side = suppressed.where("!isNull(ParentFeed) && ParentFeed != `CURRENT`").view(["System", "Topic"])
        child_side = (suppressed.where("ChildCount > 0").view(["ChildSystem", "ChildTopics"]).ungroup(["ChildTopics"])
                      .natural_join(fh, on=["ChildSystem=System", "ChildTopics=Topic"], joins=["StaleInt"])
                      .where("!isNull(StaleInt) && StaleInt > 0").view(["System = ChildSystem", "Topic = ChildTopics"]))
        system_side = (suppressed.where("ChildCount == 0 && ChildSideStale").view(["ChildSystem"])
                       .join(fh.where("StaleInt > 0").view(["System", "Topic"]), on=["ChildSystem=System"], joins=["Topic"])
                       .view(["System = ChildSystem", "Topic"]))
        self._deps = {
            "trace_stats": tables["trace_stats"],
            "unresolved_alerted": tables["unresolved_links"].where("Alerted").view(["System", "OrderId", "AgeSec"]),
            "feed_health": fh.view(["System", "Topic", "TopicState", "SilentSec"]),
            "suppressed_by_topic": merge([parent_side, child_side, system_side]).count_by("N", by=["System", "Topic"]),
            "over_routed_roots": tables["drift_by_a_order"].where("OverRoutedQty > 0").view(["RootId", "OverRoutedQty"]),
            "drift_by_symbol": tables["drift_by_symbol"].view(["Symbol", "Breaks"]),
        }
        self._handles = [
            listen(tables["gaps"], self._on_gaps, description="oms-trace break events (gaps)", do_replay=True),
            listen(tables["unresolved_links"], self._on_links, description="oms-trace break events (links)", do_replay=True),
            listen(clock, self._on_tick, description="oms-trace alert policy", do_replay=True,
                   dependencies=list(self._deps.values())),
        ]
        history = ring_table(self._events_table, alert_settings.ring)
        alerts = ring_table(self._alerts_table, alert_settings.ring).natural_join(
            self._posts_table.last_by(["AlertId"]), on=["AlertId"], joins=["Posted", "PostedTs", "Attempts"])
        self.tables: Dict[str, Any] = {
            "break_events": self._events_table, "break_history": history,
            "alert_events": self._alerts_table, "alerts": alerts,
        }

    # -- publishing ---------------------------------------------------------------------------

    def _publish(self, publisher: Any, spec: Sequence, rows: List[Dict[str, Any]]) -> None:
        if rows:
            with self._ctx:
                publisher.add(self._new_table(self._typed_columns(spec, rows)))

    def _emit_events(self, events: List[Dict[str, Any]]) -> None:
        self.counts["events"] += len(events)
        self._publish(self._events_pub, alerting.EVENT_COLUMNS, events)

    def _on_post_result(self, alert_id: int, posted: str, attempts: int, ts: int) -> None:
        self._publish(self._posts_pub, alerting.POST_COLUMNS,
                      [{"AlertId": alert_id, "Posted": posted, "PostedTs": ts, "Attempts": attempts}])

    # -- listeners (update-graph thread) ----------------------------------------------------------

    def _on_gaps(self, update: Any, is_replay: bool) -> None:
        try:
            events = []
            for g in _rows(update.added(GAP_COLS)):
                events.append(alerting._gap_event(g, "NONE", g["BreakKind"], g["Now"]))
            if not is_replay:
                for g in _rows(update.removed(GAP_COLS)):
                    events.append(alerting._gap_event(g, g["BreakKind"], "NONE", g["Now"]))
                cur, prev = _rows(update.modified(GAP_COLS)), _rows(update.modified_prev(["BreakKind"]))
                for g, p in zip(cur, prev):
                    if p["BreakKind"] != g["BreakKind"]:
                        events.append(alerting._gap_event(g, p["BreakKind"], g["BreakKind"], g["Now"]))
            self._emit_events(events)
        except Exception:  # noqa: BLE001 - a listener exception would be removed from the graph
            traceback.print_exc()

    def _on_links(self, update: Any, is_replay: bool) -> None:
        try:
            events = []
            for u in _rows(update.added(LINK_COLS)):
                events.append(alerting._link_event(u, "NEW", u["ParentState"], u["Now"]))
            if not is_replay:
                for u in _rows(update.removed(LINK_COLS)):
                    events.append(alerting._link_event(u, u["ParentState"], "LINKED", u["Now"]))
                cur, prev = _rows(update.modified(LINK_COLS)), _rows(update.modified_prev(["ParentState"]))
                for u, p in zip(cur, prev):
                    if p["ParentState"] != u["ParentState"]:
                        events.append(alerting._link_event(u, p["ParentState"], u["ParentState"], u["Now"]))
            self._emit_events(events)
        except Exception:  # noqa: BLE001
            traceback.print_exc()

    def _on_tick(self, update: Any, is_replay: bool) -> None:
        try:
            nows = _rows(update.added(["Now"])) or _rows(update.modified(["Now"]))
            now = nows[-1]["Now"] if nows and nows[-1]["Now"] is not None else time.time_ns()
            alerts = self.policy.observe(self.observe(now))
            if not alerts:
                return
            self.counts["alerts"] += len(alerts)
            self._publish(self._alerts_pub, alerting.ALERT_COLUMNS, [a.row() for a in alerts])
            self._emit_events([alerting.alert_event(a) for a in alerts])
            if self.sink is not None:
                for a in alerts:
                    self.sink.post(a)
            else:
                self._publish(self._posts_pub, alerting.POST_COLUMNS,
                              [{"AlertId": a.id, "Posted": alerting.NO_SINK, "PostedTs": now, "Attempts": 0} for a in alerts])
        except Exception:  # noqa: BLE001
            traceback.print_exc()

    # -- observation ------------------------------------------------------------------------------

    def observe(self, now: int) -> Observation:
        """The :class:`Observation` from the live tables (the twin of ``alerting.observe_result``)."""
        obs = Observation(now=now)
        for r in _table_rows(self._deps["unresolved_alerted"]):
            obs.unresolved[(r["System"], r["OrderId"])] = int(r["AgeSec"])
        for r in _table_rows(self._deps["trace_stats"]):
            if r["Metric"] == "CParentsMinusBChildren":
                obs.c_parents_minus_b_children = int(r["Value"])
            elif r["Metric"] in ("MissingDownstreamQty", "UnbookedUpstreamQty") and r["Value"] > 0:
                obs.drift_qty[(r["Key"], r["Metric"])] = int(r["Value"])
        for r in _table_rows(self._deps["feed_health"]):
            obs.channels[(r["System"], r["Topic"])] = (r["TopicState"], int(r["SilentSec"]))
        for r in _table_rows(self._deps["suppressed_by_topic"]):
            obs.suppressed_by_topic[(r["System"], r["Topic"])] = int(r["N"])
        for r in _table_rows(self._deps["over_routed_roots"]):
            obs.over_routed_by_root[r["RootId"]] = int(r["OverRoutedQty"])
        for r in _table_rows(self._deps["drift_by_symbol"]):
            if r["Breaks"] > 0:
                obs.symbol_breaks[r["Symbol"]] = obs.symbol_breaks.get(r["Symbol"], 0) + int(r["Breaks"])
        return obs

    def stop(self) -> None:
        for handle in self._handles:
            try:
                handle.stop()
            except Exception:  # noqa: BLE001
                pass
        self._handles = []
        if self.sink is not None:
            try:
                self.sink.stop()
            except Exception:  # noqa: BLE001
                pass


#: Strong references: a collected attachment takes its listeners with it.
_ACTIVE: List[Attachment] = []


def attach(tables: Mapping[str, Any], settings: config.Settings, alert_settings: Optional[AlertSettings] = None,
           clock: Optional[Any] = None, **kwargs: Any) -> Attachment:
    """Attach the break-event listeners and the alert policy to a built lineage; keeps the attachment alive."""
    alert_settings = alert_settings or AlertSettings.from_env(settings=settings)
    if clock is None:
        raise ValueError("attach needs the lineage's clock table (build it once and pass it to both)")
    attachment = Attachment(tables, settings, alert_settings, clock, **kwargs)
    _ACTIVE.append(attachment)
    return attachment
