"""Break transitions, the alert policy and the HTTP sink (docs/design/03 §4.5, 04 §7, 08 §2).

Pure python; the engine glue is :mod:`oms_trace.events`. Three things live here:

* **Transitions.** :func:`gap_transitions` / :func:`link_transitions` diff two lineage states
  into ``break_events`` rows (``Ts, Level, System, OrderId, RootId, Edge, From, To, FillGap,
  GapAgeSec``): a gap appearing is ``NONE -> kind``, a gap closing ``kind -> NONE``, a child
  arriving unlinked ``NEW -> UNRESOLVED``, its parent landing ``UNRESOLVED -> LINKED``. On the
  engine the same rows come out of a table listener (added / modified / removed rows).
* **The alert policy.** :class:`AlertPolicy` evaluates the rules of doc 08 §2 on an
  :class:`Observation` (what the rules look at, extracted from the oracle's result here and
  from the live tables in ``oms_trace.events``) each clock tick, with persistence ("for more
  than N minutes"), escalation (``WARN`` to ``PAGE``), dedupe (one alert per key per crossing),
  summaries, re-alerts on growth, and a ``CLEAR`` when a condition ends. Deterministic in
  ``now``: the oracle and the engine run the same code on the same observations.
* **The sink.** :class:`HttpSink` posts one JSON document per alert row to the one URL (Q9),
  retries once, and reports the result for ``alerts.Posted``. The table is the record either
  way.

Every alert row is also a ``break_events`` row (:func:`alert_event`) so it lands in
``OMS_TRACE.DriftEvents`` (doc 03 §4.5).
"""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, time as dtime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

from oms_trace import config
from oms_trace.config import EDGES, SYSTEMS, SettingsError

Row = Dict[str, Any]
SEC = config.SECOND_NANOS

#: ``break_events`` (doc 03 §4.5): one row per node whose BreakKind / ParentState changed.
EVENT_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("Ts", "Instant"), ("Level", "String"), ("System", "String"), ("OrderId", "String"), ("RootId", "String"),
    ("Edge", "String"), ("From", "String"), ("To", "String"), ("FillGap", "long"), ("GapAgeSec", "long"),
)
#: ``alerts`` (doc 03 §4.5): one row per threshold crossing; Posted is joined from the sink's results.
ALERT_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("AlertId", "long"), ("Ts", "Instant"), ("Metric", "String"), ("Key", "String"), ("Value", "long"),
    ("Threshold", "long"), ("Severity", "String"), ("Message", "String"),
)
POST_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("AlertId", "long"), ("Posted", "String"), ("PostedTs", "Instant"), ("Attempts", "long"),
)
SEVERITIES = ("WARN", "PAGE", "CLEAR")
#: The edge on which a level is the child (an unresolved node's edge).
CHILD_EDGE: Dict[str, str] = {e.child_level: e.name for e in EDGES}
ALERT_LEVEL = "ALERT"
NO_SINK = "NO_SINK"

# ---------------------------------------------------------------------------------------------
# settings
# ---------------------------------------------------------------------------------------------

URL_ENV = "OMS_TRACE_ALERT_URL"
AUTH_HEADER_ENV = "OMS_TRACE_ALERT_AUTH_HEADER"
MARKET_HOURS_ENV = "OMS_TRACE_MARKET_HOURS"
_HOURS = re.compile(r"^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$")


def _int(env: Mapping[str, str], key: str, default: int, minimum: int = 0) -> int:
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


def _float(env: Mapping[str, str], key: str, default: float, minimum: float = 0.0) -> float:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise SettingsError(f"{key}={raw!r} is not a number") from exc
    if value < minimum:
        raise SettingsError(f"{key}={value} must be >= {minimum}")
    return value


def parse_market_hours(raw: str) -> Optional[Tuple[dtime, dtime]]:
    """``09:30-16:00`` -> (open, close); empty means always open."""
    raw = raw.strip()
    if not raw:
        return None
    m = _HOURS.match(raw)
    if not m:
        raise SettingsError(f"{MARKET_HOURS_ENV}={raw!r} must look like 09:30-16:00 (empty = always)")
    h1, m1, h2, m2 = (int(x) for x in m.groups())
    try:
        open_, close = dtime(h1, m1), dtime(h2, m2)
    except ValueError as exc:
        raise SettingsError(f"{MARKET_HOURS_ENV}={raw!r}: {exc}") from exc
    if close <= open_:
        raise SettingsError(f"{MARKET_HOURS_ENV}={raw!r}: the close must be after the open")
    return open_, close


@dataclass(frozen=True)
class AlertSettings:
    """The thresholds of doc 08 §2 ("defaults; tune after the first week") and the sink."""

    url: str = ""                              # OMS_TRACE_ALERT_URL; empty: table only
    auth_header: str = ""                      # "Name: value", from the secret store
    timeout_sec: float = 5.0
    retries: int = 1                           # "retried once"
    retry_delay_sec: float = 1.0
    max_pending: int = 10_000
    ring: int = 10_000                         # rows kept in the worker's alerts / break_history
    market_hours: str = "09:30-16:00"          # channel and system staleness alert in these hours only
    tz: str = "America/New_York"
    unresolved_alert_sec: int = 3600           # the Alerted threshold (config.Settings)
    unresolved_page_count: int = 100
    unresolved_page_age_sec: int = 4 * 3600
    unresolved_summary_sec: int = 900
    ratio_persist_sec: int = 600               # CParentsMinusBChildren != 0 for this long
    ratio_page: int = -100
    channel_stale_page_sec: int = 600
    channel_catchup_page_sec: int = 3600
    drift_persist_sec: int = 300               # over-routed and drift quantities > 0 for this long
    drift_page_qty: int = 0                    # 0: no PAGE level until the desks agree a number
    drift_growth: float = 2.0                  # re-alert when the quantity doubles
    over_routed_page_qty: int = 0
    symbols_page: int = 10
    system_warn_sec: int = 60
    system_page_sec: int = 300

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None, settings: Optional[config.Settings] = None) -> "AlertSettings":
        env = os.environ if env is None else env
        settings = settings or config.load_settings(env)
        hours = env.get(MARKET_HOURS_ENV, "09:30-16:00")
        parse_market_hours(hours)
        header = env.get(AUTH_HEADER_ENV, "").strip()
        if header and ":" not in header:
            raise SettingsError(f"{AUTH_HEADER_ENV} must be 'Header-Name: value'")
        return cls(
            url=env.get(URL_ENV, "").strip(),
            auth_header=header,
            timeout_sec=_float(env, "OMS_TRACE_ALERT_TIMEOUT_SEC", 5.0),
            retries=_int(env, "OMS_TRACE_ALERT_RETRIES", 1),
            retry_delay_sec=_float(env, "OMS_TRACE_ALERT_RETRY_DELAY_SEC", 1.0),
            max_pending=_int(env, "OMS_TRACE_ALERT_MAX_PENDING", 10_000, 1),
            ring=_int(env, "OMS_TRACE_ALERT_RING", 10_000, 1),
            market_hours=hours.strip(),
            tz=settings.tz,
            unresolved_alert_sec=settings.unresolved_alert_sec,
            unresolved_page_count=_int(env, "OMS_TRACE_ALERT_UNRESOLVED_PAGE_COUNT", 100, 1),
            unresolved_page_age_sec=_int(env, "OMS_TRACE_ALERT_UNRESOLVED_PAGE_AGE_SEC", 4 * 3600, 1),
            unresolved_summary_sec=_int(env, "OMS_TRACE_ALERT_UNRESOLVED_SUMMARY_SEC", 900),
            ratio_persist_sec=_int(env, "OMS_TRACE_ALERT_RATIO_PERSIST_SEC", 600),
            ratio_page=-_int(env, "OMS_TRACE_ALERT_RATIO_PAGE", 100, 1),
            channel_stale_page_sec=_int(env, "OMS_TRACE_ALERT_CHANNEL_STALE_PAGE_SEC", 600),
            channel_catchup_page_sec=_int(env, "OMS_TRACE_ALERT_CHANNEL_CATCHUP_PAGE_SEC", 3600),
            drift_persist_sec=_int(env, "OMS_TRACE_ALERT_DRIFT_PERSIST_SEC", 300),
            drift_page_qty=_int(env, "OMS_TRACE_ALERT_DRIFT_PAGE_QTY", 0),
            drift_growth=_float(env, "OMS_TRACE_ALERT_DRIFT_GROWTH", 2.0),
            over_routed_page_qty=_int(env, "OMS_TRACE_ALERT_OVER_ROUTED_PAGE_QTY", 0),
            symbols_page=_int(env, "OMS_TRACE_ALERT_SYMBOLS_PAGE", 10, 1),
            system_warn_sec=_int(env, "OMS_TRACE_ALERT_SYSTEM_WARN_SEC", 60),
            system_page_sec=_int(env, "OMS_TRACE_ALERT_SYSTEM_PAGE_SEC", 300),
        )

    def headers(self) -> Dict[str, str]:
        out = {"Content-Type": "application/json"}
        if self.auth_header:
            name, _, value = self.auth_header.partition(":")
            out[name.strip()] = value.strip()
        return out

    def in_market_hours(self, now_ns: int) -> bool:
        window = parse_market_hours(self.market_hours)
        if window is None:
            return True
        local = datetime.fromtimestamp(now_ns // SEC, ZoneInfo(self.tz)).time()
        return window[0] <= local < window[1]


# ---------------------------------------------------------------------------------------------
# transitions
# ---------------------------------------------------------------------------------------------


def _gap_event(g: Mapping[str, Any], from_: str, to: str, now: int) -> Row:
    return {"Ts": now, "Level": g["Level"], "System": g["System"], "OrderId": g["OrderId"], "RootId": g.get("RootId"),
            "Edge": g["Edge"], "From": from_, "To": to, "FillGap": g.get("FillGap"), "GapAgeSec": g.get("GapAgeSec")}


def _link_event(u: Mapping[str, Any], from_: str, to: str, now: int) -> Row:
    return {"Ts": now, "Level": u["Level"], "System": u["System"], "OrderId": u["OrderId"], "RootId": u.get("RootId"),
            "Edge": CHILD_EDGE[u["Level"]], "From": from_, "To": to, "FillGap": None, "GapAgeSec": u.get("AgeSec")}


def gap_transitions(prev: Mapping[Any, Mapping[str, Any]], cur: Mapping[Any, Mapping[str, Any]], now: int) -> List[Row]:
    """``break_events`` rows between two ``gaps`` states keyed (Edge, OrderId)."""
    out: List[Row] = []
    for key, g in cur.items():
        p = prev.get(key)
        if p is None:
            out.append(_gap_event(g, "NONE", g["BreakKind"], now))
        elif p["BreakKind"] != g["BreakKind"]:
            out.append(_gap_event(g, p["BreakKind"], g["BreakKind"], now))
    for key, p in prev.items():
        if key not in cur:
            out.append(_gap_event(p, p["BreakKind"], "NONE", now))
    return out


def link_transitions(prev: Mapping[Any, Mapping[str, Any]], cur: Mapping[Any, Mapping[str, Any]], now: int) -> List[Row]:
    """``break_events`` rows between two ``unresolved_links`` states keyed (Level, OrderId)."""
    out: List[Row] = []
    for key, u in cur.items():
        p = prev.get(key)
        if p is None:
            out.append(_link_event(u, "NEW", u["ParentState"], now))
        elif p["ParentState"] != u["ParentState"]:
            out.append(_link_event(u, p["ParentState"], u["ParentState"], now))
    for key, p in prev.items():
        if key not in cur:
            out.append(_link_event(p, p["ParentState"], "LINKED", now))
    return out


def event_key(e: Mapping[str, Any]) -> Tuple[Any, ...]:
    """The comparable identity of an event (the oracle and the engine may order a cycle differently)."""
    return (e["Ts"], e["Level"], e["System"], e["OrderId"], e.get("RootId"), e["Edge"], e["From"], e["To"],
            e.get("FillGap"), e.get("GapAgeSec"))


# ---------------------------------------------------------------------------------------------
# observations
# ---------------------------------------------------------------------------------------------


@dataclass
class Observation:
    """What the rules of doc 08 §2 look at on one clock tick."""

    now: int
    #: (System, OrderId) -> AgeSec, for unresolved links past the alert age and not feed-suppressed.
    unresolved: Dict[Tuple[str, str], int] = field(default_factory=dict)
    c_parents_minus_b_children: int = 0
    #: (System, Topic) -> (TopicState, SilentSec)
    channels: Dict[Tuple[str, str], Tuple[str, int]] = field(default_factory=dict)
    #: (System, Topic) -> number of FEED_STALE gaps the channel suppresses: the parent's channel when it
    #: is not current, each non-current child channel, every non-current channel of a child system that
    #: has no children at all (the system-down case). One gap can count against several channels.
    suppressed_by_topic: Dict[Tuple[str, str], int] = field(default_factory=dict)
    #: RootId -> OverRoutedQty (> 0 only).
    over_routed_by_root: Dict[str, int] = field(default_factory=dict)
    #: (Edge, "MissingDownstreamQty" | "UnbookedUpstreamQty") -> quantity (> 0 only).
    drift_qty: Dict[Tuple[str, str], int] = field(default_factory=dict)
    #: Symbol -> breaks over every edge (> 0 only).
    symbol_breaks: Dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """A comparable form (the engine's extraction is checked against the oracle's)."""
        return {
            "now": self.now, "unresolved": dict(self.unresolved), "ratio": self.c_parents_minus_b_children,
            "channels": dict(self.channels), "suppressed": {k: v for k, v in self.suppressed_by_topic.items() if v},
            "over_routed": dict(self.over_routed_by_root), "drift_qty": dict(self.drift_qty),
            "symbols": dict(self.symbol_breaks),
        }


def observe_result(res: Any, now: int) -> Observation:
    """An :class:`Observation` from an :class:`oms_trace.reference.Result` (the oracle side)."""
    obs = Observation(now=now)
    for u in res.unresolved_links.values():
        if u["Alerted"]:
            obs.unresolved[(u["System"], u["OrderId"])] = int(u["AgeSec"])
    obs.c_parents_minus_b_children = int(res.trace_stats.get(("CParentsMinusBChildren", ""), 0))
    for (system, topic), fh in res.feed_health.items():
        obs.channels[(system, topic)] = (fh["TopicState"], int(fh["SilentSec"]))
    def bump(key: Tuple[str, str]) -> None:
        obs.suppressed_by_topic[key] = obs.suppressed_by_topic.get(key, 0) + 1

    for g in res.feed_suppressed.values():
        if g["ParentFeed"] is not None and g["ParentFeed"] != "CURRENT":
            bump((g["System"], g["Topic"]))
        if g["ChildCount"] == 0:
            if g["ChildSideStale"]:
                for (system, topic), fh in res.feed_health.items():
                    if system == g["ChildSystem"] and fh["StaleInt"]:
                        bump((system, topic))
        else:
            for topic in g["ChildTopics"]:
                fh = res.feed_health.get((g["ChildSystem"], topic))
                if fh is not None and fh["StaleInt"]:
                    bump((g["ChildSystem"], topic))
    for rid, row in res.drift_by_a_order.items():
        if row["OverRoutedQty"] > 0:
            obs.over_routed_by_root[rid] = int(row["OverRoutedQty"])
    for e in EDGES:
        for metric in ("MissingDownstreamQty", "UnbookedUpstreamQty"):
            qty = int(res.trace_stats.get((metric, e.name), 0))
            if qty > 0:
                obs.drift_qty[(e.name, metric)] = qty
    for (symbol, _side, _edge), row in res.drift_by_symbol.items():
        if row["Breaks"] > 0:
            obs.symbol_breaks[symbol] = obs.symbol_breaks.get(symbol, 0) + int(row["Breaks"])
    return obs


# ---------------------------------------------------------------------------------------------
# the policy
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Alert:
    id: int
    ts: int
    metric: str
    key: str
    value: int
    threshold: int
    severity: str
    message: str
    system: str = ""      # for the break_events row
    previous: str = ""    # the severity before this crossing ("" for the first)

    def row(self) -> Row:
        return {"AlertId": self.id, "Ts": self.ts, "Metric": self.metric, "Key": self.key, "Value": self.value,
                "Threshold": self.threshold, "Severity": self.severity, "Message": self.message}

    def json(self) -> str:
        return json.dumps({
            "source": "oms-trace", "alert_id": self.id,
            "ts": datetime.fromtimestamp(self.ts / SEC, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "metric": self.metric, "key": self.key, "value": self.value, "threshold": self.threshold,
            "severity": self.severity, "message": self.message,
        })


def alert_event(alert: Alert) -> Row:
    """The ``break_events`` row an alert also lands as: Level ALERT, Edge = Metric, OrderId = Key."""
    return {"Ts": alert.ts, "Level": ALERT_LEVEL, "System": alert.system, "OrderId": alert.key, "RootId": None,
            "Edge": alert.metric, "From": alert.previous, "To": alert.severity, "FillGap": alert.value,
            "GapAgeSec": alert.threshold}


@dataclass
class Reading:
    """One rule's view of one key on one tick: the condition holds at this severity."""

    value: int
    threshold: int
    severity: str          # WARN or PAGE
    message: str
    system: str = ""
    tag: str = ""          # a state label; a change re-emits (one alert per topic per state change)


@dataclass(frozen=True)
class Rule:
    metric: str
    persist_sec: int = 0        # the condition must hold this long before the first alert
    summary_sec: int = 0        # re-emit while the condition holds, every summary_sec (0: never)
    growth: float = 0.0         # re-emit when Value >= growth x the last alerted Value (0: never)
    clear: bool = True          # emit CLEAR when the condition ends


@dataclass
class _Track:
    since: int
    level: Optional[str] = None
    alerted_value: int = 0
    last_emit: int = 0
    tag: str = ""
    threshold: int = 0
    system: str = ""


class AlertPolicy:
    """The rules of doc 08 §2 as a deterministic state machine over observations.

    ``observe`` returns the alert rows of the tick, ids increasing. Feed it every clock tick,
    in order; the oracle and the engine share one instance type, so equal observations give
    equal alerts.
    """

    def __init__(self, settings: AlertSettings) -> None:
        self.s = settings
        self._tracks: Dict[Tuple[str, str], _Track] = {}
        self._channel_since: Dict[Tuple[str, str], Tuple[str, int]] = {}  # (System, Topic) -> (state, since)
        self._next_id = 1
        self.rules: Dict[str, Rule] = {
            "unresolved_link": Rule("unresolved_link"),
            "unresolved_links": Rule("unresolved_links", summary_sec=settings.unresolved_summary_sec),
            "c_parents_vs_b_children": Rule("c_parents_vs_b_children", persist_sec=settings.ratio_persist_sec),
            "channel_health": Rule("channel_health"),
            "over_routed": Rule("over_routed", persist_sec=settings.drift_persist_sec),
            "drift_qty": Rule("drift_qty", persist_sec=settings.drift_persist_sec, growth=settings.drift_growth),
            "drift_symbols": Rule("drift_symbols"),
            "drift_symbol_count": Rule("drift_symbol_count"),
            "stale_system": Rule("stale_system"),
        }

    # -- readings per rule ----------------------------------------------------------------

    def readings(self, obs: Observation) -> Dict[str, Dict[str, Reading]]:
        s = self.s
        out: Dict[str, Dict[str, Reading]] = {name: {} for name in self.rules}
        # unresolved links older than the alert age: per node, and per system with a page level
        per_system: Dict[str, List[int]] = {}
        for (system, order_id), age in obs.unresolved.items():
            out["unresolved_link"][f"{system}:{order_id}"] = Reading(
                age, s.unresolved_alert_sec, "WARN", f"{system} {order_id}: parent not found for {age // 60} min", system)
            per_system.setdefault(system, []).append(age)
        for system, ages in per_system.items():
            count, oldest = len(ages), max(ages)
            page = count > s.unresolved_page_count or oldest > s.unresolved_page_age_sec
            out["unresolved_links"][system] = Reading(
                count, s.unresolved_page_count, "PAGE" if page else "WARN",
                f"{system}: {count} unresolved links past {s.unresolved_alert_sec // 60} min, oldest {oldest // 60} min", system)
        # C parents versus B children
        ratio = obs.c_parents_minus_b_children
        if ratio != 0:
            out["c_parents_vs_b_children"]["fleet"] = Reading(
                ratio, s.ratio_page, "PAGE" if ratio < s.ratio_page else "WARN",
                f"C parents minus B children = {ratio} ({'B children never reaching C' if ratio < 0 else 'C parents without a B child'})")
        # channels and systems, market hours only
        market = s.in_market_hours(obs.now)
        silence: Dict[str, int] = {}
        for (system, topic), (state, silent) in obs.channels.items():
            silence[system] = min(silence.get(system, silent), silent)
            if state == "CURRENT":
                self._channel_since.pop((system, topic), None)
                continue
            prev = self._channel_since.get((system, topic))
            since = prev[1] if prev and prev[0] == state else obs.now
            self._channel_since[(system, topic)] = (state, since)
            if not market:
                continue
            duration = silent if state == "STALE" else int((obs.now - since) // SEC)
            limit = s.channel_stale_page_sec if state == "STALE" else s.channel_catchup_page_sec
            suppressed = obs.suppressed_by_topic.get((system, topic), 0)
            out["channel_health"][f"{system}:{topic}"] = Reading(
                duration, limit, "PAGE" if duration > limit else "WARN",
                f"{system} {topic} {state} for {duration} s; {suppressed} gaps suppressed", system, tag=state)
        if market:
            for system, silent in silence.items():
                if silent > s.system_warn_sec:
                    out["stale_system"][system] = Reading(
                        silent, s.system_page_sec, "PAGE" if silent > s.system_page_sec else "WARN",
                        f"{system}: no message on any channel for {silent} s", system)
        # over-routed open quantity per root
        for rid, qty in obs.over_routed_by_root.items():
            page = s.over_routed_page_qty > 0 and qty > s.over_routed_page_qty
            out["over_routed"][rid] = Reading(qty, s.over_routed_page_qty, "PAGE" if page else "WARN",
                                              f"root {rid}: {qty} over-routed open shares", "A")
        # drift quantity per edge and direction
        for (edge, metric), qty in obs.drift_qty.items():
            page = s.drift_page_qty > 0 and qty > s.drift_page_qty
            out["drift_qty"][f"{edge}:{metric}"] = Reading(qty, s.drift_page_qty, "PAGE" if page else "WARN",
                                                           f"{edge}: {qty} shares {metric[:-3]}")
        # drift by symbol: any new symbol, and too many symbols
        for symbol, breaks in obs.symbol_breaks.items():
            out["drift_symbols"][symbol] = Reading(breaks, 0, "WARN", f"{symbol}: {breaks} drift breaks")
        if len(obs.symbol_breaks) > s.symbols_page:
            out["drift_symbol_count"]["fleet"] = Reading(len(obs.symbol_breaks), s.symbols_page, "PAGE",
                                                         f"{len(obs.symbol_breaks)} symbols with drift breaks")
        return out

    # -- the state machine ---------------------------------------------------------------------

    def observe(self, obs: Observation) -> List[Alert]:
        alerts: List[Alert] = []
        readings = self.readings(obs)
        for name, rule in self.rules.items():
            seen = readings[name]
            for key, r in seen.items():
                alert = self._advance(rule, key, r, obs.now)
                if alert is not None:
                    alerts.append(alert)
            for (metric, key), track in list(self._tracks.items()):
                if metric != name or key in seen:
                    continue
                if track.level is not None and rule.clear:
                    alerts.append(self._emit(obs.now, name, key, 0, track.threshold, "CLEAR",
                                             f"{key}: {name} cleared", track.system, track.level))
                del self._tracks[(metric, key)]
        return alerts

    def _advance(self, rule: Rule, key: str, r: Reading, now: int) -> Optional[Alert]:
        tk = self._tracks.get((rule.metric, key))
        if tk is None:
            tk = self._tracks[(rule.metric, key)] = _Track(since=now)
        tk.threshold, tk.system = r.threshold, r.system
        if now - tk.since < rule.persist_sec * SEC:
            return None
        previous = tk.level or ""
        first = tk.level is None
        escalate = r.severity == "PAGE" and tk.level == "WARN"
        retag = bool(r.tag) and tk.level is not None and r.tag != tk.tag  # one alert per topic per state change
        if first or escalate or retag:
            tk.level = r.severity
            tk.tag, tk.alerted_value, tk.last_emit = r.tag, r.value, now
            return self._emit(now, rule.metric, key, r.value, r.threshold, tk.level, r.message, r.system, previous)
        if rule.growth and r.value > tk.alerted_value and r.value >= rule.growth * tk.alerted_value:
            tk.alerted_value, tk.last_emit = r.value, now
            return self._emit(now, rule.metric, key, r.value, r.threshold, tk.level, r.message + " (grew)", r.system, previous)
        if rule.summary_sec and now - tk.last_emit >= rule.summary_sec * SEC:
            tk.last_emit = now
            return self._emit(now, rule.metric, key, r.value, r.threshold, tk.level, r.message + " (summary)", r.system, previous)
        return None

    def _emit(self, now: int, metric: str, key: str, value: int, threshold: int, severity: str, message: str,
              system: str, previous: str) -> Alert:
        alert = Alert(self._next_id, now, metric, key, int(value), int(threshold), severity, message, system, previous)
        self._next_id += 1
        return alert

    def open_alerts(self) -> Dict[Tuple[str, str], str]:
        """(metric, key) -> level, for everything currently alerted."""
        return {k: t.level for k, t in self._tracks.items() if t.level is not None}


# ---------------------------------------------------------------------------------------------
# the sink
# ---------------------------------------------------------------------------------------------


class HttpSink:
    """One JSON POST per alert to the one URL (Q9); retried once; the result goes to ``on_result``.

    ``on_result(alert_id, posted, attempts, ts_ns)`` with ``posted`` an HTTP status (``"200"``),
    ``"HTTP 503"``, ``"ERR <reason>"`` or ``"DROPPED"`` (the queue was full). Runs on its own
    daemon thread; ``post`` never blocks the caller.
    """

    def __init__(self, settings: AlertSettings, on_result: Optional[Callable[[int, str, int, int], None]] = None,
                 clock: Callable[[], int] = time.time_ns) -> None:
        if not settings.url:
            raise ValueError(f"{URL_ENV} is empty: no sink")
        self.s = settings
        self._on_result = on_result or (lambda *_: None)
        self._clock = clock
        self._queue: "queue.Queue[Optional[Alert]]" = queue.Queue(maxsize=settings.max_pending)
        self.counts = {"posted": 0, "failed": 0, "dropped": 0}
        self.last_error: Optional[str] = None
        self._thread = threading.Thread(target=self._run, name="oms-trace-alert-sink", daemon=True)
        self._thread.start()

    def post(self, alert: Alert) -> bool:
        try:
            self._queue.put_nowait(alert)
            return True
        except queue.Full:
            self.counts["dropped"] += 1
            self._report(alert.id, "DROPPED", 0)
            return False

    def stop(self, timeout: float = 5.0) -> None:
        self._queue.put(None)
        self._thread.join(timeout)

    def _report(self, alert_id: int, posted: str, attempts: int) -> None:
        try:
            self._on_result(alert_id, posted, attempts, self._clock())
        except Exception:  # noqa: BLE001 - a result callback must never kill the sink
            traceback.print_exc()

    def _run(self) -> None:
        while True:
            alert = self._queue.get()
            if alert is None:
                return
            posted, attempts = self._deliver(alert)
            self.counts["posted" if posted.isdigit() else "failed"] += 1
            self._report(alert.id, posted, attempts)

    def _deliver(self, alert: Alert) -> Tuple[str, int]:
        body = alert.json().encode("utf-8")
        result = "ERR not attempted"
        attempts = 0
        for attempt in range(self.s.retries + 1):
            attempts = attempt + 1
            try:
                request = urllib.request.Request(self.s.url, data=body, headers=self.s.headers(), method="POST")
                with urllib.request.urlopen(request, timeout=self.s.timeout_sec) as response:
                    result = str(response.status)
                return result, attempts
            except urllib.error.HTTPError as exc:
                result = f"HTTP {exc.code}"
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                result = f"ERR {getattr(exc, 'reason', exc)}"
            self.last_error = result
            if attempt < self.s.retries:
                time.sleep(self.s.retry_delay_sec)
        return result, attempts
