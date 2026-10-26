"""The pre-cutover feed validation (docs/design/09 section 6), pure python.

    python -m oms_trace.trace_check --dir /path/with/orders_{a,b,c}.jsonl [--json]
    python -m oms_trace.trace_check --mock                       # the scenario book
    python -m oms_trace.trace_check --export-mock DIR             # write the book as JSON lines

Per system and per hop it reports what the adapters assume: the role split (null parent id =
parent; empty strings must be 0), children carrying no cross-system reference, resolution rates
per hop, cardinality (one B parent per A, one C parent per B child), children per parent,
CumQty monotonicity, LeavesQty <= OrderQty - CumQty, EventTs presence and offset from RecvTs,
parent-before-child arrival share, cross-system lag percentiles, and each topic's longest
silence. The cutover rules are in docs/design/09 section 6.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from oms_trace import config
from oms_trace.config import SECOND_NANOS as SEC

Row = Dict[str, Any]


def _pct(values: List[int], p: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round((len(values) - 1) * p))))
    return values[k] / SEC


def analyze(rows_by_system: Mapping[str, List[Row]], parent_col: str = "ParentOrderId") -> Dict[str, Any]:
    report: Dict[str, Any] = {"systems": {}, "hops": {}, "topics": {}, "verdicts": []}
    last: Dict[str, Dict[str, Row]] = {}
    for system in config.SYSTEMS:
        rows = rows_by_system.get(system, [])
        states: Dict[str, Row] = {}
        history: Dict[str, List[Row]] = defaultdict(list)
        for r in rows:
            states[r["OrderId"]] = r
            history[r["OrderId"]].append(r)
        last[system] = states
        parents = [r for r in states.values() if r.get(parent_col) is None]
        children = [r for r in states.values() if r.get(parent_col) is not None]
        empty_parent_ids = sum(1 for r in children if r.get(parent_col) == "")
        children_with_upstream = sum(1 for r in children if r.get("UpstreamRef") not in (None, ""))
        non_monotonic = 0
        for hist in history.values():
            cums = [h.get("CumQty") or 0 for h in hist]
            if any(b < a for a, b in zip(cums, cums[1:])):
                non_monotonic += 1
        leaves_bad = sum(1 for r in states.values()
                         if (r.get("LeavesQty") or 0) > max((r.get("OrderQty") or 0) - (r.get("CumQty") or 0), 0)
                         and r.get("OrdStatus") != "CANCELED")
        event_missing = sum(1 for r in rows if r.get("EventTs") is None)
        offsets = [abs(r["RecvTs"] - r["EventTs"]) for r in rows if r.get("EventTs") is not None]
        report["systems"][system] = {
            "messages": len(rows), "orders": len(states), "parents": len(parents), "children": len(children),
            "messages_per_order_median": statistics.median([len(h) for h in history.values()]) if history else 0,
            "empty_string_parent_ids": empty_parent_ids,
            "children_with_upstream_ref": children_with_upstream,
            "orders_with_non_monotonic_cum": non_monotonic,
            "orders_with_leaves_above_open": leaves_bad,
            "messages_without_event_ts": event_missing,
            "recv_minus_event_p99_sec": _pct(offsets, 0.99),
        }
        if empty_parent_ids:
            report["verdicts"].append(f"{system}: {empty_parent_ids} child rows carry an empty-string {parent_col} (contract: null means parent)")
        if children_with_upstream:
            report["verdicts"].append(f"{system}: {children_with_upstream} child rows carry UpstreamRef (contract: children carry {parent_col} only)")
        if event_missing:
            report["verdicts"].append(f"{system}: {event_missing} messages without EventTs (OMS_TRACE_AGE_CLOCK=event would fall back to RecvTs)")

    def nodes(level: str) -> Dict[str, Row]:
        system = config.LEVEL_SYSTEM[level]
        states = last[system]
        if level == "A_ROOT":
            return states
        is_parent = level.endswith("_PARENT")
        return {k: r for k, r in states.items() if (r.get(parent_col) is None) == is_parent}

    by_level = {level: nodes(level) for level in config.LEVELS}
    for edge in config.EDGES:
        parents, children = by_level[edge.parent_level], by_level[edge.child_level]
        link_col = config.LEVEL_LINK_COLUMN[edge.child_level]
        refs = [c.get(link_col) for c in children.values()]
        resolved = sum(1 for ref in refs if ref and ref in parents)
        fanout = Counter(ref for ref in refs if ref and ref in parents)
        per_parent = sorted(fanout.values())
        child_first = 0
        lags: List[int] = []
        for c in children.values():
            p = parents.get(c.get(link_col) or "")
            if p is None:
                continue
            if c["RecvTs"] < p["RecvTs"]:
                child_first += 1
            lags.append(c["RecvTs"] - p["RecvTs"])
        report["hops"][edge.name] = {
            "children": len(children), "resolved": resolved,
            "resolution_rate": (resolved / len(children)) if children else None,
            "parents_with_children": len(fanout), "parents": len(parents),
            "children_per_parent_median": statistics.median(per_parent) if per_parent else 0,
            "children_per_parent_max": per_parent[-1] if per_parent else 0,
            "parents_with_more_than_one_child": sum(1 for n in per_parent if n > 1),
            "child_before_parent_share": (child_first / len(lags)) if lags else None,
            "lag_child_minus_parent_sec_p50": _pct(lags, 0.5), "lag_p99": _pct(lags, 0.99),
            "lag_max": (max(lags) / SEC) if lags else None,
        }
        rate = report["hops"][edge.name]["resolution_rate"]
        if rate is not None and rate < 0.999:
            report["verdicts"].append(f"{edge.name}: resolution rate {rate:.4f} < 0.999")
        if edge.expect_one and report["hops"][edge.name]["parents_with_more_than_one_child"]:
            report["verdicts"].append(f"{edge.name}: {report['hops'][edge.name]['parents_with_more_than_one_child']} parents have more than one child on a 1:1 hop")

    for system in config.SYSTEMS:
        by_topic: Dict[str, List[int]] = defaultdict(list)
        for r in rows_by_system.get(system, []):
            by_topic[r.get("Topic") or ""].append(r["RecvTs"])
        for topic, ts in by_topic.items():
            ts = sorted(ts)
            gaps = [b - a for a, b in zip(ts, ts[1:])]
            report["topics"][f"{system}:{topic}"] = {"messages": len(ts), "longest_silence_sec": (max(gaps) / SEC) if gaps else 0.0}
    if not report["verdicts"]:
        report["verdicts"].append("no contract violations found")
    return report


def render(report: Dict[str, Any]) -> str:
    lines = ["== systems =="]
    for system, s in report["systems"].items():
        lines.append(f"{system}: " + ", ".join(f"{k}={v}" for k, v in s.items()))
    lines.append("== hops ==")
    for hop, h in report["hops"].items():
        lines.append(f"{hop}: " + ", ".join(f"{k}={v if not isinstance(v, float) else round(v, 4)}" for k, v in h.items()))
    lines.append("== topics (longest silence, s) ==")
    for topic, t in sorted(report["topics"].items()):
        lines.append(f"{topic}: messages={t['messages']} longest_silence_sec={t['longest_silence_sec']:.1f}")
    lines.append("== verdicts ==")
    lines.extend(f"- {v}" for v in report["verdicts"])
    return "\n".join(lines)


def load_dir(directory: str) -> Dict[str, List[Row]]:
    from oms_trace.app import _files_rows

    return _files_rows(directory)


def export_mock(directory: str, seed: int = 42, families: int = 24) -> None:
    from oms_trace import mockdata

    os.makedirs(directory, exist_ok=True)
    book = mockdata.generate(seed=seed, families=families)
    for system in config.SYSTEMS:
        with open(os.path.join(directory, f"orders_{system.lower()}.jsonl"), "w", encoding="utf-8") as fh:
            for r in book.rows[system]:
                fh.write(json.dumps(r) + "\n")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", help="directory with orders_a.jsonl, orders_b.jsonl, orders_c.jsonl")
    ap.add_argument("--mock", action="store_true", help="analyze the scenario book")
    ap.add_argument("--export-mock", metavar="DIR", help="write the scenario book as JSON lines and exit")
    ap.add_argument("--json", action="store_true", help="print the report as JSON")
    args = ap.parse_args(argv)
    if args.export_mock:
        export_mock(args.export_mock)
        print(f"wrote orders_{{a,b,c}}.jsonl to {args.export_mock}")
        return 0
    if args.mock:
        from oms_trace import mockdata

        rows = mockdata.generate().rows
    elif args.dir:
        rows = load_dir(args.dir)
    else:
        ap.error("one of --dir, --mock, --export-mock is required")
        return 2
    report = analyze(rows)
    print(json.dumps(report, indent=2, default=str) if args.json else render(report))
    return 0 if report["verdicts"] == ["no contract violations found"] else 1


if __name__ == "__main__":
    sys.exit(main())
