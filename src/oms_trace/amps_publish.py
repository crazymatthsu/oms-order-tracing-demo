"""Publish order states into AMPS SOW topics: the broker side of the demo (docs/design/11 §7).

::

    python -m oms_trace.amps_publish --uri tcp://localhost:9007/amps/fix --mock [--seed 42] [--families 24] [--no-rebase]
    python -m oms_trace.amps_publish --uri ... --dir /path/with/orders_{a,b,c}.jsonl
    python -m oms_trace.amps_publish --uri ... --delete oms.c.t1 Cc-MB-2-1     # sow_delete one record: an oof downstream
    python -m oms_trace.amps_publish --uri ... --clear                          # sow_delete every record of every topic

Every row goes to its own ``Topic`` as a **FIX 4.2 execution report** by default (``--format fix``,
decision D18): the standard tags of ``config.FIX42_TAGS`` carry the columns of the same name
(37 ``OrderID``, 11 ``ClOrdID``, 55, 54, 38, 14, 151, 6, 39, 150, 35, 60 ``TransactTime``, 34
``MsgSeqNum``, 75 ``Date``), and the two link columns go out in the tags the source is configured
with: ``OMS_AMPS_TAG_MAP_{A,B,C}`` from the environment (the same keys the source reads, so the
demo's two ends agree) or ``--tag-map-a/-b/-c``. ``--format json`` writes the raw contract as a JSON
object instead (``TransactTime`` as epoch nanoseconds). The source-stamped columns (``RecvTs``,
``Topic``, ``KafkaPartition``, ``KafkaOffset``, ``SowDeleted``) never travel. Rows are published in
the book's arrival order so the latest state wins, as on the real feeds. ``--mock`` re-bases the
book to the wall clock (as ``OMS_TRACE_SOURCE=mock`` does) so aging and feed health run live.

Needs ``amps-python-client`` (the ``amps`` extra); nothing here imports ``deephaven``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from oms_trace import amps_sow, config, files, mockdata
from oms_trace.config import INGESTER_COLUMNS, SYSTEMS, SettingsError

Row = Dict[str, Any]
SOH = "\x01"
BEGIN_STRING = "FIX.4.2"
#: The order the standard tags go out in: header, then body (tag 9 and 10 are computed).
_BODY_TAGS = ("35", "34", "52", "37", "11", "41", "17", "150", "39", "55", "54", "38", "14", "151", "6", "60", "75")
_TIME_COLUMNS = ("TransactTime",)


class Encoding:
    """How rows become bodies: the format and, for FIX, each system's complete tag map."""

    def __init__(self, body_format: str = "fix", tag_maps: Optional[Mapping[str, Mapping[str, str]]] = None) -> None:
        if body_format not in ("fix", "json"):
            raise SettingsError(f"format {body_format!r} must be fix or json")
        self.body_format = body_format
        if tag_maps is None:
            _, tag_maps = amps_sow.tag_maps_from_env({})
        self.tag_maps = tag_maps
        if body_format == "fix":
            for system in SYSTEMS:
                amps_sow.check_link_tags(system, self.tag_maps[system])

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None, body_format: Optional[str] = None,
                 overrides: Optional[Mapping[str, str]] = None) -> "Encoding":
        """``OMS_AMPS_FORMAT`` and ``OMS_AMPS_TAG_MAP(_<X>)`` as the source reads them; ``overrides`` is
        ``{"A": "5011=ParentOrderID", ...}`` from the command line, replacing that system's env key."""
        env = dict(os.environ if env is None else env)
        for system, spec in (overrides or {}).items():
            if spec:
                env[amps_sow.TAG_MAP_SYSTEM_ENV.format(system=system)] = spec
        _, tag_maps = amps_sow.tag_maps_from_env(env)
        fmt = (body_format or env.get(amps_sow.FORMAT_ENV, "")).strip().lower() or "fix"
        return cls(fmt, tag_maps)

    def columns_to_tags(self, system: str) -> Dict[str, str]:
        """``{raw column: tag}`` for one system; when two tags feed a column the lowest tag number is used."""
        out: Dict[str, str] = {}
        for tag, column in sorted(self.tag_maps[system].items(), key=lambda kv: (len(kv[0]), kv[0]), reverse=True):
            out[column] = tag
        return out

    def body(self, system: str, row: Mapping[str, Any]) -> str:
        return fix_body(row, self.columns_to_tags(system)) if self.body_format == "fix" else json_body(row)

    def key_filter(self, order_id: str) -> str:
        """The AMPS filter that selects one record: on the SOW key field."""
        return f"/37 = '{order_id}'" if self.body_format == "fix" else f"/OrderID = '{order_id}'"


def json_body(row: Mapping[str, Any]) -> str:
    """The JSON body of one raw row: the contract minus what the source stamps on arrival."""
    return json.dumps({k: v for k, v in row.items() if k not in INGESTER_COLUMNS})


def fix_field(column: str, value: Any) -> str:
    """One column value as FIX text: timestamps as UTCTimestamp, Date as YYYYMMDD, the rest as is."""
    if column in _TIME_COLUMNS:
        return amps_sow.format_fix_time(int(value))
    if column == "Date":
        return str(value).replace("-", "")
    if isinstance(value, float) and value.is_integer() and column != "AvgPx":
        return str(int(value))
    return str(value)


def fix_body(row: Mapping[str, Any], columns_to_tags: Mapping[str, str]) -> str:
    """A FIX 4.2 message (SOH delimited, with 8, 9 and 10) carrying every non-null contract column of the
    row that has a tag: the standard ones plus the system's link tags. ``17`` (ExecID) is derived."""
    fields: List[Tuple[str, str]] = []
    values: Dict[str, str] = {}
    for column, value in row.items():
        if value is None or column in INGESTER_COLUMNS:
            continue
        tag = columns_to_tags.get(column)
        if tag is not None:
            values[tag] = fix_field(column, value)
    values.setdefault("35", mockdata.EXECUTION_REPORT)
    values.setdefault("17", f"{values.get('37', '')}-{values.get('34', '0')}")
    values.setdefault("52", values.get("60", amps_sow.format_fix_time(time.time_ns())))
    for tag in _BODY_TAGS:
        if tag in values:
            fields.append((tag, values.pop(tag)))
    fields.extend(sorted(values.items(), key=lambda kv: int(kv[0])))  # the firm's tags, ascending
    body = "".join(f"{tag}={value}{SOH}" for tag, value in fields)
    head = f"8={BEGIN_STRING}{SOH}9={len(body.encode('utf-8'))}{SOH}"
    checksum = sum((head + body).encode("utf-8")) % 256
    return f"{head}{body}10={checksum:03d}{SOH}"


def messages(rows_by_system: Mapping[str, Sequence[Row]], encoding: Optional[Encoding] = None) -> Iterable[Tuple[str, str]]:
    """``(topic, body)`` for every row, system by system, in arrival order."""
    encoding = encoding or Encoding.from_env()
    for system in SYSTEMS:
        for row in rows_by_system.get(system, ()):
            yield row["Topic"], encoding.body(system, row)


def connect(uri: str, name: str = "oms-trace-publisher") -> Any:
    import AMPS

    client = AMPS.Client(name)
    client.connect(uri)
    client.logon()
    return client


def publish(client: Any, items: Iterable[Tuple[str, str]], flush_every: int = 1000) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    n = 0
    for topic, body in items:
        client.publish(topic, body)
        counts[topic] = counts.get(topic, 0) + 1
        n += 1
        if n % flush_every == 0:
            client.publish_flush()
    client.publish_flush()
    return counts


def delete(client: Any, topic: str, order_id: Optional[str] = None, encoding: Optional[Encoding] = None) -> str:
    """``sow_delete`` one record (by its key, tag 37 / ``OrderID``) or, without an id, every record of the topic."""
    filter_ = (encoding or Encoding.from_env()).key_filter(order_id) if order_id else "1=1"
    client.sow_delete(topic, filter_)
    return filter_


def mock_rows(seed: int, families: int, rebase: bool) -> Tuple[Mapping[str, List[Row]], List[str]]:
    book = mockdata.generate(seed=seed, families=families)
    if rebase:
        from oms_trace.app import _rebase

        _rebase(book, time.time_ns())
    topics = sorted({r["Topic"] for r in book.all_rows()})
    return book.rows, topics


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="oms_trace.amps_publish", description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", required=True, help="AMPS URI, e.g. tcp://localhost:9007/amps/fix")
    parser.add_argument("--client-name", default="oms-trace-publisher")
    parser.add_argument("--format", choices=("fix", "json"), default=None,
                        help="body format (default: OMS_AMPS_FORMAT, else fix); the URI's message type must agree")
    for system in SYSTEMS:
        parser.add_argument(f"--tag-map-{system.lower()}", metavar="TAG=Column,...", default=None,
                            help=f"system {system}'s link tags (default: OMS_AMPS_TAG_MAP_{system})")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--mock", action="store_true", help="publish the scenario book (oms_trace.mockdata)")
    source.add_argument("--dir", help="publish orders_{a,b,c}.jsonl from this directory")
    source.add_argument("--delete", nargs=2, metavar=("TOPIC", "ORDER_ID"), help="sow_delete one record")
    source.add_argument("--clear", action="store_true", help="sow_delete every record of every mock topic")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--families", type=int, default=24)
    parser.add_argument("--no-rebase", action="store_true", help="keep the book's own timestamps (2026-10-26)")
    args = parser.parse_args(argv)

    encoding = Encoding.from_env(body_format=args.format,
                                 overrides={s: getattr(args, f"tag_map_{s.lower()}") for s in SYSTEMS})
    if encoding.body_format == "fix":
        tags = "; ".join(f"{s}: " + ", ".join(f"{c}->{t}" for c, t in sorted(encoding.columns_to_tags(s).items())
                                              if c in config.LINK_COLUMNS_BY_SYSTEM[s]) for s in SYSTEMS)
        print(f"[amps-publish] FIX 4.2 bodies, link tags {tags}")
    client = connect(args.uri, args.client_name)
    try:
        if args.delete:
            filter_ = delete(client, args.delete[0], args.delete[1], encoding)
            print(f"[amps-publish] sow_delete {args.delete[0]} {filter_}")
            return 0
        if args.clear:
            for topic in sorted({t for ts in mockdata.TOPICS.values() for t in ts} | {mockdata.TOPIC_C_STALE, mockdata.TOPIC_B_CATCHUP}):
                delete(client, topic, encoding=encoding)
                print(f"[amps-publish] sow_delete {topic} (all)")
            return 0
        if args.dir:
            rows = files.rows_from_dir(args.dir)
        else:
            rows, _ = mock_rows(args.seed, args.families, rebase=not args.no_rebase)
        counts = publish(client, messages(rows, encoding))
        for topic, n in sorted(counts.items()):
            print(f"[amps-publish] {topic}: {n} messages")
        print(f"[amps-publish] {sum(counts.values())} messages on {len(counts)} topics -> {args.uri}")
        return 0
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    sys.exit(main())
