"""Publish order states into AMPS SOW topics: the broker side of the demo (docs/design/11 §7).

::

    python -m oms_trace.amps_publish --uri tcp://localhost:9007/amps/json --mock [--seed 42] [--families 24] [--no-rebase]
    python -m oms_trace.amps_publish --uri ... --dir /path/with/orders_{a,b,c}.jsonl
    python -m oms_trace.amps_publish --uri ... --delete oms.c.t1 Cc-MB-2-1     # sow_delete one record: an oof downstream
    python -m oms_trace.amps_publish --uri ... --clear                          # sow_delete every record of every topic

Every row goes to its own ``Topic`` as a JSON body of the raw contract minus the source-stamped
columns (``RecvTs``, ``Topic``, ``KafkaPartition``, ``KafkaOffset``, ``SowDeleted``); ``Date``
stays in the body (the ``MID`` scenario), ``EventTs`` is epoch nanoseconds. Rows are published in
the book's arrival order so the latest state wins, as on the real feeds. ``--mock`` re-bases the
book to the wall clock (as ``OMS_TRACE_SOURCE=mock`` does) so aging and feed health run live.

Needs ``amps-python-client`` (the ``amps`` extra); nothing here imports ``deephaven``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from oms_trace import config, files, mockdata
from oms_trace.config import INGESTER_COLUMNS, SYSTEMS

Row = Dict[str, Any]


def sow_body(row: Mapping[str, Any]) -> str:
    """The JSON body of one raw row: the contract minus what the source stamps on arrival."""
    return json.dumps({k: v for k, v in row.items() if k not in INGESTER_COLUMNS})


def messages(rows_by_system: Mapping[str, Sequence[Row]]) -> Iterable[Tuple[str, str]]:
    """``(topic, body)`` for every row, system by system, in arrival order."""
    for system in SYSTEMS:
        for row in rows_by_system.get(system, ()):
            yield row["Topic"], sow_body(row)


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


def delete(client: Any, topic: str, order_id: Optional[str] = None) -> None:
    """``sow_delete`` one record (``/OrderId = '...'``) or, without an id, every record of the topic."""
    filter_ = f"/OrderId = '{order_id}'" if order_id else "1=1"
    client.sow_delete(topic, filter_)


def mock_rows(seed: int, families: int, rebase: bool) -> Tuple[Mapping[str, List[Row]], List[str]]:
    book = mockdata.generate(seed=seed, families=families)
    if rebase:
        from oms_trace.app import _rebase

        _rebase(book, time.time_ns())
    topics = sorted({r["Topic"] for r in book.all_rows()})
    return book.rows, topics


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="oms_trace.amps_publish", description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", required=True, help="AMPS URI, e.g. tcp://localhost:9007/amps/json")
    parser.add_argument("--client-name", default="oms-trace-publisher")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--mock", action="store_true", help="publish the scenario book (oms_trace.mockdata)")
    source.add_argument("--dir", help="publish orders_{a,b,c}.jsonl from this directory")
    source.add_argument("--delete", nargs=2, metavar=("TOPIC", "ORDER_ID"), help="sow_delete one record")
    source.add_argument("--clear", action="store_true", help="sow_delete every record of every mock topic")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--families", type=int, default=24)
    parser.add_argument("--no-rebase", action="store_true", help="keep the book's own timestamps (2026-10-26)")
    args = parser.parse_args(argv)

    client = connect(args.uri, args.client_name)
    try:
        if args.delete:
            delete(client, args.delete[0], args.delete[1])
            print(f"[amps-publish] sow_delete {args.delete[0]} /OrderId = {args.delete[1]!r}")
            return 0
        if args.clear:
            for topic in sorted({t for ts in mockdata.TOPICS.values() for t in ts} | {mockdata.TOPIC_C_STALE, mockdata.TOPIC_B_CATCHUP}):
                delete(client, topic)
                print(f"[amps-publish] sow_delete {topic} (all)")
            return 0
        if args.dir:
            rows = files.rows_from_dir(args.dir)
        else:
            rows, _ = mock_rows(args.seed, args.families, rebase=not args.no_rebase)
        counts = publish(client, messages(rows))
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
