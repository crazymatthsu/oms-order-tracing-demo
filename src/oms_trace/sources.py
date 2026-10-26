"""Raw tables for the Deephaven layer.

* ``mock_tables``  the mock book as three in-memory tables with the raw contract's types
* ``clock_table``  a one-row table carrying ``Now`` (tests inject a fixed clock; production uses
  ``time_table`` through :func:`oms_trace.dag.live_clock`)
* ``input_tables`` append-only input tables with the raw contract, for live tests and demos

The Enterprise sources (``db.live_table`` over DIS partitions) live in :mod:`oms_trace.enterprise`.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from deephaven import dtypes, input_table, new_table
from deephaven.column import bool_col, datetime_col, double_col, int_col, long_col, string_col
from deephaven.table import Table
from deephaven.time import to_j_instant

from oms_trace import config

_DTYPES = {
    "String": dtypes.string,
    "long": dtypes.long,
    "int": dtypes.int32,
    "double": dtypes.double,
    "Instant": dtypes.Instant,
    "boolean": dtypes.bool_,
}


def raw_column_definitions() -> Dict[str, Any]:
    """The raw contract as Deephaven column definitions (for input tables and schemas)."""
    return {name: _DTYPES[kind] for name, kind in config.RAW_COLUMNS}


def _instants(values: Iterable[Optional[int]]) -> List[Any]:
    return [None if v is None else to_j_instant(int(v)) for v in values]


def dtype(kind: str) -> Any:
    """A contract kind (``String``, ``long``, ``int``, ``double``, ``Instant``, ``boolean``) as a Deephaven dtype."""
    return _DTYPES[kind]


def typed_columns(spec: Iterable[Tuple[str, str]], rows: List[Mapping[str, Any]]) -> List[Any]:
    """Typed columns for ``rows`` following a ``(name, kind)`` spec (timestamps as epoch nanoseconds)."""
    cols = []
    for name, kind in spec:
        values = [r.get(name) for r in rows]
        if kind == "String":
            cols.append(string_col(name, [None if v is None else str(v) for v in values]))
        elif kind == "long":
            cols.append(long_col(name, [None if v is None else int(v) for v in values]))
        elif kind == "int":
            cols.append(int_col(name, [None if v is None else int(v) for v in values]))
        elif kind == "double":
            cols.append(double_col(name, [None if v is None else float(v) for v in values]))
        elif kind == "Instant":
            cols.append(datetime_col(name, _instants(values)))
        elif kind == "boolean":
            cols.append(bool_col(name, [None if v is None else bool(v) for v in values]))
        else:  # pragma: no cover - the contract only has these kinds
            raise ValueError(f"unknown raw column kind {kind} for {name}")
    return cols


def raw_columns(rows: List[Mapping[str, Any]]) -> List[Any]:
    """The raw contract's typed columns for ``rows``."""
    return typed_columns(config.RAW_COLUMNS, rows)


def raw_table(rows: List[Mapping[str, Any]]) -> Table:
    """One raw table from rows of the contract."""
    return new_table(raw_columns(rows))


def mock_tables(book) -> Dict[str, Table]:
    """``{"A": table, "B": table, "C": table}`` from a :class:`oms_trace.mockdata.Book`."""
    return {system: raw_table(book.rows[system]) for system in config.SYSTEMS}


def clock_table(now_nanos: int) -> Table:
    """A fixed one-row clock: ``Now`` as an Instant."""
    return new_table([datetime_col("Now", [to_j_instant(int(now_nanos))])])


def input_tables() -> Dict[str, Any]:
    """Append-only input tables with the raw contract, one per system (``.add(raw_table(rows))``)."""
    defs = raw_column_definitions()
    return {system: input_table(col_defs=defs) for system in config.SYSTEMS}
