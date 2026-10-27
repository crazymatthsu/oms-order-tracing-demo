"""The tree for a selection of roots (docs/design/04 section 7).

Never the whole book: a selection is capped (``Settings.ui_max_roots``), the synthetic
``NodeKey`` / ``ParentKey`` strings exist only on the selected rows, and ``promote_orphans``
keeps an unresolved node visible at the top of its partial subtree.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Sequence, Tuple

from deephaven import merge
from deephaven.table import Table

from oms_trace import config

UI_COLUMNS = ["Level", "System", "OrderId", "LinkId", "Symbol", "Side", "Status", "OrderQty", "CumQty", "LeavesQty",
              "RootId", "Topic"]
TREE_COLUMNS = ["Level", "OrderId", "BreakKind", "Status", "OrderQty", "CumQty", "LeavesQty", "FillGap", "RouteGap",
                "ParentState", "RootState", "Symbol", "Side", "System", "Topic", "LinkId", "RootId", "Depth", "NodeKey", "ParentKey"]


def sanitize_id(value: str) -> str:
    """An order id as a safe query-string literal: backticks and backslashes are not ids."""
    value = str(value)
    if "`" in value or "\\" in value or "\n" in value:
        raise ValueError(f"not a valid order id: {value!r}")
    return value


def _in_list(values: Iterable[str]) -> str:
    return ", ".join(f"`{sanitize_id(v)}`" for v in values)


def lineage_nodes(tables: Mapping[str, Table], root_ids: Sequence[str], settings: config.Settings) -> Tuple[Table, Any]:
    """One row per node of the selected roots (all six levels) and the native tree over them."""
    roots = [r for r in dict.fromkeys(root_ids) if r]
    if not roots:
        raise ValueError("select at least one root order id")
    if len(roots) > settings.ui_max_roots:
        raise ValueError(f"{len(roots)} roots selected; the tree shows at most {settings.ui_max_roots} "
                         f"(OMS_TRACE_UI_MAX_ROOTS) - narrow the selection")
    sel = _in_list(roots)
    prefix = {level: config.LEVEL_SYSTEM[level] for level in config.LEVELS}
    parts = []
    for level, name in (("A_ROOT", "a_root"), ("A_DEST", "links_dest"), ("B_PARENT", "links_bp"), ("B_CHILD", "links_bc"),
                        ("C_PARENT", "links_cp"), ("C_CHILD", "links_cc")):
        t = tables[name].where(f"RootId in {sel}")
        if level == "A_ROOT":
            extra = ["ParentState", "RootState", "NodeKey = `A|` + OrderId", "ParentKey = (String) null", "Depth = 0"]
        else:
            parent_prefix = prefix[config.LEVEL_PARENT[level]]
            extra = ["ParentState", "RootState", f"NodeKey = `{prefix[level]}|` + OrderId",
                     f"ParentKey = `{parent_prefix}|` + LinkId", f"Depth = {config.LEVEL_DEPTH[level]}"]
        parts.append(t.view(UI_COLUMNS + extra))
    nodes = merge(parts).natural_join(
        tables["gaps"].view(["Level", "OrderId", "FillGap", "RouteGap", "BreakKind"]),
        on=["Level", "OrderId"], joins=["FillGap", "RouteGap", "BreakKind"],
    ).update_view(["BreakKind = isNull(BreakKind) ? `NONE` : BreakKind"]).view(TREE_COLUMNS)
    tree = nodes.tree("NodeKey", "ParentKey", promote_orphans=True)
    return nodes, tree


def roots_for_order(tables: Mapping[str, Table], order_id: str) -> Table:
    """The root id(s) of any order id from any level (one row, or none while unresolved)."""
    oid = sanitize_id(order_id)
    parts = [tables["a_root"].where(f"OrderId == `{oid}`").view(["RootId"])]
    for name in ("links_dest", "links_bp", "links_bc", "links_cp", "links_cc"):
        parts.append(tables[name].where(f"OrderId == `{oid}` && !isNull(RootId)").view(["RootId"]))
    return merge(parts).select_distinct(["RootId"])
