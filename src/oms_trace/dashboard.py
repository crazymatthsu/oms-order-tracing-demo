"""The deephaven.ui dashboard: drift by root, drift edges, feed health, unresolved links, and the
tree of the selected root (docs/design/02 section 4 step 8).

``build`` returns a ``ui.dashboard``; the caller exports it as ``oms_trace_dashboard``. The
module imports ``deephaven.ui`` lazily so the tables are usable without the plugin.
"""

from __future__ import annotations

from typing import Any, Mapping

from oms_trace import config, ui_slice


def build(tables: Mapping[str, Any], settings: config.Settings, tree: bool = True):
    """The dashboard; ``tree=False`` for a UI PQ that holds the small tables only (queries/oms_trace/ui.py)."""
    from deephaven import ui

    @ui.component
    def oms_trace_app():
        root, set_root = ui.use_state("")
        any_id, set_any_id = ui.use_state("")
        error, set_error = ui.use_state("")

        def on_root_press(data):
            try:
                value = data["RootId"]["value"] if isinstance(data.get("RootId"), dict) else data.get("RootId")
                set_root(str(value) if value else "")
                set_error("")
            except Exception as exc:  # noqa: BLE001
                set_error(f"{type(exc).__name__}: {exc}")

        def on_edge_press(data):
            try:
                value = data["RootId"]["value"] if isinstance(data.get("RootId"), dict) else data.get("RootId")
                if value:
                    set_root(str(value))
            except Exception as exc:  # noqa: BLE001
                set_error(f"{type(exc).__name__}: {exc}")

        def resolve_any_id():
            try:
                found = ui_slice.roots_for_order(tables, any_id)
                rows = list(found.iter_dict()) if found.size else []
                if rows:
                    set_root(str(rows[0]["RootId"]))
                    set_error("")
                else:
                    set_error(f"{any_id}: not found, or not yet linked to a root")
            except Exception as exc:  # noqa: BLE001
                set_error(f"{type(exc).__name__}: {exc}")

        tree_panel = None
        if not tree:
            tree_panel = ui.panel(ui.text("The lineage tree is on the lineage PQ's own oms_trace_dashboard (this PQ holds the small tables only)."),
                                  title="Lineage")
        elif root:
            try:
                nodes, tree_table = ui_slice.lineage_nodes(tables, [root], settings)
                tree_panel = ui.panel(ui.table(tree_table), title=f"Lineage {root}")
            except Exception as exc:  # noqa: BLE001
                tree_panel = ui.panel(ui.text(f"{type(exc).__name__}: {exc}"), title="Lineage")
        else:
            tree_panel = ui.panel(ui.text("Press a row in drift by root or drift edges, or enter any order id."),
                                  title="Lineage")

        edges = tables["drift_edges"]
        if root:
            edges = edges.where(f"RootId == `{ui_slice.sanitize_id(root)}`")

        controls = ui.panel(
            ui.flex(
                ui.text_field(label="Any order id", value=any_id, on_change=set_any_id),
                ui.button("Find root", on_press=resolve_any_id),
                ui.button("Clear", on_press=lambda: (set_root(""), set_any_id(""), set_error(""))),
                ui.text(f"Selected root: {root or '-'}"),
                ui.text(error, color="negative") if error else None,
                direction="row", gap="size-100", align_items="end", wrap=True,
            ),
            title="Selection",
        )
        rows = [
            controls,
            ui.row(
                ui.panel(ui.table(tables["drift_by_a_order"], on_row_press=on_root_press), title="Drift by root A order"),
                ui.panel(ui.table(edges, on_row_press=on_edge_press), title="Drift edges (aged, feed current)"),
            ),
            ui.row(
                tree_panel,
                ui.column(
                    ui.panel(ui.table(tables["feed_health"]), title="Feed health per channel"),
                    ui.panel(ui.table(tables["unresolved_links"]), title="Unresolved links"),
                ),
            ),
        ]
        if "alerts" in tables and "break_history" in tables:
            rows.append(ui.row(
                ui.panel(ui.table(tables["alerts"].reverse()), title="Alerts (newest first; Posted = sink result)"),
                ui.panel(ui.table(tables["break_history"].reverse()), title="Break events (newest first)"),
            ))
        return ui.dashboard(ui.column(*rows))

    return oms_trace_app()
