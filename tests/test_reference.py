"""The oracle on the scenario catalog (docs/design/09 section 2) and the identities."""

from oms_trace import config, mockdata, reference


def gap(oracle, edge, order_id):
    return oracle.gaps.get((edge, order_id))


def kinds_for_root(oracle, root_id):
    return {(e, oid): g["BreakKind"] for (e, oid), g in oracle.gaps.items() if g["RootId"] == root_id}


def test_clean_and_working_and_healthy_families_have_no_breaks(oracle, book):
    for name, sc in book.scenarios.items():
        healthy = name.startswith("HB-") or (name.startswith("H") and name[1:].isdigit())
        if name in ("CLEAN", "WORK", "FAN", "DUP", "MID") or healthy:
            assert kinds_for_root(oracle, sc.root_id) == {}, (name, kinds_for_root(oracle, sc.root_id))
            assert sc.root_id not in oracle.drift_by_a_order
    assert oracle.drift_by_symbol.get(("AAPL", "BUY", "A-Bp"), {}).get("Breaks", 0) >= 1  # the broken scenarios are AAPL/BUY


def test_missing_at_b(oracle, book):
    s = book.scenarios["MB"]
    g = gap(oracle, "Bp-Bc", s.ids["Bp"])
    assert g["FillGap"] == 100 and g["BreakKind"] == "MISSING_DOWNSTREAM" and g["Scope"] == "INTERNAL"
    assert g["ShortSide"] == "CHILD" and g["MissedBy"] == "B" and g["IsDrift"]
    g2 = gap(oracle, "Bc-Cp", s.ids["Bc2"])
    assert g2["FillGap"] == -100 and g2["BreakKind"] == "UNBOOKED_UPSTREAM" and g2["MissedBy"] == "B"
    root = oracle.drift_by_a_order[s.root_id]
    assert root["InternalB"] == 100 and root["UnbookedAtB"] == 100 and root["MissingAtC"] == 0 and root["Breaks"] == 2


def test_missing_at_c_and_its_children(oracle, book):
    s = book.scenarios["MC"]
    g = gap(oracle, "Bc-Cp", s.ids["Bc2"])
    assert g["FillGap"] == 100 and g["BreakKind"] == "MISSING_DOWNSTREAM" and g["MissedBy"] == "C" and g["Scope"] == "CROSS"
    assert oracle.drift_by_a_order[s.root_id]["MissingAtC"] == 100
    s = book.scenarios["MCC"]
    g = gap(oracle, "Cp-Cc", s.ids["Cp2"])
    assert g["FillGap"] == 100 and g["BreakKind"] == "MISSING_DOWNSTREAM" and g["MissedBy"] == "C" and g["Scope"] == "INTERNAL"
    assert oracle.drift_by_a_order[s.root_id]["InternalC"] == 100


def test_unbooked_at_a_and_b_parent_lags(oracle, book):
    s = book.scenarios["UA"]
    g = gap(oracle, "A-Bp", s.ids["Dest"])  # the cross-system edge is keyed by A's destination order
    assert g["FillGap"] == -100 and g["BreakKind"] == "UNBOOKED_UPSTREAM" and g["MissedBy"] == "A"
    assert oracle.drift_by_a_order[s.root_id]["UnbookedAtA"] == 100
    assert gap(oracle, "Cli-Dest", s.ids["A"]) is None  # the two A books agree
    s = book.scenarios["BPL"]
    g = gap(oracle, "Bp-Bc", s.ids["Bp"])
    assert g["FillGap"] == -100 and g["BreakKind"] == "UNBOOKED_UPSTREAM" and g["ShortSide"] == "PARENT" and g["MissedBy"] == "B"


def test_transient_is_not_drift(oracle, book):
    s = book.scenarios["TR"]
    g = gap(oracle, "Bc-Cp", s.ids["Bc2"])
    assert g["GapAgeSec"] == 10 and g["BreakKind"] == "TRANSIENT" and not g["IsDrift"]
    assert s.root_id not in oracle.drift_by_a_order


def test_missing_parents_at_each_hop(oracle, book):
    s = book.scenarios["BCM"]  # B child missing: C parent unresolved for 2 h, its children root-unresolved
    cp = oracle.links["C_PARENT"][s.ids["Cp1"]]
    assert cp["ParentState"] == "UNRESOLVED" and cp["RootId"] is None and cp["RootState"] == "UNRESOLVED"
    u = oracle.unresolved_links[("C_PARENT", s.ids["Cp1"])]
    assert u["AgeSec"] >= 7190 and u["Alerted"] and not u["FeedStale"]
    for key in ("Cc1a", "Cc1b"):
        cc = oracle.links["C_CHILD"][s.ids[key]]
        assert cc["ParentState"] == "LINKED" and cc["RootState"] == "UNRESOLVED"
        assert ("C_CHILD", s.ids[key]) in oracle.root_unresolved
    assert gap(oracle, "Cp-Cc", s.ids["Cp1"]) is None  # C's own edge still reconciles

    s = book.scenarios["BPM"]  # B parent missing: its children unresolved, A's destination order short of everything
    for key in ("Bc1", "Bc2"):
        assert oracle.links["B_CHILD"][s.ids[key]]["ParentState"] == "UNRESOLVED"
    g = gap(oracle, "A-Bp", s.ids["Dest"])
    assert g["ChildCount"] == 0 and g["FillGap"] == 1000 and g["BreakKind"] == "MISSING_DOWNSTREAM" and g["MissedBy"] == "B"

    s = book.scenarios["AM"]  # the client order missing: its destination order is unresolved, the chain below root-unresolved
    dest = oracle.links["A_DEST"][s.ids["Dest"]]
    assert dest["ParentState"] == "UNRESOLVED" and dest["RootId"] is None and dest["RootState"] == "UNRESOLVED"
    bp = oracle.links["B_PARENT"][s.ids["Bp"]]
    assert bp["ParentState"] == "LINKED" and bp["RootId"] is None and bp["RootState"] == "UNRESOLVED"
    assert ("B_PARENT", s.ids["Bp"]) in oracle.root_unresolved
    assert s.root_id not in oracle.drift_by_a_order  # no client row, no root row


def test_client_and_destination_orders(oracle, book):
    s = book.scenarios["DM"]  # the destination order missing: the B parent is unresolved, the client order short
    bp = oracle.links["B_PARENT"][s.ids["Bp"]]
    assert bp["ParentState"] == "UNRESOLVED" and bp["RootId"] is None and bp["LinkId"] == s.ids["Dest"]
    g = gap(oracle, "Cli-Dest", s.ids["A"])
    assert g["ChildCount"] == 0 and g["FillGap"] == 1000 and g["BreakKind"] == "MISSING_DOWNSTREAM"
    assert g["Scope"] == "INTERNAL" and g["MissedBy"] == "A" and g["ExpectOne"]
    assert oracle.drift_by_a_order[s.root_id]["MissingAtDest"] == 1000
    s = book.scenarios["DU"]  # the client order shows less than its destination order booked
    g = gap(oracle, "Cli-Dest", s.ids["A"])
    assert g["FillGap"] == -100 and g["BreakKind"] == "UNBOOKED_UPSTREAM" and g["MissedBy"] == "A" and g["ShortSide"] == "PARENT"
    assert oracle.drift_by_a_order[s.root_id]["UnbookedAtCli"] == 100 and oracle.drift_by_a_order[s.root_id]["Breaks"] == 1
    assert gap(oracle, "A-Bp", s.ids["Dest"]) is None  # the destination order and B agree
    s = book.scenarios["DD"]  # two destination orders on a 1 : 1 hop
    g = gap(oracle, "Cli-Dest", s.ids["A"])
    assert g["ChildCount"] == 2 and g["BreakKind"] == "CARDINALITY" and g["IsDrift"]
    assert oracle.links["A_DEST"][s.ids["DestDup"]]["RootId"] == s.root_id
    # the 1 : 1 counter: AM drops a client order (+1), DM a destination order (-1), DD adds one (+1)
    assert oracle.trace_stats[("DestMinusCli", "")] == len(oracle.nodes["A_DEST"]) - len(oracle.nodes["A_ROOT"]) == 1
    # every destination order links to its client order and every B parent to a destination order
    assert all(d["ParentState"] == "LINKED" for oid, d in oracle.links["A_DEST"].items() if oid != book.scenarios["AM"].ids["Dest"])
    assert sum(1 for b in oracle.links["B_PARENT"].values() if b["ParentState"] != "LINKED") == 1  # DM only


def test_no_link_and_empty_parent_id(oracle, book):
    s = book.scenarios["NL"]
    assert oracle.links["C_PARENT"][s.ids["Cp1"]]["ParentState"] == "NO_LINK"
    assert ("C_PARENT", s.ids["Cp1"]) in oracle.unresolved_links
    s = book.scenarios["EP"]
    bc = oracle.links["B_CHILD"][s.ids["Bc1"]]
    assert bc["ParentState"] == "NO_LINK" and bc["LinkId"] == ""   # empty string = a child with no link (Q15)
    assert s.ids["Bc1"] not in oracle.nodes["B_PARENT"]


def test_cardinality_and_missing_child(oracle, book):
    s = book.scenarios["TB"]
    g = gap(oracle, "A-Bp", s.ids["Dest"])
    assert g["ChildCount"] == 2 and g["BreakKind"] == "CARDINALITY" and g["IsDrift"]
    s = book.scenarios["NRC"]
    g = gap(oracle, "Bc-Cp", s.ids["Bc3"])
    assert g["ChildCount"] == 0 and g["CumQty"] == 0 and g["BreakKind"] == "MISSING_CHILD" and not g["IsDrift"]
    assert gap(oracle, "Bp-Bc", s.ids["Bp"]) is None and gap(oracle, "A-Bp", s.ids["Dest"]) is None
    assert oracle.trace_stats[("CParentsMinusBChildren", "")] == len(oracle.nodes["C_PARENT"]) - len(oracle.nodes["B_CHILD"])
    # NRC drops one C parent (-1) and BCM drops one B child (+1): the book nets to zero on purpose
    assert sum(1 for cp in oracle.links["C_PARENT"].values() if cp["ParentState"] != "LINKED") >= 1


def test_mismatch_overfill_duplicates_and_ordering(oracle, book):
    s = book.scenarios["MM"]
    assert ("C_CHILD", s.ids["Cc1a"]) in oracle.mismatch_links
    s = book.scenarios["OF"]
    assert gap(oracle, "Cp-Cc", s.ids["Cp1"])["BreakKind"] == "OVERFILL"
    s = book.scenarios["DUP"]
    assert kinds_for_root(oracle, s.root_id) == {}
    s = book.scenarios["OOO"]  # arrival order wins: the stale SeqNo 1 state (300) is the last word
    assert oracle.nodes["C_CHILD"][s.ids["Cc2a"]]["CumQty"] == 300
    assert gap(oracle, "Cp-Cc", s.ids["Cp2"])["BreakKind"] == "MISSING_DOWNSTREAM"


def test_open_quantity_kinds(oracle, book):
    s = book.scenarios["HELD"]
    g = gap(oracle, "Bp-Bc", s.ids["Bp"])
    assert g["FillGap"] == 0 and g["RouteGap"] == 200 and g["BreakKind"] == "HELD" and not g["IsDrift"] and g["OpenAt"] == "PARENT"
    assert s.root_id not in oracle.drift_by_a_order
    s = book.scenarios["OR"]
    g = gap(oracle, "Cp-Cc", s.ids["Cp1"])
    assert g["RouteGap"] == -100 and g["BreakKind"] == "OVER_ROUTED" and g["IsDrift"] and g["OpenAt"] == "CHILD"
    assert oracle.drift_by_a_order[s.root_id]["OverRoutedQty"] == 100
    s = book.scenarios["CNP"]
    g = gap(oracle, "Cp-Cc", s.ids["Cp1"])
    assert g["RouteGap"] == -300 and g["LeavesQty"] == 0 and g["BreakKind"] == "CHILD_STILL_OPEN"
    assert gap(oracle, "Bc-Cp", s.ids["Bc1"]) is None  # B and C agree on Bc1/Cp1
    s = book.scenarios["LMX"]
    g = gap(oracle, "A-Bp", s.ids["Dest"])
    assert g["FillGap"] == 0 and g["RouteGap"] == 100 and g["BreakKind"] == "LEAVES_MISMATCH"


def test_feed_health_suppresses_gaps(oracle, book):
    assert oracle.feed_health[("C", mockdata.TOPIC_C_STALE)]["TopicState"] == "STALE"
    assert oracle.feed_health[("B", mockdata.TOPIC_B_CATCHUP)]["TopicState"] == "CATCHING_UP"
    for system, topics in mockdata.TOPICS.items():
        for topic in topics:
            assert oracle.feed_health[(system, topic)]["TopicState"] == "CURRENT"
    s = book.scenarios["FS"]
    g = gap(oracle, "Bc-Cp", s.ids["Bc2"])
    assert g["ChildSideStale"] and g["BreakKind"] == "FEED_STALE" and not g["IsDrift"]
    assert ("Bc-Cp", s.ids["Bc2"]) in oracle.feed_suppressed
    s = book.scenarios["FC"]
    g = gap(oracle, "Bp-Bc", s.ids["Bp"])
    assert g["ParentFeed"] == "CATCHING_UP" and g["BreakKind"] == "FEED_STALE"
    assert gap(oracle, "Bc-Cp", s.ids["Bc2"])["BreakKind"] == "FEED_STALE"
    # the suppressed roots are not drift
    for name in ("FS", "FC"):
        assert book.scenarios[name].root_id not in oracle.drift_by_a_order


def test_unresolved_not_alerted_while_parent_system_stale():
    book = mockdata.Book()
    s = mockdata.family(book, "X", recv=mockdata.NOW - 2 * mockdata.HOUR,
                        topics={"A": ("oms.a.t1",), "B": ("oms.b.only",), "C": ("oms.c.t1",)})
    # drop the B child; every B row is 2 h old, so B's only channel is STALE; make C current
    book.rows["B"] = [r for r in book.rows["B"] if r["OrderId"] != s.ids["Bc1"]]
    for r in book.rows["C"] + book.rows["A"]:
        r["RecvTs"] = mockdata.NOW - 30 * mockdata.SEC
        r["EventTs"] = r["RecvTs"] - 20 * mockdata.MS
    res = reference.reconcile(book.rows, mockdata.settings_for(book), book.now)
    u = res.unresolved_links[("C_PARENT", s.ids["Cp1"])]
    assert res.system_health["B"] == 1 and u["FeedStale"] and not u["Alerted"] and u["AgeSec"] == 30


def test_identities_hold_on_every_fully_linked_root(oracle, book):
    checked = 0
    for root_id in oracle.nodes["A_ROOT"]:
        fully_linked = all(l["RootId"] == root_id and l["ParentState"] == "LINKED"
                           for level in config.LEVELS[1:] for l in oracle.links[level].values() if l["RootId"] == root_id)
        if not fully_linked:
            continue
        fill_ok, route_ok = reference.telescoping_holds(oracle, root_id)
        assert fill_ok and route_ok, root_id
        checked += 1
    assert checked >= 30


def test_drift_by_symbol_and_stats_are_consistent(oracle):
    for key, row in oracle.drift_by_symbol.items():
        drift = [g for g in oracle.drift_edges.values() if (g["Symbol"], g["Side"], g["Edge"]) == key]
        assert row["Breaks"] == len(drift)
        assert row["MissingDownstream"] == sum(max(g["FillGap"], 0) for g in drift)
    for edge in config.EDGES:
        assert oracle.trace_stats[("Drift", edge.name)] == sum(1 for (e, _), g in oracle.gaps.items() if e == edge.name and g["IsDrift"])
    assert oracle.trace_stats[("Nodes", "A_ROOT")] == len(oracle.nodes["A_ROOT"])
