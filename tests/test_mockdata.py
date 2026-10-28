from oms_trace import config, mockdata


def test_book_has_every_scenario_and_consistent_healthy_families(book):
    names = set(book.scenarios)
    for expected in ("CLEAN", "WORK", "MB", "MC", "MCC", "UA", "BPL", "TR", "BCM", "BPM", "AM", "DM", "DU", "DD", "NL",
                     "EP", "TB", "NRC", "MM", "OF", "DUP", "OOO", "MID", "HELD", "OR", "CNP", "LMX", "FS", "FC", "FAN"):
        assert expected in names
    assert sum(1 for n in names if n.startswith("H")) >= 24
    rows = book.all_rows()
    assert len(rows) > 2500  # the fan-out scenario alone is 2 102 rows
    for row in rows:
        assert set(row) == set(config.RAW_COLUMN_NAMES)
        assert row["RecvTs"] <= book.now


def test_ids_unique_within_a_system_except_deliberate_restatements(book):
    for system in config.SYSTEMS:
        seen = {}
        for row in book.rows[system]:
            seen.setdefault(row["OrderID"], []).append(row)
        multi = {k: v for k, v in seen.items() if len(v) > 1}
        # only the duplicate-delivery and out-of-order scenarios restate an order
        assert set(multi) <= {book.scenarios["DUP"].ids["Cc1a"], book.scenarios["OOO"].ids["Cc2a"]}, multi.keys()


def test_offsets_increase_per_topic(book):
    last = {}
    for row in book.all_rows():  # strictly increasing per topic (removed scenario rows leave gaps)
        assert row["KafkaOffset"] > last.get(row["Topic"], -1)
        last[row["Topic"]] = row["KafkaOffset"]


def test_every_regular_topic_is_current(book, settings):
    for system, topics in mockdata.TOPICS.items():
        for topic in topics:
            latest = max(r["RecvTs"] for r in book.rows[system] if r["Topic"] == topic)
            assert book.now - latest <= settings.feed_stale_sec * config.SECOND_NANOS
