import json
import os

from oms_trace import config, mockdata, trace_check
from oms_trace.files import rows_from_dir


def test_analyze_flags_the_deliberate_violations(book):
    report = trace_check.analyze(book.rows)
    verdicts = "\n".join(report["verdicts"])
    assert "empty-string ParentOrderID" in verdicts          # scenario EP
    assert "A-Bp: 1 parents have more than one child" in verdicts  # scenario TB
    assert "Cli-Dest: 1 parents have more than one child" in verdicts  # scenario DD
    assert report["systems"]["A"]["parents"] + 1 == report["systems"]["A"]["children"]  # AM, DM and DD net to one
    assert "Bc-Cp: resolution rate" in verdicts              # scenario BCM (the B child never arrived)
    assert report["systems"]["B"]["children_with_upstream_ref"] == 0  # Q16 contract holds in the mock
    assert report["hops"]["Cp-Cc"]["resolution_rate"] == 1.0
    assert report["hops"]["Bp-Bc"]["children_per_parent_max"] == 300
    assert report["topics"]["C:" + mockdata.TOPIC_C_STALE]["messages"] >= 1
    text = trace_check.render(report)
    assert "== verdicts ==" in text and "A-Bp" in text


def test_clean_book_has_no_violations():
    book = mockdata.generate(seed=7, families=10, scenarios=False)
    report = trace_check.analyze(book.rows)
    assert report["verdicts"] == ["no contract violations found"]
    for hop in report["hops"].values():
        assert hop["resolution_rate"] == 1.0


def test_export_and_reload_round_trip(tmp_path, book):
    trace_check.export_mock(str(tmp_path), seed=42, families=24)
    reloaded = rows_from_dir(str(tmp_path))
    for system in config.SYSTEMS:
        assert len(reloaded[system]) == len(book.rows[system])
        assert reloaded[system][0]["RecvTs"] == book.rows[system][0]["RecvTs"]
        assert set(reloaded[system][0]) == set(config.RAW_COLUMN_NAMES)
    with open(os.path.join(tmp_path, "orders_a.jsonl"), encoding="utf-8") as fh:
        first = json.loads(fh.readline())
    assert first["OrderID"] == book.rows["A"][0]["OrderID"]


def test_cli_exit_code_reflects_verdicts(capsys):
    assert trace_check.main(["--mock"]) == 1
    out = capsys.readouterr().out
    assert "verdicts" in out
    assert trace_check.main(["--mock", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["hops"]["Cp-Cc"]["resolution_rate"] == 1.0
