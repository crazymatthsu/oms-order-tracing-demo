"""The PQ definitions and the deployment scripts on a bare python."""

from __future__ import annotations

import glob
import os
import subprocess
import sys

import pytest

from oms_trace import pqdefs

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PQ_DIR = os.path.join(ROOT, "pq")
ENV = {"OMS_TRACE_TZ": "America/New_York", "OMS_TOPICS_A": "a1,a2", "OMS_TOPICS_B": "b1", "OMS_TOPICS_C": "c1",
       "KAFKA_BOOTSTRAP": "k:9093", "DH_CONNECTION_JSON_URL": "https://infra/connection.json",
       "DH_SERVICE_KEYFILE": "/secrets/svc.pem", "DH_SERVICE_USER": "oms-trace-svc", "DIS_STORAGE_ROOT": "/db/dis",
       "OMS_AMPS_URI": "tcp://amps:9007/amps/json", "QUERY_SERVER": "Query_1", "MERGE_SERVER": "Merge_1",
       "DH_ADMIN_GROUPS": "oms-trace-ops", "DH_VIEWER_GROUPS": "oms-desk-a,oms-desk-b", "GIT_REPO_NAME": "oms_trace"}


def test_every_definition_loads_validates_and_names_an_existing_script():
    defs = pqdefs.load_definitions(PQ_DIR)
    assert {d.name for d in defs} >= {"oms-ingest-a", "oms-ingest-b", "oms-ingest-c", "oms-lineage", "oms-drift-writer",
                                      "oms-trace-ui", "oms-ingest-amps-a"}
    assert pqdefs.validate(defs, ROOT, ENV) == []
    by = {d.name: d for d in defs}
    assert by["oms-lineage"].dispatcher == "query" and by["oms-drift-writer"].dispatcher == "merge"
    assert by["oms-drift-writer"].dependencies == ["oms-lineage"] and by["oms-trace-ui"].dependencies == ["oms-lineage"]
    # staggered daily restarts: ingesters, lineage, writer, ui (docs/design/07 section 7)
    assert by["oms-ingest-a"].restart_minutes < by["oms-lineage"].restart_minutes < by["oms-drift-writer"].restart_minutes \
        < by["oms-trace-ui"].restart_minutes
    assert not by["oms-ingest-amps-a"].enabled and by["oms-ingest-a"].enabled
    for d in defs:
        assert d.env["OMS_TRACE_TZ"] == "${OMS_TRACE_TZ}"
        assert "KAFKA_PROP" not in " ".join(d.env) and "password" not in " ".join(d.env.values()).lower()


def test_env_expansion_defaults_and_missing_variables():
    assert pqdefs.expand_env({"A": "${X}", "B": "${Y:-dflt}", "C": "${Z:-}", "D": "plain"}, {"X": "1"}) == \
        {"A": "1", "B": "dflt", "C": "", "D": "plain"}
    with pytest.raises(pqdefs.DefinitionError):
        pqdefs.expand_env({"A": "${MISSING}"}, {})
    defs = pqdefs.load_definitions(PQ_DIR)
    problems = pqdefs.validate(defs, ROOT, {k: v for k, v in ENV.items() if k != "KAFKA_BOOTSTRAP"})
    assert any("oms-ingest-a" in p and "KAFKA_BOOTSTRAP" in p for p in problems)


def test_controller_config_and_scheduling(tmp_path):
    defs = {d.name: d for d in pqdefs.load_definitions(PQ_DIR)}
    cfg = pqdefs.controller_config(defs["oms-lineage"], ENV)
    assert cfg["scriptPath"] == "oms_trace/queries/oms_trace/lineage.py" and cfg["serverName"] == "Query_1"
    assert cfg["heapSizeGb"] == 64 and cfg["configurationType"] == "Script" and cfg["owner"] == "oms-trace-svc"
    assert "OMS_TRACE_SOURCE=enterprise" in cfg["extraEnvironmentVariables"] and "OMS_TOPICS_A=a1,a2" in cfg["extraEnvironmentVariables"]
    assert "-Duser.timezone=America/New_York" in cfg["extraJvmArguments"] and cfg["timeoutNanos"] == 3600 * 10**9
    assert "DailyRestart=true" in cfg["scheduling"] and "StartTime=00:15:00" in cfg["scheduling"]
    writer = pqdefs.controller_config(defs["oms-drift-writer"], ENV)
    assert writer["serverName"] == "Merge_1" and "DependentQueryName=oms-lineage" in writer["scheduling"]
    assert writer["viewerGroups"] == ["oms-desk-a", "oms-desk-b"]
    with pytest.raises(pqdefs.DefinitionError):
        pqdefs.controller_config(defs["oms-lineage"], {k: v for k, v in ENV.items() if k != "QUERY_SERVER"})
    done = pqdefs.apply([defs["oms-lineage"]], ENV, dry_run=True)
    assert len(done) == 1 and done[0].startswith("oms-lineage: would create/update")
    # a malformed definition is named
    bad = tmp_path / "x.json"
    bad.write_text('{"name": "y", "type": "Nope", "dispatcher": "query", "script": "q.py", "heap_gb": 1}')
    with pytest.raises(pqdefs.DefinitionError):
        pqdefs.load_definition(str(bad))


def test_cli_check_and_dry_run(capsys):
    assert pqdefs.main(["--check", PQ_DIR, "--repo-root", ROOT]) == 0
    assert "definitions valid" in capsys.readouterr().out
    env_backup = dict(os.environ)
    try:
        os.environ.update(ENV)
        assert pqdefs.main(["--apply", PQ_DIR, "--dry-run", "--only", "oms-trace-ui", "--repo-root", ROOT]) == 0
        out = capsys.readouterr().out
        assert "oms-trace-ui" in out and "queries/oms_trace/ui.py" in out and "would create/update" in out
        assert pqdefs.main(["--apply", PQ_DIR, "--dry-run", "--only", "nope", "--repo-root", ROOT]) == 2
    finally:
        os.environ.clear()
        os.environ.update(env_backup)


def test_deployment_scripts_parse_and_dry_run_without_a_cluster(tmp_path):
    for script in glob.glob(os.path.join(ROOT, "scripts", "*.sh")) + [os.path.join(ROOT, "routing", "dis.sh")]:
        subprocess.run(["bash", "-n", script], check=True)
    env_file = tmp_path / "deploy.env"
    env_file.write_text("DHCONFIG=echo dhconfig\nDIS_STORAGE_ROOT=/tmp/dis\nGIT_REPO_URI=git@example.com:x/y.git\n"
                        "KAFKA_BOOTSTRAP=\nQUERY_SERVER=Query_1\nMERGE_SERVER=Merge_1\n")
    env = {**os.environ, "DEPLOY_ENV": str(env_file), "DRY_RUN": "1", "PATH": os.environ["PATH"]}
    for script in ("deploy_schemas.sh", "deploy_dis.sh"):
        res = subprocess.run(["bash", os.path.join(ROOT, "scripts", script)], env=env, capture_output=True, text=True)
        assert res.returncode == 0, res.stderr
        assert "schema import" in res.stdout or "dis add" in res.stdout
    res = subprocess.run(["bash", os.path.join(ROOT, "scripts", "deploy_git_repo.sh")], env=env, capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    frag = os.path.join(ROOT, "deploy", "iris-environment.oms_trace.prop")
    assert os.path.exists(frag)
    with open(frag) as fh:
        text = fh.read()
    assert "iris.scripts.repo.oms_trace.uri=git@example.com:x/y.git" in text and "paths=queries/oms_trace" in text
    os.remove(frag)
