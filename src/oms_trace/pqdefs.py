"""Persistent query definitions: ``pq/*.json`` is the single source of truth for every ``oms-*`` PQ.

Each file describes one PQ the way docs/design/07 section 2 tabulates them: type, dispatcher,
script, heap, schedule, restart policy, dependencies, environment variables. The deployment uses
them three ways (docs/deployment.md step 6):

* ``--check``: validate (every script exists, every type and dispatcher is known, every
  ``${VAR}`` in an environment value is resolvable when ``--env`` is given, dependencies and
  daily restarts are ordered).
* ``--apply --dry-run``: print the definitions as the table an operator types into the Query
  Monitor the first time.
* ``--apply``: create or update the PQs through the Core+ controller client inside a Core+ Python
  environment. The client calls and the configuration field names follow the Core+ Python
  documentation as read at design time; **confirm on 2026.01.054** (``CONTROLLER_FIELDS`` is the
  one place to adjust). The definitions do not change whatever the API spelling turns out to be.

Nothing here imports ``deephaven`` or ``deephaven_enterprise`` at module scope.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

#: Controller type labels (docs/design/07 section 2; confirm the exact labels in the Query Monitor).
PQ_TYPES: Dict[str, str] = {
    "Live Query (Script)": "Script",
    "Live Query - Merge Server": "Script",
    "Batch Query (RunAndDone)": "RunAndDone",
}
DISPATCHERS = ("query", "merge")
_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}")


class DefinitionError(ValueError):
    """A malformed PQ definition; the message names the file and the field."""


@dataclass
class PQDefinition:
    name: str
    type: str
    dispatcher: str
    script: str
    heap_gb: float
    description: str = ""
    enabled: bool = True
    daily_restart: str = ""          # HH:MM in timezone, empty = no daily restart
    timezone: str = "America/New_York"
    restart_attempts: int = 3
    restart_delay_sec: int = 60
    init_timeout_sec: int = 1800
    dependencies: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    jvm_args: List[str] = field(default_factory=list)
    source: str = ""                 # the file it came from

    @property
    def restart_minutes(self) -> Optional[int]:
        if not self.daily_restart:
            return None
        h, m = self.daily_restart.split(":")
        return int(h) * 60 + int(m)


def _field(raw: Mapping[str, Any], key: str, kind: type, path: str, default: Any = None, required: bool = False) -> Any:
    if key not in raw:
        if required:
            raise DefinitionError(f"{path}: missing '{key}'")
        return default
    value = raw[key]
    if kind is float and isinstance(value, int):
        value = float(value)
    if not isinstance(value, kind):
        raise DefinitionError(f"{path}: '{key}' must be {kind.__name__}, got {type(value).__name__}")
    return value


def load_definition(path: str) -> PQDefinition:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict):
        raise DefinitionError(f"{path}: not a JSON object")
    d = PQDefinition(
        name=_field(raw, "name", str, path, required=True),
        type=_field(raw, "type", str, path, required=True),
        dispatcher=_field(raw, "dispatcher", str, path, required=True),
        script=_field(raw, "script", str, path, required=True),
        heap_gb=_field(raw, "heap_gb", float, path, required=True),
        description=_field(raw, "description", str, path, ""),
        enabled=_field(raw, "enabled", bool, path, True),
        daily_restart=_field(raw, "daily_restart", str, path, ""),
        timezone=_field(raw, "timezone", str, path, "America/New_York"),
        restart_attempts=_field(raw, "restart_attempts", int, path, 3),
        restart_delay_sec=_field(raw, "restart_delay_sec", int, path, 60),
        init_timeout_sec=_field(raw, "init_timeout_sec", int, path, 1800),
        dependencies=list(_field(raw, "dependencies", list, path, [])),
        env=dict(_field(raw, "env", dict, path, {})),
        jvm_args=list(_field(raw, "jvm_args", list, path, [])),
        source=path,
    )
    if d.type not in PQ_TYPES:
        raise DefinitionError(f"{path}: type {d.type!r} is not one of {list(PQ_TYPES)}")
    if d.dispatcher not in DISPATCHERS:
        raise DefinitionError(f"{path}: dispatcher {d.dispatcher!r} must be one of {DISPATCHERS}")
    if d.heap_gb <= 0:
        raise DefinitionError(f"{path}: heap_gb must be positive")
    if d.daily_restart and not re.fullmatch(r"\d{2}:\d{2}", d.daily_restart):
        raise DefinitionError(f"{path}: daily_restart must be HH:MM")
    if os.path.basename(path) != f"{d.name}.json":
        raise DefinitionError(f"{path}: the file must be named {d.name}.json")
    for k, v in d.env.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise DefinitionError(f"{path}: env entries must be string: string")
    return d


def load_definitions(directory: str) -> List[PQDefinition]:
    defs = [load_definition(p) for p in sorted(glob.glob(os.path.join(directory, "*.json")))]
    names = [d.name for d in defs]
    if len(set(names)) != len(names):
        raise DefinitionError(f"{directory}: duplicate PQ names")
    return defs


def validate(defs: Sequence[PQDefinition], repo_root: str, env: Optional[Mapping[str, str]] = None) -> List[str]:
    """Problems as messages (empty = valid). With ``env``, every ``${VAR}`` must resolve."""
    problems: List[str] = []
    by_name = {d.name: d for d in defs}
    for d in defs:
        if not os.path.isfile(os.path.join(repo_root, d.script)):
            problems.append(f"{d.name}: script {d.script} does not exist")
        for dep in d.dependencies:
            if dep not in by_name:
                problems.append(f"{d.name}: depends on unknown PQ {dep}")
            elif d.restart_minutes is not None and by_name[dep].restart_minutes is not None \
                    and d.restart_minutes <= by_name[dep].restart_minutes:
                problems.append(f"{d.name}: its daily restart {d.daily_restart} must follow {dep}'s {by_name[dep].daily_restart}")
        if env is not None:
            try:
                expand_env(d.env, env)
            except DefinitionError as exc:
                problems.append(f"{d.name}: {exc}")
    return problems


def expand_env(values: Mapping[str, str], env: Mapping[str, str]) -> Dict[str, str]:
    """``${VAR}`` and ``${VAR:-default}`` in values, from ``env``; an unresolvable variable is an error."""
    out: Dict[str, str] = {}
    for key, value in values.items():
        def sub(m: "re.Match[str]") -> str:
            name, has_default, default = m.group(1), m.group(2), m.group(3)
            if name in env and env[name] != "":
                return env[name]
            if has_default is not None:
                return default or ""
            raise DefinitionError(f"env {key}: ${{{name}}} is not set")
        out[key] = _VAR.sub(sub, value)
    return out


def render_table(defs: Sequence[PQDefinition]) -> str:
    """The definitions as a text table (what an operator enters in the Query Monitor the first time)."""
    lines = []
    for d in defs:
        lines.append(f"{d.name}  [{'enabled' if d.enabled else 'disabled'}]")
        lines.append(f"  type        {d.type}   dispatcher: {d.dispatcher}   heap: {d.heap_gb:g} GB")
        lines.append(f"  script      {d.script}")
        lines.append(f"  schedule    continuous; daily restart {d.daily_restart or 'none'} {d.timezone}; "
                     f"restart on error x{d.restart_attempts} after {d.restart_delay_sec}s; init timeout {d.init_timeout_sec}s")
        if d.dependencies:
            lines.append(f"  depends on  {', '.join(d.dependencies)}")
        if d.jvm_args:
            lines.append(f"  jvm         {' '.join(d.jvm_args)}")
        for k, v in d.env.items():
            lines.append(f"  env         {k}={v}")
        if d.description:
            lines.append(f"  note        {d.description}")
        lines.append("")
    return "\n".join(lines)


# --- the controller client (Core+), confirm on 2026.01.054 -------------------------------------------

#: PersistentQueryConfigMessage field names as read from the Core+ Python documentation at design
#: time. If a name differs on the installed release, change it here only.
CONTROLLER_FIELDS = {
    "name": "name", "owner": "owner", "enabled": "enabled", "heap": "heapSizeGb", "server": "serverName",
    "type": "configurationType", "script_path": "scriptPath", "language": "scriptLanguage",
    "worker_kind": "workerKind", "env": "extraEnvironmentVariables", "jvm": "extraJvmArguments",
    "scheduling": "scheduling", "timeout": "timeoutNanos", "admins": "adminGroups", "viewers": "viewerGroups",
    "restart_users": "restartUsers",
}


def scheduling_lines(d: PQDefinition) -> List[str]:
    """The controller's scheduling strings (continuous, daily restart, error restarts, dependents)."""
    lines = [
        "SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerContinuous",
        f"TimeZone={d.timezone}",
        "SchedulingDisabled=false",
        "Overnight=false",
        f"RestartErrorCount={d.restart_attempts}",
        f"RestartErrorDelay={d.restart_delay_sec}",
    ]
    if d.daily_restart:
        lines += ["DailyRestart=true", f"StartTime={d.daily_restart}:00"]
    else:
        lines += ["DailyRestart=false"]
    if d.dependencies:
        lines = ["SchedulerType=com.illumon.iris.controller.IrisQuerySchedulerDependent",
                 f"TimeZone={d.timezone}", f"DependentQueryName={','.join(d.dependencies)}",
                 f"RestartErrorCount={d.restart_attempts}", f"RestartErrorDelay={d.restart_delay_sec}"]
        if d.daily_restart:
            lines += ["DailyRestart=true", f"StartTime={d.daily_restart}:00"]
    return lines


def controller_config(d: PQDefinition, env: Mapping[str, str]) -> Dict[str, Any]:
    """The controller configuration of one PQ as a plain dict keyed by CONTROLLER_FIELDS values."""
    f = CONTROLLER_FIELDS
    server = env.get("MERGE_SERVER" if d.dispatcher == "merge" else "QUERY_SERVER", "")
    if not server:
        raise DefinitionError(f"{d.name}: set {'MERGE_SERVER' if d.dispatcher == 'merge' else 'QUERY_SERVER'} in deploy/deploy.env")
    repo = env.get("GIT_REPO_NAME", "oms_trace")
    expanded = expand_env(d.env, env)
    return {
        f["name"]: d.name,
        f["owner"]: env.get("DH_SERVICE_USER", ""),
        f["enabled"]: d.enabled,
        f["heap"]: d.heap_gb,
        f["server"]: server,
        f["type"]: PQ_TYPES[d.type],
        f["script_path"]: f"{repo}/{d.script}",            # the Git-sourced script (docs/git-persistent-query-scripts.md)
        f["language"]: "Python",
        f["worker_kind"]: "DeephavenCommunity",            # a Core+ worker
        f["env"]: [f"{k}={v}" for k, v in expanded.items()],
        f["jvm"]: list(d.jvm_args) + [f"-Duser.timezone={d.timezone}"],
        f["scheduling"]: scheduling_lines(d),
        f["timeout"]: d.init_timeout_sec * 1_000_000_000,
        f["admins"]: [g for g in env.get("DH_ADMIN_GROUPS", "").split(",") if g],
        f["viewers"]: [g for g in env.get("DH_VIEWER_GROUPS", "").split(",") if g],
        f["restart_users"]: "ADMIN",
    }


def apply(defs: Sequence[PQDefinition], env: Mapping[str, str], dry_run: bool = False) -> List[str]:
    """Create or update every definition through the Core+ controller client; returns what was done."""
    done: List[str] = []
    configs = {d.name: controller_config(d, env) for d in defs}
    if dry_run:
        for d in defs:
            done.append(f"{d.name}: would {'create/update' if d.enabled else 'create (disabled)'} with {json.dumps(configs[d.name], sort_keys=True)}")
        return done
    # --- Core+ client inside a Core+ Python environment: confirm on 2026.01.054 ---------------------
    from deephaven_enterprise.client.session_manager import SessionManager  # type: ignore[import-not-found]

    sm = SessionManager(env["DH_CONNECTION_JSON_URL"])
    sm.private_key(env["DH_SERVICE_KEYFILE"])                 # the service account's key, never a password
    cc = sm.controller_client
    existing = {q.config.name: q for q in cc.map().values()} if hasattr(cc, "map") else {}
    for d in defs:
        cfg = controller_config(d, env)
        if d.name in existing:
            message = existing[d.name].config
            for key, value in cfg.items():
                _assign(message, key, value)
            cc.modify_query(message)
            done.append(f"{d.name}: updated")
        else:
            message = cc.make_temporary_config(d.name, d.heap_gb)   # a config message to fill in; confirm the factory name
            for key, value in cfg.items():
                _assign(message, key, value)
            cc.add_query(message)
            done.append(f"{d.name}: created")
    return done


def _assign(message: Any, key: str, value: Any) -> None:
    """Set a protobuf field, extending repeated ones."""
    current = getattr(message, key)
    if isinstance(value, list):
        del current[:]
        current.extend(value)
    else:
        setattr(message, key, value)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="oms_trace.pqdefs", description="validate, print or apply the oms-* PQ definitions")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", metavar="DIR", help="validate pq/*.json (scripts exist, types, dependencies, restarts)")
    mode.add_argument("--apply", metavar="DIR", help="create or update the PQs through the Core+ controller client")
    parser.add_argument("--dry-run", action="store_true", help="with --apply: print the definitions and the configurations only")
    parser.add_argument("--only", metavar="NAME", help="one PQ")
    parser.add_argument("--env", action="store_true", help="with --check: also resolve every ${VAR} from the environment")
    parser.add_argument("--repo-root", default=None, help="the repository root (default: the parent of DIR)")
    args = parser.parse_args(argv)
    directory = args.check or args.apply
    repo_root = args.repo_root or os.path.dirname(os.path.abspath(directory.rstrip("/")))
    try:
        defs = load_definitions(directory)
        problems = validate(defs, repo_root, os.environ if (args.env or args.apply) else None)  # the whole set: dependencies
        if args.only:
            defs = [d for d in defs if d.name == args.only]
            if not defs:
                print(f"no definition named {args.only}", file=sys.stderr)
                return 2
            problems = [p for p in problems if p.startswith(f"{args.only}:")]
        if problems:
            for p in problems:
                print(f"PROBLEM {p}", file=sys.stderr)
            return 1
        if args.check:
            print(f"{len(defs)} definitions valid: {', '.join(d.name for d in defs)}")
            return 0
        print(render_table(defs))
        for line in apply(defs, os.environ, dry_run=args.dry_run):
            print(line)
        return 0
    except DefinitionError as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
