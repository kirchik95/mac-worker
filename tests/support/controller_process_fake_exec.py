#!/usr/bin/env python3
"""Tests-only fake execution worker. Not a live host.

Speaks the existing host JSON opcodes (probe, lease-acquire, prepare,
session/prebind, submit, task-turn, status-logs, …) and mints a result
commit that is a Git descendant of the frozen base ingested from the
controller-transfer cache on the same disk. Stdout is one protocol JSON
line. Git upload-pack is handled by the labeled fake SSH hop, not here.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

PROTOCOL_VERSION = 7
SUPERVISION_VERSION = 3
GIT = "/usr/bin/git"
GIT_REMOVALS = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
)


def emit(obj: Any) -> None:
    sys.stdout.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=True))
    sys.stdout.write("\n")
    sys.stdout.flush()


def fail(message: str, code: int = 1) -> None:
    sys.stderr.write(f"fake execution worker: {message}\n")
    raise SystemExit(code)


def find_opcode(cmd: str) -> str:
    tokens = cmd.split()
    if "host" in tokens:
        index = tokens.index("host")
        if index + 1 < len(tokens):
            return tokens[index + 1]
    fail(f"no host opcode in {cmd!r}")


def walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def first_str(body: Any, *keys: str) -> str | None:
    if not isinstance(body, dict):
        return None
    for obj in walk(body):
        for key in keys:
            found = obj.get(key)
            if isinstance(found, str) and found:
                return found
    return None


def material_of(body: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(body.get("material"), dict):
        return body["material"]
    submit = body.get("submit")
    if isinstance(submit, dict) and isinstance(submit.get("material"), dict):
        return submit["material"]
    return None


def command_summary(command: dict[str, Any]) -> dict[str, Any]:
    mode = command.get("mode")
    if mode == "argv":
        argv = command.get("argv") or []
        return {"mode": "argv", "arg_count": len(argv)}
    return {"mode": "shell"}


def job_meta_from_material(material: dict[str, Any]) -> dict[str, Any]:
    return {
        "protocol_version": int(material.get("protocol_version") or PROTOCOL_VERSION),
        "job_id": material["job_id"],
        "client_id": material["client_id"],
        "worker_name": material["worker_name"],
        "project_id": material["project_id"],
        "worktree_id": material["worktree_id"],
        "manifest_digest": material["manifest_digest"],
        "request_fingerprint": material.get("request_fingerprint")
        or first_str(material, "request_fingerprint"),
        "command_summary": command_summary(material["command"]),
        "relative_working_dir": material.get("relative_working_dir") or "",
        "timeout_millis": int(material["timeout_millis"]),
        "resource_class": material["resource_class"],
        "created_at_millis": int(material["created_at_millis"]),
    }


def job_status(state: str, updated_at: int, *, terminal: bool) -> dict[str, Any]:
    status = {
        "state": state,
        "updated_at_millis": updated_at,
        "supervisor_pid": None,
        "supervisor_start_identity": None,
        "child_pid": None,
        "child_start_identity": None,
        "exit_code": 0 if terminal and state == "succeeded" else None,
        "terminating_signal": None,
        "final_stdout_bytes": 0 if terminal else None,
        "final_stderr_bytes": 0 if terminal else None,
        "error_code": None,
        "cleanup_error_code": None,
    }
    return status


def empty_chunk(stream: str, offset: int) -> dict[str, Any]:
    return {
        "stream": stream,
        "offset": offset,
        "next_offset": offset,
        "data": "",
    }


def git_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in GIT_REMOVALS:
        env.pop(name, None)
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env.setdefault("GIT_AUTHOR_NAME", "Fake Worker")
    env.setdefault("GIT_AUTHOR_EMAIL", "fake-worker@example.test")
    env.setdefault("GIT_COMMITTER_NAME", "Fake Worker")
    env.setdefault("GIT_COMMITTER_EMAIL", "fake-worker@example.test")
    return env


def git(args: list[str], *, git_dir: Path | None = None, stdin: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
    command = [GIT]
    if git_dir is not None:
        command.append(f"--git-dir={git_dir}")
    command.extend(args)
    return subprocess.run(
        command,
        input=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=git_env(),
        check=False,
    )


def git_text(args: list[str], *, git_dir: Path | None = None, stdin: bytes | None = None) -> str:
    result = git(args, git_dir=git_dir, stdin=stdin)
    if result.returncode != 0:
        fail(
            f"git {args!r} failed: {result.stderr.decode('utf-8', 'replace')}",
        )
    return result.stdout.decode("utf-8", "replace")


class Store:
    def __init__(self, path: Path, git_dir: Path) -> None:
        self.path = path
        self.git_dir = git_dir
        self.lock_path = path.with_name(path.name + ".lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.git_dir.mkdir(parents=True, exist_ok=True)
        if not (self.git_dir / "HEAD").exists() and not (self.git_dir / "refs").exists():
            git_text(["init", "--bare", str(self.git_dir)])

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"jobs": {}, "tasks": {}}
        return json.loads(self.path.read_text(encoding="utf-8") or "{}")

    def _save(self, data: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def mutate(self) -> Any:
        return _Locked(self)

    def candidate_gits(self) -> list[Path]:
        cache = Path(os.environ.get("XDG_CACHE_HOME", "")) / "mac-worker"
        found: list[Path] = []
        transfer = cache / "controller-transfer"
        if transfer.is_dir():
            found.extend(sorted(transfer.glob("*.git")))
        found.extend(sorted(cache.glob("*.git")))
        unique: list[Path] = []
        seen: set[str] = set()
        for path in found:
            key = str(path)
            if key not in seen:
                unique.append(path)
                seen.add(key)
        return unique

    def ingest(self, oid: str) -> None:
        existing = git(["cat-file", "-t", oid], git_dir=self.git_dir)
        if existing.returncode == 0:
            return
        last_err = "base object not present in controller-transfer"
        for source in self.candidate_gits():
            probe = git(["cat-file", "-t", oid], git_dir=source)
            if probe.returncode != 0:
                continue
            git(["-C", str(source), "config", "uploadpack.allowReachableSHA1InWant", "true"])
            git(["-C", str(source), "config", "uploadpack.allowAnySHA1InWant", "true"])
            fetched = git(
                [
                    "-c",
                    "protocol.file.allow=always",
                    "fetch",
                    "--no-tags",
                    str(source),
                    f"{oid}:refs/mac-worker/ingested/{oid}",
                ],
                git_dir=self.git_dir,
            )
            if fetched.returncode == 0:
                return
            mirrored = git(
                [
                    "-c",
                    "protocol.file.allow=always",
                    "fetch",
                    "--no-tags",
                    str(source),
                    "+refs/*:refs/mac-worker/from-cache/*",
                ],
                git_dir=self.git_dir,
            )
            if mirrored.returncode == 0:
                present = git(["cat-file", "-t", oid], git_dir=self.git_dir)
                if present.returncode == 0:
                    return
            last_err = (fetched.stderr or mirrored.stderr).decode("utf-8", "replace")
        fail(f"could not ingest frozen base {oid}: {last_err}")

    def descendant(self, task_id: str, base_oid: str, job_id: str) -> str:
        self.ingest(oid=base_oid)
        blob = git_text(
            ["hash-object", "-w", "--stdin"],
            git_dir=self.git_dir,
            stdin=f"fake-worker-result {job_id}\n".encode(),
        ).strip()
        tree = git_text(
            ["mktree"],
            git_dir=self.git_dir,
            stdin=f"100644 blob {blob}\tRESULT.txt\n".encode(),
        ).strip()
        commit = git_text(
            ["commit-tree", tree, "-p", base_oid, "-m", "fake worker result"],
            git_dir=self.git_dir,
        ).strip()
        if len(commit) != 40:
            fail(f"commit-tree produced {commit!r}")
        git_text(
            ["update-ref", f"refs/heads/task/{task_id}", commit],
            git_dir=self.git_dir,
        )
        ancestor = git(
            ["merge-base", "--is-ancestor", base_oid, commit],
            git_dir=self.git_dir,
        )
        if ancestor.returncode != 0:
            fail(f"result {commit} is not a descendant of {base_oid}")
        return commit


class _Locked:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.fd = None
        self.data: dict[str, Any] = {}

    def __enter__(self) -> dict[str, Any]:
        self.fd = open(self.store.lock_path, "a+", encoding="utf-8")
        fcntl.flock(self.fd.fileno(), fcntl.LOCK_EX)
        self.data = self.store._load()
        self.data.setdefault("jobs", {})
        self.data.setdefault("tasks", {})
        return self.data

    def __exit__(self, *exc: object) -> None:
        self.store._save(self.data)
        if self.fd is not None:
            fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
            self.fd.close()


def journal(journal_path: Path, record: dict[str, Any]) -> None:
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, separators=(",", ":"), ensure_ascii=True)
    lock_path = journal_path.with_suffix(journal_path.suffix + ".lock")
    with open(lock_path, "a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(journal_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def put_job(data: dict[str, Any], material: dict[str, Any], fingerprint: str, status: dict[str, Any]) -> dict[str, Any]:
    meta_material = dict(material)
    if "request_fingerprint" not in meta_material:
        meta_material["request_fingerprint"] = fingerprint
    meta = job_meta_from_material(meta_material)
    data["jobs"][meta["job_id"]] = {"meta": meta, "status": status}
    return meta


def task_status_from_task(task: dict[str, Any]) -> dict[str, Any]:
    executions = task.get("executions") if isinstance(task.get("executions"), dict) else {}
    order = task.get("execution_order") if isinstance(task.get("execution_order"), list) else []
    turns = []
    for job_id in order:
        if not isinstance(job_id, str) or job_id not in executions:
            continue
        exe = executions[job_id]
        created = int(exe.get("created_at_millis") or task.get("created_at_millis") or 100)
        turns.append(
            {
                "turn_number": int(exe.get("turn_number") or len(turns) + 1),
                "turn_id": job_id,
                "terminal": "succeeded",
                "outcome": {"kind": "done"},
                "agent_committed": True,
                "log_truncated": False,
                "started_at_millis": created,
                "ended_at_millis": created + 1,
            }
        )
    created_at = int(task.get("created_at_millis") or 100)
    pending_id = task.get("job_id")
    if not isinstance(pending_id, str) or pending_id in executions:
        pending_id = None
    completed = bool(task.get("result_oid"))
    if pending_id:
        history_len = len(turns)
        turns.append(
            {
                "turn_number": history_len + 1,
                "turn_id": pending_id,
                "terminal": None,
                "outcome": None,
                "agent_committed": None,
                "log_truncated": False,
                # ensure_active_status: first write sets started_at; follow-up append does not.
                "started_at_millis": created_at if history_len == 0 else None,
                "ended_at_millis": None,
            }
        )
        return {
            "state": "active",
            "last_outcome": {"kind": "done"} if completed else None,
            "worker": task.get("worker") or "mini-1",
            "session_present": completed,
            "head_oid": task.get("result_oid")
            if completed
            else (task.get("original_base_oid") or task.get("base_oid")),
            "summary": None,
            "questions": [],
            "files_changed": ["RESULT.txt"] if completed else [],
            "diff_stat": None,
            "turns": turns,
            "updated_at_millis": created_at,
        }
    return {
        "state": "open" if completed else "active",
        "last_outcome": {"kind": "done"} if completed else None,
        "worker": task.get("worker") or "mini-1",
        "session_present": completed,
        "head_oid": task.get("result_oid")
        if completed
        else (task.get("original_base_oid") or task.get("base_oid")),
        "summary": None,
        "questions": [],
        "files_changed": ["RESULT.txt"] if completed else [],
        "diff_stat": None,
        "turns": turns,
        "updated_at_millis": created_at + (1 if completed else 0),
    }


def task_status_body(
    *,
    task_id: str,
    turn_id: str,
    worker: str,
    head_oid: str | None,
    created_at: int,
    terminal: bool,
) -> dict[str, Any]:
    return task_status_from_task(
        {
            "worker": worker,
            "created_at_millis": created_at,
            "result_oid": head_oid if terminal else None,
            "executions": {
                turn_id: {
                    "turn_number": 1,
                    "created_at_millis": created_at,
                }
            }
            if terminal and head_oid and turn_id
            else {},
            "execution_order": [turn_id] if terminal and head_oid and turn_id else [],
        }
    )


def handle_probe(_body: dict[str, Any], _store: Store) -> dict[str, Any]:
    agents = []
    for name in ("claude", "codex", "cursor", "opencode"):
        agents.append(
            {
                "name": name,
                "version": "0.1.0",
                "auth": "authenticated",
                "auth_by_profile": [["secure", "authenticated"]],
            }
        )
    return {
        "protocol_version": PROTOCOL_VERSION,
        "supervision_version": SUPERVISION_VERSION,
        "hostname": "mini-1.local",
        "arch": "arm64",
        "os_version": "26.2",
        "free_disk_bytes": 100 * 1024 * 1024 * 1024,
        "total_disk_bytes": 250 * 1024 * 1024 * 1024,
        "memory_pressure": "normal",
        "swap_used_bytes": 0,
        "available_memory_bytes": 12 * 1024 * 1024 * 1024,
        "cpu_counters": {
            "user_ticks": 10,
            "system_ticks": 20,
            "idle_ticks": 30,
            "nice_ticks": 40,
        },
        "slot_state": "idle",
        "active_lease": None,
        "capabilities": ["darwin-arm64"],
        "agent_facts": {
            "agents": agents,
            "env_profiles": [{"name": "secure", "secure": True}],
            "git_identity": True,
            "collected_at_millis": 2**62,
        },
        "facts_age_millis": 0,
        "configured_slots": 1,
        "busy_slots": 0,
    }


def handle_lease(body: dict[str, Any], store: Store) -> dict[str, Any]:
    material = material_of(body)
    if material is None:
        fail("lease-acquire missing material")
    fingerprint = body.get("request_fingerprint") or material.get("request_fingerprint")
    if not fingerprint:
        fail("lease-acquire missing request_fingerprint")
    created = int(material["created_at_millis"])
    timeout = int(material["timeout_millis"])
    lease = {
        "job_id": material["job_id"],
        "client_id": material["client_id"],
        "lease_token": material["lease_token"],
        "request_fingerprint": fingerprint,
        "worker_name": material["worker_name"],
        "project_id": material["project_id"],
        "worktree_id": material["worktree_id"],
        "manifest_digest": material["manifest_digest"],
        "timeout_millis": timeout,
        "resource_class": material["resource_class"],
        "command_summary": command_summary(material["command"]),
        "created_at_millis": created,
        "expires_at_millis": created + timeout,
    }
    with store.mutate() as data:
        put_job(data, material, fingerprint, job_status("accepted", created, terminal=False))
        scope = body.get("execution_scope") or {}
        task_id = None
        if isinstance(scope, dict) and scope.get("kind") == "task":
            task_id = scope.get("task_id")
        if task_id:
            task = data["tasks"].setdefault(task_id, {})
            task.update(
                {
                    "job_id": material["job_id"],
                    "worker": material["worker_name"],
                    "created_at_millis": created,
                }
            )
    return {"outcome": "acquired", "lease": lease}


def handle_prepare(body: dict[str, Any], store: Store) -> dict[str, Any]:
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    base_oid = first_str(body, "base_oid")
    task_id = first_str(body, "task_id")
    if not base_oid or not task_id:
        fail("task-prepare missing meta.base_oid/task_id")
    store.ingest(base_oid)
    worker = body.get("worker") or "mini-1"
    job_id = body.get("job_id")
    with store.mutate() as data:
        task = data["tasks"].setdefault(task_id, {})
        if not task.get("original_base_oid"):
            task["original_base_oid"] = base_oid
        task.update(
            {
                "base_oid": task.get("original_base_oid") or base_oid,
                "worker": worker,
                "agent": (meta.get("agent") if isinstance(meta, dict) else None) or "codex",
                "job_id": job_id or task.get("job_id"),
                "created_at_millis": int(
                    (meta.get("created_at_millis") if isinstance(meta, dict) else None)
                    or task.get("created_at_millis")
                    or 100
                ),
            }
        )
    return {
        "protocol_version": PROTOCOL_VERSION,
        "head": base_oid,
        "reused": False,
    }


def handle_session(body: dict[str, Any], store: Store) -> dict[str, Any]:
    task_id = first_str(body, "task_id") or "unknown"
    agent = body.get("agent") or "codex"
    bound = 100
    with store.mutate() as data:
        task = data["tasks"].get(task_id) or {}
        agent = task.get("agent") or agent
        bound = int(task.get("created_at_millis") or bound)
        task = data["tasks"].setdefault(task_id, task)
        task["agent"] = agent
        task["session_ref"] = f"fake-session-{task_id}"
    return {
        "protocol_version": PROTOCOL_VERSION,
        "binding": {
            "agent": agent if isinstance(agent, str) else "codex",
            "session_ref": f"fake-session-{task_id}",
            "bound_at_millis": bound,
        },
    }


def complete_task(body: dict[str, Any], store: Store) -> tuple[dict[str, Any], dict[str, Any], str]:
    material = material_of(body)
    if material is None:
        fail("submit/task-turn missing material")
    fingerprint = (
        body.get("request_fingerprint")
        or (body.get("submit") or {}).get("request_fingerprint")
        or material.get("request_fingerprint")
    )
    if not fingerprint:
        fail("submit/task-turn missing request_fingerprint")
    turn = body.get("turn") if isinstance(body.get("turn"), dict) else {}
    task_id = turn.get("task_id") if isinstance(turn.get("task_id"), str) else None
    if not task_id:
        scope = (body.get("submit") or body).get("execution_scope") or {}
        if isinstance(scope, dict):
            task_id = scope.get("task_id")
    if not task_id:
        task_id = first_str(body, "task_id")
    submitted_base = turn.get("base_oid") if isinstance(turn.get("base_oid"), str) else None
    turn_number = turn.get("turn_number") if isinstance(turn.get("turn_number"), int) else 1
    created = int(material["created_at_millis"])
    worker = material.get("worker_name") or "mini-1"
    job_id = material["job_id"]
    if not task_id:
        fail("submit/task-turn missing task_id")
    with store.mutate() as data:
        task = data["tasks"].setdefault(task_id, {})
        executions = task.setdefault("executions", {})
        order = task.setdefault("execution_order", [])
        submitted_base = submitted_base or task.get("original_base_oid") or task.get("base_oid")
        if not submitted_base:
            fail("submit/task-turn missing turn.base_oid")
        if not task.get("original_base_oid"):
            task["original_base_oid"] = submitted_base
        existing = executions.get(job_id) if isinstance(executions.get(job_id), dict) else None
        if existing:
            if existing.get("fingerprint") != fingerprint:
                fail("submit/task-turn request_fingerprint does not match the stored turn")
            result_oid = existing["result_oid"]
        else:
            result_oid = store.descendant(task_id, submitted_base, job_id)
            executions[job_id] = {
                "fingerprint": fingerprint,
                "result_oid": result_oid,
                "base_oid": submitted_base,
                "turn_number": turn_number,
                "created_at_millis": created,
            }
            order.append(job_id)
            task["result_oid"] = result_oid
            task["job_id"] = job_id
            task["turn_id"] = job_id
        task["worker"] = worker
        task["base_oid"] = task["original_base_oid"]
        if not task.get("created_at_millis"):
            task["created_at_millis"] = created
        meta = put_job(
            data,
            material,
            fingerprint,
            job_status("succeeded", created + 1, terminal=True),
        )
        # Task-turn Accepted uses accepted job status; later polls are succeeded.
        accepted = job_status("accepted", created + 1, terminal=False)
        submit = {"outcome": "accepted", "meta": meta, "status": accepted}
        task_body = task_status_from_task(task)
        task["status"] = task_body
    return submit, task_body, result_oid


def handle_submit(body: dict[str, Any], store: Store) -> dict[str, Any]:
    submit, _, _ = complete_task(body, store)
    return submit


def handle_turn(body: dict[str, Any], store: Store) -> dict[str, Any]:
    submit, task_body, _ = complete_task(body, store)
    return {"submit": submit, "task": task_body}


def lookup_job(body: dict[str, Any], store: Store) -> tuple[dict[str, Any], dict[str, Any]]:
    job_id = first_str(body, "job_id")
    with store.mutate() as data:
        jobs = data["jobs"]
        if job_id and job_id in jobs:
            record = jobs[job_id]
            return record["meta"], record["status"]
        if len(jobs) == 1:
            record = next(iter(jobs.values()))
            return record["meta"], record["status"]
    fail(f"unknown job_id {job_id}")


def handle_status(body: dict[str, Any], store: Store) -> dict[str, Any]:
    meta, status = lookup_job(body, store)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "meta": meta,
        "status": status,
    }


def handle_status_logs(body: dict[str, Any], store: Store) -> dict[str, Any]:
    meta, status = lookup_job(body, store)
    stdout_off = int(body.get("stdout_offset") or 0)
    stderr_off = int(body.get("stderr_offset") or 0)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "status": {
            "protocol_version": PROTOCOL_VERSION,
            "meta": meta,
            "status": status,
        },
        "stdout": empty_chunk("stdout", stdout_off),
        "stderr": empty_chunk("stderr", stderr_off),
    }


def handle_log_chunk(body: dict[str, Any], store: Store) -> dict[str, Any]:
    lookup_job(body, store)
    stream = body.get("stream") or "stdout"
    offset = int(body.get("offset") or 0)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "chunk": empty_chunk(stream, offset),
    }


def handle_task_status(body: dict[str, Any], store: Store) -> dict[str, Any]:
    task_id = first_str(body, "task_id")
    with store.mutate() as data:
        task = (data["tasks"].get(task_id) or {}) if task_id else {}
        status = task_status_from_task(task)
    return {"protocol_version": PROTOCOL_VERSION, "status": status}


MAX_DIFF_BYTES = 512 * 1024


def handle_task_diff(body: dict[str, Any], store: Store) -> dict[str, Any]:
    task_id = first_str(body, "task_id")
    if not task_id:
        fail("task-diff missing task_id")
    with store.mutate() as data:
        task = data["tasks"].get(task_id) or {}
        original = task.get("original_base_oid") or task.get("base_oid")
        head = task.get("result_oid")
    if not original or not head:
        fail("task-diff missing original task base or current result")
    store.ingest(original)
    store.ingest(head)
    args = ["diff"]
    if body.get("stat"):
        args.append("--stat")
    args.extend([original, head])
    result = git(args, git_dir=store.git_dir)
    if result.returncode not in (0, 1):
        fail(f"git diff failed: {result.stderr.decode('utf-8', 'replace')}")
    raw = result.stdout
    truncated = len(raw) > MAX_DIFF_BYTES
    if truncated:
        raw = raw[:MAX_DIFF_BYTES]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        fail("git diff output is not UTF-8")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "text": text,
        "truncated": truncated,
    }


def handle_resolve(_body: dict[str, Any], _store: Store) -> dict[str, Any]:
    return {"protocol_version": PROTOCOL_VERSION, "outcome": "abandoned"}


def handle_snapshot(body: dict[str, Any], store: Store) -> dict[str, Any]:
    material = None
    job_id = first_str(body, "job_id")
    with store.mutate() as data:
        if job_id and job_id in data["jobs"]:
            material = data["jobs"][job_id]["meta"]
    if material is None:
        material = {
            "job_id": job_id or first_str(body, "job_id"),
            "client_id": first_str(body, "client_id"),
            "project_id": first_str(body, "project_id"),
            "worktree_id": first_str(body, "worktree_id"),
            "manifest_digest": first_str(body, "manifest_digest"),
        }
    missing = [key for key in ("job_id", "client_id", "project_id", "worktree_id", "manifest_digest") if not material.get(key)]
    if missing:
        fail(f"snapshot-verify missing {missing}")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "job_id": material["job_id"],
        "client_id": material["client_id"],
        "project_id": material["project_id"],
        "worktree_id": material["worktree_id"],
        "manifest_digest": material["manifest_digest"],
        "verified_at_millis": 100,
        "cache_reused": False,
    }


HANDLERS = {
    "probe": handle_probe,
    "lease-acquire": handle_lease,
    "task-prepare": handle_prepare,
    "task-session": handle_session,
    "task-prebind": handle_session,
    "submit": handle_submit,
    "task-turn": handle_turn,
    "status": handle_status,
    "status-logs": handle_status_logs,
    "log-chunk": handle_log_chunk,
    "task-status": handle_task_status,
    "task-diff": handle_task_diff,
    "resolve-or-abandon": handle_resolve,
    "snapshot-verify": handle_snapshot,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="tests-only fake execution worker")
    parser.add_argument("--dest", required=True)
    parser.add_argument("--cmd", required=True)
    args = parser.parse_args()
    sys.stderr.write("fake execution worker: labeled fixture; not a live agent\n")
    opcode = find_opcode(args.cmd)
    raw = sys.stdin.buffer.read()
    body: dict[str, Any] = {}
    if raw.strip():
        parsed = json.loads(raw.decode("utf-8"))
        if isinstance(parsed, dict):
            body = parsed
        else:
            fail("stdin JSON must be an object")
    journal_path = Path(os.environ["FAKE_EXEC_JOURNAL"])
    store = Store(Path(os.environ["FAKE_EXEC_STATE"]), Path(os.environ["FAKE_EXEC_GIT"]))
    record = {
        "dest": args.dest,
        "cmd": args.cmd,
        "opcode": f"host {opcode}",
        "stdin_bytes": len(raw),
    }
    turn = body.get("turn") if isinstance(body.get("turn"), dict) else None
    material = material_of(body)
    if isinstance(turn, dict):
        if isinstance(turn.get("task_id"), str) and turn["task_id"]:
            record["task_id"] = turn["task_id"]
        if isinstance(turn.get("base_oid"), str) and turn["base_oid"]:
            record["base_oid"] = turn["base_oid"]
        if isinstance(turn.get("turn_number"), int):
            record["turn_number"] = turn["turn_number"]
    else:
        if task_id := first_str(body, "task_id"):
            record["task_id"] = task_id
        if base_oid := first_str(body, "base_oid"):
            record["base_oid"] = base_oid
    if material and isinstance(material.get("job_id"), str) and material["job_id"]:
        record["job_id"] = material["job_id"]
    if project_id := first_str(body, "project_id"):
        record["project_id"] = project_id
    if opcode == "refresh-facts":
        journal(journal_path, record)
        return 0
    handler = HANDLERS.get(opcode)
    if handler is None:
        journal(journal_path, record)
        fail(f"unhandled host opcode {opcode}", 2)
    try:
        response = handler(body, store)
    except SystemExit:
        journal(journal_path, record)
        raise
    if isinstance(response, dict):
        head = first_str(response, "head_oid")
        task = response.get("task")
        if head is None and isinstance(task, dict):
            head = first_str(task, "head_oid")
        if head:
            record["result_oid"] = head
    journal(journal_path, record)
    if response is not None:
        emit(response)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except json.JSONDecodeError as error:
        fail(f"stdin is not JSON: {error}")
