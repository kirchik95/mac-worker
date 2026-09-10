#!/usr/bin/env python3
"""Protocol-valid fake SSH for tests/controller_fail_closed.rs.

Records argv and one stdin frame (when present). Never writes empty JSON
or a success ACK. Default: length-prefixed HostControlError and exit 70.
This is not a live network hop.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import sys


def _log(entry: dict) -> None:
    path = os.environ.get("MAC_WORKER_FAIL_CLOSED_SSH_LOG")
    if not path:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":")) + "\n")


def _framed_error(code: str, message: str) -> bytes:
    payload = json.dumps(
        {
            "protocol_version": 7,
            "error": {"code": code, "message": message},
        },
        separators=(",", ":"),
    ).encode("utf-8")
    return struct.pack(">I", len(payload)) + payload


def _decode_command(stdin: bytes) -> str | None:
    if len(stdin) < 4:
        return None
    (length,) = struct.unpack(">I", stdin[:4])
    if length == 0 or length > len(stdin) - 4:
        return None
    try:
        body = json.loads(stdin[4 : 4 + length].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None
    if isinstance(body, dict):
        command = body.get("command")
        if isinstance(command, str):
            return command
    return None


def main() -> int:
    stdin = sys.stdin.buffer.read()
    remote = sys.argv[-1] if len(sys.argv) > 1 else ""
    _log(
        {
            "argv": sys.argv,
            "remote": remote,
            "stdin_len": len(stdin),
            "stdin_sha256": hashlib.sha256(stdin).hexdigest(),
            "command": _decode_command(stdin),
        }
    )
    sys.stdout.buffer.write(
        _framed_error(
            "CONTROLLER_UNAVAILABLE",
            "labelled fail-closed fake ssh refused the hop",
        )
    )
    sys.stdout.buffer.flush()
    return 70


if __name__ == "__main__":
    raise SystemExit(main())
