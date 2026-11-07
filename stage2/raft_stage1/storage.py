"""Durable storage for the Raft term, vote, log and commit index."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class StableStateStore:
    """Persist Raft state using atomic file replacement.

    Old stage-1 state files contain only ``current_term`` and ``voted_for``.
    Missing stage-2 fields are loaded as an empty log with commit index zero.
    """

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._path = data_dir / "state.json"

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> tuple[int, str | None, list[dict[str, Any]], int]:
        if not self._path.exists():
            return 0, None, [], 0
        try:
            data: Any = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "cannot read stable Raft state %s: %s" % (self._path, exc)
            ) from exc
        if not isinstance(data, dict):
            raise RuntimeError("stable Raft state must be a JSON object")

        term = data.get("current_term")
        voted_for = data.get("voted_for")
        log = data.get("log", [])
        commit_index = data.get("commit_index", 0)

        if isinstance(term, bool) or not isinstance(term, int) or term < 0:
            raise RuntimeError("stable Raft current_term must be a non-negative integer")
        if voted_for is not None and not isinstance(voted_for, str):
            raise RuntimeError("stable Raft voted_for must be a string or null")
        if not isinstance(log, list):
            raise RuntimeError("stable Raft log must be a list")
        for entry in log:
            if not isinstance(entry, dict):
                raise RuntimeError("each stable Raft log entry must be an object")
            entry_term = entry.get("term")
            if (
                isinstance(entry_term, bool)
                or not isinstance(entry_term, int)
                or entry_term < 0
            ):
                raise RuntimeError("each stable Raft log term must be non-negative")
            if "command" not in entry:
                raise RuntimeError("each stable Raft log entry must contain command")
        if (
            isinstance(commit_index, bool)
            or not isinstance(commit_index, int)
            or not 0 <= commit_index <= len(log)
        ):
            raise RuntimeError("stable Raft commit_index is outside the log")
        return term, voted_for, log, commit_index

    def save(
        self,
        current_term: int,
        voted_for: str | None,
        log: list[dict[str, Any]],
        commit_index: int,
    ) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        payload = {
            "current_term": current_term,
            "voted_for": voted_for,
            "log": log,
            "commit_index": commit_index,
        }
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._path)
