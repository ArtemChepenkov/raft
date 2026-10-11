"""Small durable store for the Raft fields that must survive a restart."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class StableStateStore:
    """Persist current_term and voted_for using atomic file replacement."""

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        self._path = data_dir / "state.json"

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> tuple[int, str | None]:
        if not self._path.exists():
            return 0, None
        try:
            data: Any = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("cannot read stable Raft state %s: %s" % (self._path, exc)) from exc
        if not isinstance(data, dict):
            raise RuntimeError("stable Raft state must be a JSON object")
        term = data.get("current_term")
        voted_for = data.get("voted_for")
        if isinstance(term, bool) or not isinstance(term, int) or term < 0:
            raise RuntimeError("stable Raft current_term must be a non-negative integer")
        if voted_for is not None and not isinstance(voted_for, str):
            raise RuntimeError("stable Raft voted_for must be a string or null")
        return term, voted_for

    def save(self, current_term: int, voted_for: str | None) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(".tmp")
        payload = {
            "current_term": current_term,
            "voted_for": voted_for,
        }
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._path)

