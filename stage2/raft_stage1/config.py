"""Configuration loading and validation for the stage-1 Raft node."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


class ConfigError(ValueError):
    """Raised when a node configuration is incomplete or unsafe."""


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    host: str
    port: int
    peers: dict[str, str]
    data_dir: Path
    election_timeout_min: float = 1.5
    election_timeout_max: float = 3.0
    heartbeat_interval: float = 0.5
    rpc_timeout: float = 0.7

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def quorum(self) -> int:
        return self.cluster_size // 2 + 1

    def validate(self) -> None:
        if not self.node_id.strip():
            raise ConfigError("id must be a non-empty string")
        if not self.host.strip():
            raise ConfigError("host must be a non-empty string")
        if not 0 <= self.port <= 65535:
            raise ConfigError("port must be between 0 and 65535")
        if self.node_id in self.peers:
            raise ConfigError("peers must not contain this node's own id")
        if self.election_timeout_min <= 0:
            raise ConfigError("election_timeout_min must be positive")
        if self.election_timeout_max <= self.election_timeout_min:
            raise ConfigError(
                "election_timeout_max must be greater than election_timeout_min"
            )
        if self.heartbeat_interval <= 0:
            raise ConfigError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.election_timeout_min:
            raise ConfigError(
                "heartbeat_interval must be shorter than election_timeout_min"
            )
        if self.rpc_timeout <= 0:
            raise ConfigError("rpc_timeout must be positive")

        for peer_id, peer_url in self.peers.items():
            if not peer_id.strip():
                raise ConfigError("peer ids must be non-empty strings")
            parsed = urlparse(peer_url)
            if parsed.scheme != "http" or not parsed.hostname or parsed.port is None:
                raise ConfigError(
                    "peer URL for %s must look like http://host:port" % peer_id
                )
            if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
                raise ConfigError(
                    "peer URL for %s must not contain a path, query, or fragment"
                    % peer_id
                )


def _number(data: dict[str, Any], key: str, default: float) -> float:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError("%s must be a number" % key)
    return float(value)


def load_config(path: str | Path) -> NodeConfig:
    """Load a JSON config. Relative data_dir paths are based on the config file."""

    config_path = Path(path).resolve()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError("cannot read config %s: %s" % (config_path, exc)) from exc
    except json.JSONDecodeError as exc:
        raise ConfigError("invalid JSON in %s: %s" % (config_path, exc)) from exc

    if not isinstance(raw, dict):
        raise ConfigError("config root must be a JSON object")

    missing = [key for key in ("id", "host", "port", "peers", "data_dir") if key not in raw]
    if missing:
        raise ConfigError("missing config fields: %s" % ", ".join(missing))
    if not isinstance(raw["id"], str) or not isinstance(raw["host"], str):
        raise ConfigError("id and host must be strings")
    if isinstance(raw["port"], bool) or not isinstance(raw["port"], int):
        raise ConfigError("port must be an integer")
    if not isinstance(raw["peers"], dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in raw["peers"].items()
    ):
        raise ConfigError("peers must be an object mapping node ids to HTTP URLs")
    if not isinstance(raw["data_dir"], str) or not raw["data_dir"].strip():
        raise ConfigError("data_dir must be a non-empty string")

    data_dir = Path(raw["data_dir"])
    if not data_dir.is_absolute():
        data_dir = config_path.parent / data_dir

    config = NodeConfig(
        node_id=raw["id"],
        host=raw["host"],
        port=raw["port"],
        peers={key: value.rstrip("/") for key, value in raw["peers"].items()},
        data_dir=data_dir.resolve(),
        election_timeout_min=_number(raw, "election_timeout_min", 1.5),
        election_timeout_max=_number(raw, "election_timeout_max", 3.0),
        heartbeat_interval=_number(raw, "heartbeat_interval", 0.5),
        rpc_timeout=_number(raw, "rpc_timeout", 0.7),
    )
    config.validate()
    return config

