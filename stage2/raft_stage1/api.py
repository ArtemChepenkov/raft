"""HTTP transport for Raft RPCs, status and log inspection."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .node import CommitTimeoutError, NotLeaderError, RaftNode

MAX_REQUEST_BYTES = 1024 * 1024


class RaftHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], node: RaftNode) -> None:
        super().__init__(address, RaftRequestHandler)
        self.raft_node = node


class RaftRequestHandler(BaseHTTPRequestHandler):
    server: RaftHTTPServer
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in ("/", "/status"):
            self._send_json(HTTPStatus.OK, self.server.raft_node.status())
            return
        if path in ("/log", "/log/entries"):
            self._send_json(HTTPStatus.OK, self.server.raft_node.log_view())
            return
        if path == "/health":
            self._send_json(
                HTTPStatus.OK,
                {"status": "ok", "node_id": self.server.raft_node.config.node_id},
            )
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def do_POST(self) -> None:
        try:
            payload = self._read_json_object()
            path = urlsplit(self.path).path
            if path == "/raft/request-vote":
                response = self.server.raft_node.handle_request_vote(payload)
                status = HTTPStatus.OK
            elif path == "/raft/append-entries":
                response = self.server.raft_node.handle_append_entries(payload)
                status = HTTPStatus.OK
            elif path == "/log/entries":
                commands = self._commands_from_payload(payload)
                timeout = payload.get("timeout_seconds", 5.0)
                response = self.server.raft_node.append_commands(
                    commands, timeout=timeout
                )
                status = HTTPStatus.CREATED
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "route not found"})
                return
            self._send_json(status, response)
        except NotLeaderError as exc:
            self._send_json(
                HTTPStatus.CONFLICT,
                {
                    "error": "not_leader",
                    "leader_id": exc.leader_id,
                    "leader_url": exc.leader_url,
                },
            )
        except CommitTimeoutError as exc:
            self._send_json(
                HTTPStatus.GATEWAY_TIMEOUT,
                {
                    "error": "commit_timeout",
                    "first_index": exc.first_index,
                    "last_index": exc.last_index,
                },
            )
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    @staticmethod
    def _commands_from_payload(payload: dict[str, Any]) -> list[Any]:
        has_command = "command" in payload
        has_commands = "commands" in payload
        if has_command == has_commands:
            raise ValueError("provide exactly one of command or commands")
        if has_command:
            return [payload["command"]]
        commands = payload["commands"]
        if not isinstance(commands, list) or not commands:
            raise ValueError("commands must be a non-empty list")
        return commands

    def _read_json_object(self) -> dict[str, Any]:
        content_type = self.headers.get_content_type()
        if content_type != "application/json":
            raise ValueError("Content-Type must be application/json")
        content_length_header = self.headers.get("Content-Length")
        if content_length_header is None:
            raise ValueError("Content-Length is required")
        try:
            content_length = int(content_length_header)
        except ValueError as exc:
            raise ValueError("Content-Length must be an integer") from exc
        if content_length < 0 or content_length > MAX_REQUEST_BYTES:
            raise ValueError("request body is too large")
        try:
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("request body must be valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
            "utf-8"
        )
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return

