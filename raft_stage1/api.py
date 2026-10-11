"""HTTP transport for Raft RPCs and the public status endpoint."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from .node import RaftNode

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
            elif path == "/raft/append-entries":
                response = self.server.raft_node.handle_append_entries(payload)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "route not found"})
                return
            self._send_json(HTTPStatus.OK, response)
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

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
        # Keep the demo output focused on Raft role changes.
        return

