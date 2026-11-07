from __future__ import annotations

import json
import random
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from raft_stage1.api import RaftHTTPServer
from raft_stage1.config import NodeConfig
from raft_stage1.node import RaftNode


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until(predicate, timeout: float = 6.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("condition was not met within %.1f seconds" % timeout)


def request_json(
    url: str, *, method: str = "GET", payload=None, timeout: float = 2.0
):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {} if data is None else {"Content-Type": "application/json"}
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class RunningNode:
    def __init__(self, config: NodeConfig, seed: int) -> None:
        self.node = RaftNode(config, random_source=random.Random(seed))
        self.server = RaftHTTPServer((config.host, config.port), self.node)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()
        self.node.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.node.stop()
        self.thread.join(timeout=2.0)


class RaftStage2Test(unittest.TestCase):
    def test_stage1_state_file_is_loaded_as_an_empty_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            (data_dir / "state.json").write_text(
                json.dumps({"current_term": 7, "voted_for": "node2"}),
                encoding="utf-8",
            )
            config = NodeConfig(
                node_id="node1",
                host="127.0.0.1",
                port=0,
                peers={"node2": "http://127.0.0.1:10002"},
                data_dir=data_dir,
            )

            node = RaftNode(config)

            self.assertEqual(7, node.status()["term"])
            self.assertEqual("node2", node.status()["voted_for"])
            self.assertEqual([], node.log_view()["entries"])

    def test_follower_replaces_conflicting_tail_and_persists_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = NodeConfig(
                node_id="node1",
                host="127.0.0.1",
                port=0,
                peers={"node2": "http://127.0.0.1:10002"},
                data_dir=Path(temporary),
            )
            node = RaftNode(config)

            first = node.handle_append_entries(
                {
                    "term": 1,
                    "leader_id": "node2",
                    "prev_log_index": 0,
                    "prev_log_term": 0,
                    "entries": [
                        {"term": 1, "command": {"value": "A"}},
                        {"term": 1, "command": {"value": "B"}},
                    ],
                    "leader_commit": 1,
                }
            )
            self.assertTrue(first["success"])

            second = node.handle_append_entries(
                {
                    "term": 2,
                    "leader_id": "node2",
                    "prev_log_index": 1,
                    "prev_log_term": 1,
                    "entries": [{"term": 2, "command": {"value": "C"}}],
                    "leader_commit": 1,
                }
            )
            self.assertTrue(second["success"])
            self.assertEqual(
                [{"value": "A"}, {"value": "C"}],
                [entry["command"] for entry in node.log_view()["entries"]],
            )

            restarted = RaftNode(config)
            view = restarted.log_view()
            self.assertEqual(2, view["last_index"])
            self.assertEqual(1, view["commit_index"])
            self.assertEqual({"value": "C"}, view["entries"][1]["command"])

    def test_node_with_newer_log_rejects_outdated_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = NodeConfig(
                node_id="node1",
                host="127.0.0.1",
                port=0,
                peers={"node2": "http://127.0.0.1:10002"},
                data_dir=Path(temporary),
            )
            node = RaftNode(config)
            node.handle_append_entries(
                {
                    "term": 2,
                    "leader_id": "node2",
                    "prev_log_index": 0,
                    "prev_log_term": 0,
                    "entries": [{"term": 2, "command": "newer"}],
                    "leader_commit": 0,
                }
            )

            response = node.handle_request_vote(
                {
                    "term": 3,
                    "candidate_id": "node2",
                    "last_log_index": 10,
                    "last_log_term": 1,
                }
            )
            self.assertEqual({"term": 3, "vote_granted": False}, response)

    def test_rest_replication_commit_and_follower_catch_up(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ports = [free_port() for _ in range(3)]
            ids = ["node1", "node2", "node3"]
            urls = {
                node_id: "http://127.0.0.1:%s" % port
                for node_id, port in zip(ids, ports)
            }
            configs = [
                NodeConfig(
                    node_id=node_id,
                    host="127.0.0.1",
                    port=port,
                    peers={key: value for key, value in urls.items() if key != node_id},
                    data_dir=Path(temporary) / node_id,
                    election_timeout_min=0.25,
                    election_timeout_max=0.5,
                    heartbeat_interval=0.06,
                    rpc_timeout=0.15,
                )
                for node_id, port in zip(ids, ports)
            ]
            running = [RunningNode(config, seed) for seed, config in enumerate(configs, 1)]
            for item in running:
                item.start()
            try:
                leader = wait_until(
                    lambda: next(
                        (item for item in running if item.node.status()["role"] == "leader"),
                        None,
                    )
                )
                followers = [item for item in running if item is not leader]
                wait_until(
                    lambda: all(
                        item.node.status()["leader_id"] == leader.node.config.node_id
                        for item in followers
                    )
                )

                follower_status, follower_body = request_json(
                    urls[followers[0].node.config.node_id] + "/log/entries",
                    method="POST",
                    payload={"command": {"op": "first"}},
                )
                self.assertEqual(409, follower_status)
                self.assertEqual("not_leader", follower_body["error"])
                self.assertEqual(leader.node.config.node_id, follower_body["leader_id"])

                status, body = request_json(
                    urls[leader.node.config.node_id] + "/log/entries",
                    method="POST",
                    payload={"command": {"op": "first"}, "timeout_seconds": 2.0},
                )
                self.assertEqual(201, status)
                self.assertEqual(1, body["commit_index"])

                wait_until(
                    lambda: all(
                        item.node.log_view()["commit_index"] == 1 for item in running
                    )
                )

                offline = followers[0]
                offline_config = offline.node.config
                offline.stop()
                running.remove(offline)

                status, body = request_json(
                    urls[leader.node.config.node_id] + "/log/entries",
                    method="POST",
                    payload={
                        "commands": [{"op": "second"}, {"op": "third"}],
                        "timeout_seconds": 2.0,
                    },
                )
                self.assertEqual(201, status)
                self.assertEqual(3, body["commit_index"])

                restarted = RunningNode(offline_config, seed=20)
                restarted.start()
                running.append(restarted)
                wait_until(
                    lambda: restarted.node.log_view()["last_index"] == 3
                    and restarted.node.log_view()["commit_index"] == 3
                )
                self.assertEqual(
                    [{"op": "first"}, {"op": "second"}, {"op": "third"}],
                    [
                        entry["command"]
                        for entry in restarted.node.log_view()["entries"]
                    ],
                )
            finally:
                for item in running:
                    item.stop()


if __name__ == "__main__":
    unittest.main()
