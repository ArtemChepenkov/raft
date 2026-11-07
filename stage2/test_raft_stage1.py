from __future__ import annotations

import json
import random
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.request import urlopen

from raft_stage1.api import RaftHTTPServer
from raft_stage1.config import NodeConfig
from raft_stage1.node import RaftNode


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until(predicate, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("condition was not met within %.1f seconds" % timeout)


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


class RaftStage1Test(unittest.TestCase):
    def test_current_term_heartbeat_makes_candidate_step_down(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            now = [0.0]
            config = NodeConfig(
                node_id="node1",
                host="127.0.0.1",
                port=0,
                peers={
                    "node2": "http://127.0.0.1:10002",
                    "node3": "http://127.0.0.1:10003",
                },
                data_dir=Path(temporary),
            )
            node = RaftNode(config, clock=lambda: now[0], random_source=random.Random(1))
            node._stopped.clear()
            now[0] = 10.0
            node._start_election()
            self.assertEqual("candidate", node.status()["role"])

            response = node.handle_append_entries(
                {
                    "term": node.status()["term"],
                    "leader_id": "node2",
                    "prev_log_index": 0,
                    "prev_log_term": 0,
                    "entries": [],
                    "leader_commit": 0,
                }
            )

            self.assertTrue(response["success"])
            self.assertEqual("follower", node.status()["role"])
            self.assertEqual("node2", node.status()["leader_id"])

    def test_vote_is_persisted_and_only_one_candidate_gets_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = NodeConfig(
                node_id="node1",
                host="127.0.0.1",
                port=0,
                peers={
                    "node2": "http://127.0.0.1:10002",
                    "node3": "http://127.0.0.1:10003",
                },
                data_dir=Path(temporary),
            )
            node = RaftNode(config)
            with self.assertLogs("raft_stage1.node", level="INFO") as captured:
                first = node.handle_request_vote(
                    {
                        "term": 4,
                        "candidate_id": "node2",
                        "last_log_index": 0,
                        "last_log_term": 0,
                    }
                )
                second = node.handle_request_vote(
                    {
                        "term": 4,
                        "candidate_id": "node3",
                        "last_log_index": 0,
                        "last_log_term": 0,
                    }
                )
            restarted = RaftNode(config)

            self.assertEqual({"term": 4, "vote_granted": True}, first)
            self.assertEqual({"term": 4, "vote_granted": False}, second)
            self.assertEqual(4, restarted.status()["term"])
            self.assertEqual("node2", restarted.status()["voted_for"])
            output = "\n".join(captured.output)
            self.assertIn("получил запрос голоса от node2", output)
            self.assertIn("голосует за node2", output)
            self.assertIn("голос уже отдан за node2", output)

    def test_three_nodes_elect_and_replace_a_failed_leader(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ports = [free_port() for _ in range(3)]
            ids = ["node1", "node2", "node3"]
            urls = {
                node_id: "http://127.0.0.1:%s" % port
                for node_id, port in zip(ids, ports)
            }
            configs = []
            for node_id, port in zip(ids, ports):
                configs.append(
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
                )

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
                first_term = leader.node.status()["term"]
                leader_id = leader.node.config.node_id

                status_url = urls[leader_id] + "/status"
                with urlopen(status_url, timeout=1.0) as response:
                    public_status = json.loads(response.read().decode("utf-8"))
                self.assertEqual(leader_id, public_status["node_id"])
                self.assertEqual("leader", public_status["role"])
                self.assertEqual(2, public_status["quorum"])

                leader.stop()
                survivors = [item for item in running if item is not leader]
                replacement = wait_until(
                    lambda: next(
                        (
                            item
                            for item in survivors
                            if item.node.status()["role"] == "leader"
                            and item.node.status()["term"] > first_term
                        ),
                        None,
                    ),
                    timeout=6.0,
                )
                self.assertNotEqual(leader_id, replacement.node.config.node_id)
                running.remove(leader)
            finally:
                for item in running:
                    item.stop()


if __name__ == "__main__":
    unittest.main()

