"""Command-line entry point: python -m raft_stage1 --config node1.json."""

from __future__ import annotations

import argparse
import logging
import signal
import threading

from .api import RaftHTTPServer
from .config import ConfigError, load_config
from .node import RaftNode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the stage-1 Raft node (election + status REST API)."
    )
    parser.add_argument("--config", required=True, help="path to a node JSON config")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        raise SystemExit("configuration error: %s" % exc) from exc

    node = RaftNode(config)
    server = RaftHTTPServer((config.host, config.port), node)
    stopped = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        if not stopped.is_set():
            stopped.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

    for signal_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), request_stop)

    node.start()
    host, port = server.server_address[:2]
    logging.getLogger(__name__).info(
        "node %s listens on http://%s:%s", config.node_id, host, port
    )
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

