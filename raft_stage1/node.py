"""Thread-safe Raft leader election state machine (stage 1)."""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import NodeConfig
from .storage import StableStateStore

LOGGER = logging.getLogger(__name__)


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    """The election-related subset of a Raft server.

    Log replication is deliberately absent in stage 1. AppendEntries is accepted
    only as an empty heartbeat with the initial log position (index/term zero).
    """

    def __init__(
        self,
        config: NodeConfig,
        *,
        clock: Any = time.monotonic,
        random_source: random.Random | None = None,
    ) -> None:
        config.validate()
        self.config = config
        self._clock = clock
        self._random = random_source or random.Random()
        self._store = StableStateStore(config.data_dir)
        self._lock = threading.RLock()
        self._stopped = threading.Event()
        self._stopped.set()
        self._timer_thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None

        self._current_term, self._voted_for = self._store.load()
        self._role = Role.FOLLOWER
        self._leader_id: str | None = None
        self._votes_received: set[str] = set()
        self._election_deadline = 0.0
        self._next_heartbeat_at = float("inf")
        self._started_at = self._clock()
        self._last_leader_contact_at: float | None = None
        self._last_election_at: float | None = None
        self._elections_started = 0
        self._leaderships_started = 0
        self._heartbeats_sent = 0
        self._heartbeats_received = 0
        self._rpc_errors = 0
        self._reset_election_deadline_locked()

    def start(self) -> None:
        with self._lock:
            if self._timer_thread is not None:
                return
            self._stopped.clear()
            self._executor = ThreadPoolExecutor(
                max_workers=max(4, len(self.config.peers) * 2),
                thread_name_prefix="raft-rpc-%s" % self.config.node_id,
            )
            self._timer_thread = threading.Thread(
                target=self._timer_loop,
                name="raft-timer-%s" % self.config.node_id,
                daemon=True,
            )
            self._timer_thread.start()

    def stop(self) -> None:
        self._stopped.set()
        with self._lock:
            timer_thread = self._timer_thread
            executor = self._executor
            self._timer_thread = None
            self._executor = None
        if timer_thread is not None and timer_thread is not threading.current_thread():
            timer_thread.join(timeout=2.0)
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def _reset_election_deadline_locked(self) -> None:
        timeout = self._random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        )
        self._election_deadline = self._clock() + timeout

    def _timer_loop(self) -> None:
        while not self._stopped.wait(0.02):
            now = self._clock()
            with self._lock:
                role = self._role
                election_due = role is not Role.LEADER and now >= self._election_deadline
                heartbeat_due = role is Role.LEADER and now >= self._next_heartbeat_at
            if election_due:
                self._start_election()
            elif heartbeat_due:
                self._broadcast_heartbeat()

    def _start_election(self) -> None:
        with self._lock:
            now = self._clock()
            if (
                self._stopped.is_set()
                or self._role is Role.LEADER
                or now < self._election_deadline
            ):
                return
            self._role = Role.CANDIDATE
            self._current_term += 1
            election_term = self._current_term
            self._voted_for = self.config.node_id
            self._leader_id = None
            self._votes_received = {self.config.node_id}
            self._last_election_at = now
            self._elections_started += 1
            self._reset_election_deadline_locked()
            self._save_stable_state_locked()
            LOGGER.info(
                "%s: начинает выборы term=%s и голосует за себя",
                self.config.node_id,
                election_term,
            )

            if len(self._votes_received) >= self.config.quorum:
                self._become_leader_locked()
                return

            executor = self._executor
            peers = tuple(self.config.peers.items())

        if executor is not None:
            for peer_id, peer_url in peers:
                LOGGER.info(
                    "%s: просит %s проголосовать за %s в term=%s",
                    self.config.node_id,
                    peer_id,
                    self.config.node_id,
                    election_term,
                )
                executor.submit(
                    self._request_vote_from_peer,
                    peer_id,
                    peer_url,
                    election_term,
                )

    def _request_vote_from_peer(
        self, peer_id: str, peer_url: str, election_term: int
    ) -> None:
        payload = {
            "term": election_term,
            "candidate_id": self.config.node_id,
            "last_log_index": 0,
            "last_log_term": 0,
        }
        response = self._post_json(peer_url + "/raft/request-vote", payload)
        if response is None:
            LOGGER.info(
                "%s: не получил ответ на запрос голоса от %s в term=%s",
                self.config.node_id,
                peer_id,
                election_term,
            )
            return
        response_term = response.get("term")
        vote_granted = response.get("vote_granted")
        if isinstance(response_term, bool) or not isinstance(response_term, int):
            LOGGER.warning(
                "%s: получил некорректный ответ на запрос голоса от %s",
                self.config.node_id,
                peer_id,
            )
            return
        if vote_granted is True:
            LOGGER.info(
                "%s: %s проголосовал за %s в term=%s",
                self.config.node_id,
                peer_id,
                self.config.node_id,
                response_term,
            )
        else:
            LOGGER.info(
                "%s: %s отказался голосовать за %s (term ответа=%s)",
                self.config.node_id,
                peer_id,
                self.config.node_id,
                response_term,
            )
        with self._lock:
            if response_term > self._current_term:
                self._become_follower_locked(response_term, leader_id=None)
                return
            if (
                self._role is not Role.CANDIDATE
                or self._current_term != election_term
                or response_term != election_term
                or vote_granted is not True
            ):
                return
            self._votes_received.add(peer_id)
            if len(self._votes_received) >= self.config.quorum:
                self._become_leader_locked()

    def _become_leader_locked(self) -> None:
        self._role = Role.LEADER
        self._leader_id = self.config.node_id
        self._votes_received.clear()
        self._leaderships_started += 1
        self._next_heartbeat_at = self._clock()
        LOGGER.info(
            "%s becomes leader for term %s", self.config.node_id, self._current_term
        )

    def _become_follower_locked(
        self, term: int, *, leader_id: str | None
    ) -> None:
        term_changed = term > self._current_term
        if term_changed:
            self._current_term = term
            self._voted_for = None
        self._role = Role.FOLLOWER
        self._leader_id = leader_id
        self._votes_received.clear()
        self._next_heartbeat_at = float("inf")
        self._reset_election_deadline_locked()
        if term_changed:
            self._save_stable_state_locked()

    def _broadcast_heartbeat(self) -> None:
        with self._lock:
            if self._stopped.is_set() or self._role is not Role.LEADER:
                return
            term = self._current_term
            self._next_heartbeat_at = self._clock() + self.config.heartbeat_interval
            executor = self._executor
            peers = tuple(self.config.peers.items())
            self._heartbeats_sent += len(peers)
        if executor is not None:
            for _peer_id, peer_url in peers:
                executor.submit(self._send_heartbeat, peer_url, term)

    def _send_heartbeat(self, peer_url: str, term: int) -> None:
        payload = {
            "term": term,
            "leader_id": self.config.node_id,
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }
        response = self._post_json(peer_url + "/raft/append-entries", payload)
        if response is None:
            return
        response_term = response.get("term")
        if isinstance(response_term, bool) or not isinstance(response_term, int):
            return
        with self._lock:
            if response_term > self._current_term:
                self._become_follower_locked(response_term, leader_id=None)

    def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.config.rpc_timeout) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result if isinstance(result, dict) else None
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
            with self._lock:
                self._rpc_errors += 1
            return None

    def handle_request_vote(self, request: dict[str, Any]) -> dict[str, Any]:
        term = self._required_non_negative_int(request, "term")
        candidate_id = self._required_string(request, "candidate_id")
        candidate_last_index = self._required_non_negative_int(
            request, "last_log_index"
        )
        candidate_last_term = self._required_non_negative_int(request, "last_log_term")

        LOGGER.info(
            "%s: получил запрос голоса от %s, кандидат просит голос за себя "
            "в term=%s",
            self.config.node_id,
            candidate_id,
            term,
        )

        with self._lock:
            if term < self._current_term:
                LOGGER.info(
                    "%s: не голосует за %s — term кандидата %s меньше текущего %s",
                    self.config.node_id,
                    candidate_id,
                    term,
                    self._current_term,
                )
                return {"term": self._current_term, "vote_granted": False}
            if term > self._current_term:
                self._become_follower_locked(term, leader_id=None)

            local_last_index = 0
            local_last_term = 0
            log_is_up_to_date = candidate_last_term > local_last_term or (
                candidate_last_term == local_last_term
                and candidate_last_index >= local_last_index
            )
            may_vote = self._voted_for in (None, candidate_id)
            vote_granted = may_vote and log_is_up_to_date
            if vote_granted:
                self._voted_for = candidate_id
                self._leader_id = None
                self._reset_election_deadline_locked()
                self._save_stable_state_locked()
                LOGGER.info(
                    "%s: голосует за %s в term=%s",
                    self.config.node_id,
                    candidate_id,
                    self._current_term,
                )
            elif not may_vote:
                LOGGER.info(
                    "%s: не голосует за %s в term=%s — голос уже отдан за %s",
                    self.config.node_id,
                    candidate_id,
                    self._current_term,
                    self._voted_for,
                )
            else:
                LOGGER.info(
                    "%s: не голосует за %s в term=%s — журнал кандидата отстаёт",
                    self.config.node_id,
                    candidate_id,
                    self._current_term,
                )
            return {"term": self._current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, request: dict[str, Any]) -> dict[str, Any]:
        term = self._required_non_negative_int(request, "term")
        leader_id = self._required_string(request, "leader_id")
        prev_log_index = self._required_non_negative_int(request, "prev_log_index")
        prev_log_term = self._required_non_negative_int(request, "prev_log_term")
        entries = request.get("entries")
        self._required_non_negative_int(request, "leader_commit")
        if not isinstance(entries, list):
            raise ValueError("entries must be a list")

        with self._lock:
            if term < self._current_term:
                return {
                    "term": self._current_term,
                    "success": False,
                    "match_index": 0,
                }
            self._become_follower_locked(term, leader_id=leader_id)
            self._last_leader_contact_at = self._clock()
            self._heartbeats_received += 1

            # Stage 1 has an empty log. Non-empty entries belong to stage 2.
            log_matches = prev_log_index == 0 and prev_log_term == 0
            success = log_matches and not entries
            return {
                "term": self._current_term,
                "success": success,
                "match_index": 0,
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            now = self._clock()
            deadline_remaining = (
                None
                if self._role is Role.LEADER
                else max(0.0, self._election_deadline - now)
            )
            last_contact_age = (
                None
                if self._last_leader_contact_at is None
                else max(0.0, now - self._last_leader_contact_at)
            )
            return {
                "node_id": self.config.node_id,
                "role": self._role.value,
                "term": self._current_term,
                "leader_id": self._leader_id,
                "voted_for": self._voted_for,
                "peers": dict(self.config.peers),
                "cluster_size": self.config.cluster_size,
                "quorum": self.config.quorum,
                "running": not self._stopped.is_set(),
                "uptime_seconds": round(now - self._started_at, 3),
                "election_timeout_remaining_seconds": (
                    None if deadline_remaining is None else round(deadline_remaining, 3)
                ),
                "last_leader_contact_age_seconds": (
                    None if last_contact_age is None else round(last_contact_age, 3)
                ),
                "timing": {
                    "election_timeout_min_seconds": self.config.election_timeout_min,
                    "election_timeout_max_seconds": self.config.election_timeout_max,
                    "heartbeat_interval_seconds": self.config.heartbeat_interval,
                    "rpc_timeout_seconds": self.config.rpc_timeout,
                },
                "log": {
                    "last_index": 0,
                    "last_term": 0,
                    "commit_index": 0,
                    "stage": "not implemented until step 2",
                },
                "stats": {
                    "elections_started": self._elections_started,
                    "leaderships_started": self._leaderships_started,
                    "heartbeats_sent": self._heartbeats_sent,
                    "heartbeats_received": self._heartbeats_received,
                    "rpc_errors": self._rpc_errors,
                },
            }

    def _save_stable_state_locked(self) -> None:
        self._store.save(self._current_term, self._voted_for)

    @staticmethod
    def _required_non_negative_int(request: dict[str, Any], key: str) -> int:
        value = request.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("%s must be a non-negative integer" % key)
        return value

    @staticmethod
    def _required_string(request: dict[str, Any], key: str) -> str:
        value = request.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError("%s must be a non-empty string" % key)
        return value

