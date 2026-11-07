"""Thread-safe Raft leader election and log replication state machine."""

from __future__ import annotations

import copy
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


class NotLeaderError(RuntimeError):
    def __init__(self, leader_id: str | None, leader_url: str | None) -> None:
        super().__init__("this node is not the leader")
        self.leader_id = leader_id
        self.leader_url = leader_url


class CommitTimeoutError(TimeoutError):
    def __init__(self, first_index: int, last_index: int) -> None:
        super().__init__("entries were not committed before the timeout")
        self.first_index = first_index
        self.last_index = last_index


class RaftNode:
    """A small educational Raft node with election and log replication."""

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
        self._commit_condition = threading.Condition(self._lock)
        self._stopped = threading.Event()
        self._stopped.set()
        self._timer_thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None

        (
            self._current_term,
            self._voted_for,
            self._log,
            self._commit_index,
        ) = self._store.load()
        self._role = Role.FOLLOWER
        self._leader_id: str | None = None
        self._votes_received: set[str] = set()
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._replication_inflight: dict[str, int] = {}
        self._election_deadline = 0.0
        self._next_heartbeat_at = float("inf")
        self._started_at = self._clock()
        self._last_leader_contact_at: float | None = None
        self._last_election_at: float | None = None
        self._elections_started = 0
        self._leaderships_started = 0
        self._heartbeats_sent = 0
        self._heartbeats_received = 0
        self._append_entries_sent = 0
        self._append_entries_received = 0
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
        with self._commit_condition:
            timer_thread = self._timer_thread
            executor = self._executor
            self._timer_thread = None
            self._executor = None
            self._commit_condition.notify_all()
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
                self._broadcast_append_entries()

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
            last_log_index = len(self._log)
            last_log_term = self._last_log_term_locked()

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
                    last_log_index,
                    last_log_term,
                )

    def _request_vote_from_peer(
        self,
        peer_id: str,
        peer_url: str,
        election_term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> None:
        payload = {
            "term": election_term,
            "candidate_id": self.config.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
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
        next_index = len(self._log) + 1
        self._next_index = {peer_id: next_index for peer_id in self.config.peers}
        self._match_index = {peer_id: 0 for peer_id in self.config.peers}
        self._replication_inflight.clear()
        self._leaderships_started += 1
        self._next_heartbeat_at = self._clock()
        self._commit_condition.notify_all()
        LOGGER.info(
            "%s становится лидером term=%s", self.config.node_id, self._current_term
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
        self._next_index.clear()
        self._match_index.clear()
        self._next_heartbeat_at = float("inf")
        self._reset_election_deadline_locked()
        if term_changed:
            self._save_stable_state_locked()
        self._commit_condition.notify_all()

    def _broadcast_heartbeat(self) -> None:
        """Backward-compatible name retained from stage 1."""
        self._broadcast_append_entries()

    def _broadcast_append_entries(self) -> None:
        jobs: list[tuple[ThreadPoolExecutor, str, str, int]] = []
        with self._lock:
            if self._stopped.is_set() or self._role is not Role.LEADER:
                return
            term = self._current_term
            self._next_heartbeat_at = self._clock() + self.config.heartbeat_interval
            executor = self._executor
            if executor is None:
                return
            for peer_id, peer_url in self.config.peers.items():
                if self._replication_inflight.get(peer_id) == term:
                    continue
                self._replication_inflight[peer_id] = term
                jobs.append((executor, peer_id, peer_url, term))

        for executor, peer_id, peer_url, term in jobs:
            try:
                executor.submit(self._replicate_to_peer, peer_id, peer_url, term)
            except RuntimeError:
                with self._lock:
                    if self._replication_inflight.get(peer_id) == term:
                        self._replication_inflight.pop(peer_id, None)

    def _replicate_to_peer(self, peer_id: str, peer_url: str, leader_term: int) -> None:
        try:
            while not self._stopped.is_set():
                with self._lock:
                    if (
                        self._role is not Role.LEADER
                        or self._current_term != leader_term
                    ):
                        return
                    next_index = self._next_index.get(peer_id, len(self._log) + 1)
                    next_index = max(1, min(next_index, len(self._log) + 1))
                    prev_log_index = next_index - 1
                    prev_log_term = self._term_at_locked(prev_log_index)
                    entries = copy.deepcopy(self._log[next_index - 1 :])
                    leader_commit = self._commit_index
                    if entries:
                        self._append_entries_sent += 1
                    else:
                        self._heartbeats_sent += 1

                if entries:
                    LOGGER.info(
                        "%s: отправляет %s записи с index=%s узлу %s",
                        self.config.node_id,
                        len(entries),
                        next_index,
                        peer_id,
                    )

                payload = {
                    "term": leader_term,
                    "leader_id": self.config.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": leader_commit,
                }
                response = self._post_json(peer_url + "/raft/append-entries", payload)
                if response is None:
                    return

                response_term = response.get("term")
                success = response.get("success")
                match_index = response.get("match_index")
                if (
                    isinstance(response_term, bool)
                    or not isinstance(response_term, int)
                    or not isinstance(success, bool)
                    or isinstance(match_index, bool)
                    or not isinstance(match_index, int)
                ):
                    return

                with self._lock:
                    if response_term > self._current_term:
                        self._become_follower_locked(response_term, leader_id=None)
                        return
                    if (
                        self._role is not Role.LEADER
                        or self._current_term != leader_term
                    ):
                        return
                    if success:
                        acknowledged = max(0, min(match_index, len(self._log)))
                        self._match_index[peer_id] = max(
                            self._match_index.get(peer_id, 0), acknowledged
                        )
                        self._next_index[peer_id] = acknowledged + 1
                        commit_advanced = self._advance_commit_index_locked()
                        if commit_advanced:
                            self._next_heartbeat_at = self._clock()
                        if self._next_index[peer_id] > len(self._log):
                            return
                    else:
                        conflict_index = response.get("conflict_index")
                        if (
                            isinstance(conflict_index, bool)
                            or not isinstance(conflict_index, int)
                            or conflict_index < 1
                        ):
                            conflict_index = next_index - 1
                        self._next_index[peer_id] = max(
                            1, min(next_index - 1, conflict_index)
                        )
        finally:
            with self._lock:
                if self._replication_inflight.get(peer_id) == leader_term:
                    self._replication_inflight.pop(peer_id, None)

    def append_commands(
        self, commands: list[Any], *, timeout: float = 5.0
    ) -> dict[str, Any]:
        if not isinstance(commands, list) or not commands:
            raise ValueError("commands must be a non-empty list")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout <= 0
        ):
            raise ValueError("timeout must be a positive number")
        safe_commands = [self._json_copy(command) for command in commands]

        with self._commit_condition:
            if self._stopped.is_set() or self._role is not Role.LEADER:
                raise self._not_leader_error_locked()
            term = self._current_term
            first_index = len(self._log) + 1
            for command in safe_commands:
                self._log.append({"term": term, "command": command})
            last_index = len(self._log)
            self._save_stable_state_locked()
            LOGGER.info(
                "%s: добавил записи %s..%s в term=%s",
                self.config.node_id,
                first_index,
                last_index,
                term,
            )
            self._advance_commit_index_locked()

        self._broadcast_append_entries()

        deadline = self._clock() + float(timeout)
        with self._commit_condition:
            while self._commit_index < last_index:
                if (
                    self._stopped.is_set()
                    or self._role is not Role.LEADER
                    or self._current_term != term
                ):
                    raise self._not_leader_error_locked()
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise CommitTimeoutError(first_index, last_index)
                self._commit_condition.wait(remaining)
            return {
                "term": term,
                "first_index": first_index,
                "last_index": last_index,
                "commit_index": self._commit_index,
                "entries": self._entry_views_locked(first_index, last_index),
            }

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

            local_last_index = len(self._log)
            local_last_term = self._last_log_term_locked()
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
        leader_commit = self._required_non_negative_int(request, "leader_commit")
        entries = self._validated_wire_entries(request.get("entries"))

        with self._commit_condition:
            if term < self._current_term:
                return {
                    "term": self._current_term,
                    "success": False,
                    "match_index": 0,
                    "conflict_index": len(self._log) + 1,
                }

            self._become_follower_locked(term, leader_id=leader_id)
            self._last_leader_contact_at = self._clock()
            self._append_entries_received += 1
            if not entries:
                self._heartbeats_received += 1

            if prev_log_index > len(self._log):
                LOGGER.info(
                    "%s: отклоняет AppendEntries — нет prev_log_index=%s",
                    self.config.node_id,
                    prev_log_index,
                )
                return {
                    "term": self._current_term,
                    "success": False,
                    "match_index": 0,
                    "conflict_index": len(self._log) + 1,
                }
            if (
                prev_log_index > 0
                and self._term_at_locked(prev_log_index) != prev_log_term
            ):
                conflict_term = self._term_at_locked(prev_log_index)
                conflict_index = prev_log_index
                while (
                    conflict_index > 1
                    and self._term_at_locked(conflict_index - 1) == conflict_term
                ):
                    conflict_index -= 1
                LOGGER.info(
                    "%s: отклоняет AppendEntries — term на index=%s не совпал",
                    self.config.node_id,
                    prev_log_index,
                )
                return {
                    "term": self._current_term,
                    "success": False,
                    "match_index": 0,
                    "conflict_index": conflict_index,
                }

            log_changed = False
            entry_index = prev_log_index + 1
            offset = 0
            while offset < len(entries) and entry_index <= len(self._log):
                if self._term_at_locked(entry_index) != entries[offset]["term"]:
                    del self._log[entry_index - 1 :]
                    self._log.extend(copy.deepcopy(entries[offset:]))
                    log_changed = True
                    break
                entry_index += 1
                offset += 1
            if offset < len(entries) and entry_index > len(self._log):
                self._log.extend(copy.deepcopy(entries[offset:]))
                log_changed = True

            commit_changed = False
            new_commit_index = min(leader_commit, len(self._log))
            if new_commit_index > self._commit_index:
                self._commit_index = new_commit_index
                commit_changed = True

            if log_changed or commit_changed:
                self._save_stable_state_locked()
            if log_changed:
                LOGGER.info(
                    "%s: журнал обновлён до index=%s лидером %s",
                    self.config.node_id,
                    len(self._log),
                    leader_id,
                )
            if commit_changed:
                self._commit_condition.notify_all()

            match_index = prev_log_index + len(entries)
            return {
                "term": self._current_term,
                "success": True,
                "match_index": match_index,
            }

    def log_view(self) -> dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.config.node_id,
                "role": self._role.value,
                "term": self._current_term,
                "leader_id": self._leader_id,
                "commit_index": self._commit_index,
                "last_index": len(self._log),
                "entries": self._entry_views_locked(1, len(self._log)),
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
                    "last_index": len(self._log),
                    "last_term": self._last_log_term_locked(),
                    "commit_index": self._commit_index,
                    "entry_count": len(self._log),
                    "next_index": dict(self._next_index),
                    "match_index": dict(self._match_index),
                },
                "stats": {
                    "elections_started": self._elections_started,
                    "leaderships_started": self._leaderships_started,
                    "heartbeats_sent": self._heartbeats_sent,
                    "heartbeats_received": self._heartbeats_received,
                    "append_entries_sent": self._append_entries_sent,
                    "append_entries_received": self._append_entries_received,
                    "rpc_errors": self._rpc_errors,
                },
            }

    def _advance_commit_index_locked(self) -> bool:
        for index in range(len(self._log), self._commit_index, -1):
            if self._term_at_locked(index) != self._current_term:
                continue
            replicated = 1 + sum(
                1 for match_index in self._match_index.values() if match_index >= index
            )
            if replicated >= self.config.quorum:
                self._commit_index = index
                self._save_stable_state_locked()
                self._commit_condition.notify_all()
                LOGGER.info(
                    "%s: журнал подтверждён до index=%s",
                    self.config.node_id,
                    self._commit_index,
                )
                return True
        return False

    def _last_log_term_locked(self) -> int:
        return self._log[-1]["term"] if self._log else 0

    def _term_at_locked(self, index: int) -> int:
        return 0 if index == 0 else self._log[index - 1]["term"]

    def _entry_views_locked(self, first_index: int, last_index: int) -> list[dict[str, Any]]:
        result = []
        for index in range(first_index, last_index + 1):
            entry = self._log[index - 1]
            result.append(
                {
                    "index": index,
                    "term": entry["term"],
                    "command": copy.deepcopy(entry["command"]),
                    "committed": index <= self._commit_index,
                }
            )
        return result

    def _not_leader_error_locked(self) -> NotLeaderError:
        leader_url = (
            self.config.peers.get(self._leader_id) if self._leader_id is not None else None
        )
        return NotLeaderError(self._leader_id, leader_url)

    def _save_stable_state_locked(self) -> None:
        self._store.save(
            self._current_term,
            self._voted_for,
            self._log,
            self._commit_index,
        )

    @staticmethod
    def _validated_wire_entries(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise ValueError("entries must be a list")
        result = []
        for entry in value:
            if not isinstance(entry, dict):
                raise ValueError("each entry must be a JSON object")
            term = entry.get("term")
            if isinstance(term, bool) or not isinstance(term, int) or term < 0:
                raise ValueError("each entry term must be a non-negative integer")
            if "command" not in entry:
                raise ValueError("each entry must contain command")
            result.append(
                {"term": term, "command": RaftNode._json_copy(entry["command"])}
            )
        return result

    @staticmethod
    def _json_copy(value: Any) -> Any:
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
            return json.loads(encoded)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("command must be a valid JSON value") from exc

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
