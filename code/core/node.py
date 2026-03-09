"""
core/node.py  —  Node Identity & Neighbour Table
=================================================
Defines:
  - NodeInfo      : lightweight record describing a remote peer
  - NeighbourEntry: per-neighbour state (edge probability, back-off, status)
  - LocalNode     : the running node — owns its ID, clock, factory, and
                    neighbour table.

This module is transport-agnostic.  The transport layer (gRPC / UDP) calls
into LocalNode to update neighbour state; it never mutates the table directly.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

from .message import (
    LamportClock,
    Message,
    MessageFactory,
    MessageType,
    NodeStatus,
)
from config.base_config import BaseConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NodeInfo — identity record for any peer (local or remote)
# ---------------------------------------------------------------------------

@dataclass
class NodeInfo:
    """
    Immutable identity of a node in the overlay.
    node_id is a 40-char hex string (160-bit SHA-1 of host:port by default).
    """
    node_id: str
    host: str
    port: int

    @classmethod
    def from_address(cls, host: str, port: int) -> "NodeInfo":
        raw = f"{host}:{port}".encode()
        node_id = hashlib.sha1(raw).hexdigest()
        return cls(node_id=node_id, host=host, port=port)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def __hash__(self):
        return hash(self.node_id)

    def __eq__(self, other):
        return isinstance(other, NodeInfo) and self.node_id == other.node_id

    def __repr__(self):
        return f"NodeInfo(id={self.node_id[:8]}…, addr={self.address})"


# ---------------------------------------------------------------------------
# NeighbourEntry — per-edge state
# ---------------------------------------------------------------------------

@dataclass
class NeighbourEntry:
    """
    Runtime state for a single neighbour edge.

    edge_prob        : forwarding probability [0.0 – 1.0]
    status           : ALIVE / SUSPECT / DEAD
    last_heartbeat   : wall-clock time of last received heartbeat
    missed_beats     : consecutive missed heartbeats
    backoff_delay    : current back-off wait (seconds) after edge failure
    backoff_until    : timestamp when back-off period ends
    """
    info: NodeInfo
    edge_prob: float              = 1.0
    status: NodeStatus            = NodeStatus.ALIVE
    reliability_score: float      = 1.0   # EMA of delivery outcomes [0.0–1.0]
    last_heartbeat: float         = field(default_factory=time.time)
    missed_beats: int             = 0
    backoff_delay: float          = 1.0
    backoff_until: float          = 0.0

    def is_backoff_active(self) -> bool:
        return time.time() < self.backoff_until

    def touch(self) -> None:
        """Record a successful heartbeat receipt."""
        self.last_heartbeat = time.time()
        self.missed_beats   = 0
        self.status         = NodeStatus.ALIVE

    def __repr__(self):
        return (
            f"NeighbourEntry({self.info.node_id[:8]}…, "
            f"prob={self.edge_prob:.2f}, status={self.status.value}, "
            f"missed={self.missed_beats})"
        )


# ---------------------------------------------------------------------------
# LocalNode
# ---------------------------------------------------------------------------

class LocalNode:
    """
    The running node.

    Responsibilities:
      - Own a stable NodeInfo (ID, host, port).
      - Maintain a LamportClock and MessageFactory.
      - Manage the neighbour table (add / remove / iterate).
      - Expose thread-safe helpers used by heartbeat and dissemination layers.
      - Track seen message IDs for deduplication.
      - Track per-message forward counts (receipt threshold).
    """

    def __init__(self, host: str, port: int, config: BaseConfig) -> None:
        self.info    = NodeInfo.from_address(host, port)
        self.config  = config
        self.clock   = LamportClock()
        self.factory = MessageFactory(self.info.node_id, self.clock)

        # neighbour_id -> NeighbourEntry
        self._neighbours: Dict[str, NeighbourEntry] = {}
        self._nb_lock = threading.RLock()

        # Deduplication: set of seen msg_id values
        self._seen_ids: set = set()
        self._seen_lock = threading.Lock()

        # Forward counter: msg_id -> int
        self._forward_counts: Dict[str, int] = {}
        self._fc_lock = threading.Lock()

        # Subscribed channels
        self._subscriptions: set = set()
        self._sub_lock = threading.Lock()

        logger.info("LocalNode created: %s", self.info)

    # ------------------------------------------------------------------
    # Identity helpers
    # ------------------------------------------------------------------

    @property
    def node_id(self) -> str:
        return self.info.node_id

    @property
    def address(self) -> str:
        return self.info.address

    # ------------------------------------------------------------------
    # Neighbour management
    # ------------------------------------------------------------------

    def add_neighbour(self, peer: NodeInfo, edge_prob: float = 1.0) -> bool:
        """
        Add or update a neighbour.
        Returns False if fmax is already reached and peer is not known.
        """
        with self._nb_lock:
            if peer.node_id in self._neighbours:
                # Already known — just update probability
                self._neighbours[peer.node_id].edge_prob = edge_prob
                return True

            if len(self._neighbours) >= self.config.gossip.fmax:
                logger.debug(
                    "fmax=%d reached, rejecting neighbour %s",
                    self.config.gossip.fmax, peer
                )
                return False

            self._neighbours[peer.node_id] = NeighbourEntry(
                info=peer, edge_prob=edge_prob,
                backoff_delay=self.config.gossip.backoff_initial,
            )
            logger.info("Added neighbour %s (total=%d)", peer, len(self._neighbours))
            return True

    def remove_neighbour(self, node_id: str) -> Optional[NeighbourEntry]:
        with self._nb_lock:
            entry = self._neighbours.pop(node_id, None)
            if entry:
                logger.info("Removed neighbour %s (total=%d)", node_id[:8], len(self._neighbours))
            return entry

    def get_neighbour(self, node_id: str) -> Optional[NeighbourEntry]:
        with self._nb_lock:
            return self._neighbours.get(node_id)

    def all_neighbours(self) -> List[NeighbourEntry]:
        with self._nb_lock:
            return list(self._neighbours.values())

    def alive_neighbours(self) -> List[NeighbourEntry]:
        with self._nb_lock:
            return [
                e for e in self._neighbours.values()
                if e.status != NodeStatus.DEAD
            ]

    @property
    def neighbour_count(self) -> int:
        with self._nb_lock:
            return len(self._neighbours)

    def needs_more_neighbours(self) -> bool:
        return self.neighbour_count < self.config.gossip.fmin

    # ------------------------------------------------------------------
    # Heartbeat helpers (called by HeartbeatManager)
    # ------------------------------------------------------------------

    def record_heartbeat(self, sender_id: str) -> None:
        """Mark a neighbour as alive on heartbeat receipt."""
        with self._nb_lock:
            entry = self._neighbours.get(sender_id)
            if entry:
                entry.touch()

    def mark_suspect(self, node_id: str) -> None:
        with self._nb_lock:
            entry = self._neighbours.get(node_id)
            if entry and entry.status == NodeStatus.ALIVE:
                entry.status = NodeStatus.SUSPECT
                logger.warning("Node SUSPECT: %s", node_id[:8])

    def mark_dead(self, node_id: str) -> None:
        with self._nb_lock:
            entry = self._neighbours.get(node_id)
            if entry:
                entry.status = NodeStatus.DEAD
                logger.error("Node DEAD: %s — removing from table", node_id[:8])
        self.remove_neighbour(node_id)

    # ------------------------------------------------------------------
    # Deduplication & receipt threshold
    # ------------------------------------------------------------------

    def is_seen(self, msg_id: str) -> bool:
        with self._seen_lock:
            return msg_id in self._seen_ids

    def mark_seen(self, msg_id: str) -> None:
        with self._seen_lock:
            self._seen_ids.add(msg_id)

    def increment_forward_count(self, msg_id: str) -> int:
        """Returns new count. Caller checks against receipt_threshold."""
        with self._fc_lock:
            self._forward_counts[msg_id] = self._forward_counts.get(msg_id, 0) + 1
            return self._forward_counts[msg_id]

    def should_forward(self, msg_id: str) -> bool:
        with self._fc_lock:
            count = self._forward_counts.get(msg_id, 0)
        return count < self.config.gossip.receipt_threshold

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    def subscribe(self, channel: str) -> None:
        with self._sub_lock:
            self._subscriptions.add(channel)

    def unsubscribe(self, channel: str) -> None:
        with self._sub_lock:
            self._subscriptions.discard(channel)

    def is_subscribed(self, channel: str) -> bool:
        with self._sub_lock:
            return channel in self._subscriptions

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"LocalNode(id={self.node_id[:8]}…, addr={self.address}, "
            f"neighbours={self.neighbour_count}, clock={self.clock.time})"
        )