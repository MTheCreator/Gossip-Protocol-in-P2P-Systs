"""
components/dissemination.py  --  C3a: Gossip Dissemination Engine
==================================================================
Mounia Baddou & Oussama Laaroussi -- Gossip Protocol Project

Push-based epidemic message dissemination.

Forwarding logic (per incoming message):
  1. Deduplication    -- skip if msg_id already seen
  2. Receipt threshold -- skip if forwarded >= threshold times
  3. Local delivery   -- deliver if subscribed to the channel
  4. Probabilistic forward -- for each alive neighbour (except sender),
     roll against edge_prob; on success send and reinforce the edge

Integration:
  - send_fn           injected by the transport layer
  - edge_manager      for probability gating + reinforcement
  - node (LocalNode)  for neighbour table, dedup, forward counts, subs
"""

from __future__ import annotations

import logging
import random
import threading
from typing import Any, Callable, Dict, List, Optional, Set

from core.node import LocalNode, NeighbourEntry, NodeInfo
from core.message import Message, MessageType, NodeStatus

logger = logging.getLogger(__name__)

# Type alias: callable that sends a message to a peer and returns True on success
SendFn = Callable[[NodeInfo, Message], bool]


class DisseminationEngine:
    """
    Push-based gossip forwarding engine.

    Usage:
        engine = DisseminationEngine(node, send_fn, edge_manager)
        engine.on_message("sensors", my_callback)
        engine.publish("sensors", {"temp": 22.5})

    Incoming messages (from the transport layer) are fed via:
        engine.receive(msg, sender_info)
    """

    def __init__(
        self,
        node: LocalNode,
        send_fn: SendFn,
        edge_manager: Optional[Any] = None,  # EdgeManager (optional)
    ) -> None:
        self.node = node
        self.send_fn = send_fn
        self.edge_manager = edge_manager

        # Per-channel delivery callbacks
        self._callbacks: Dict[str, List[Callable[[Message], None]]] = {}
        self._cb_lock = threading.Lock()

        # Per-message delivery tracking (instance-level)
        self._delivered_ids: Set[str] = set()
        self._delivered_lock = threading.Lock()

        # Stats
        self._published: int = 0
        self._forwarded: int = 0
        self._delivered: int = 0

    # ------------------------------------------------------------------
    # Subscription / callbacks
    # ------------------------------------------------------------------

    def on_message(self, channel: str, callback: Callable[[Message], None]) -> None:
        """Register a delivery callback for a channel."""
        with self._cb_lock:
            self._callbacks.setdefault(channel, []).append(callback)

    # ------------------------------------------------------------------
    # Publishing (originate a new message)
    # ------------------------------------------------------------------

    def publish(self, channel: str, content: Any) -> Message:
        """
        Create and disseminate a new gossip message.
        Returns the created Message.
        """
        msg = self.node.factory.gossip(channel, content)
        self.node.mark_seen(msg.msg_id)
        self.node.increment_forward_count(msg.msg_id)

        # Self-deliver if subscribed
        if self.node.is_subscribed(channel):
            self._deliver(msg)

        # Forward to neighbours
        self._forward(msg, exclude_id=None)

        self._published += 1
        logger.info(
            "[Dissemination] Published msg %s on '%s'",
            msg.msg_id[:8], channel,
        )
        return msg

    # ------------------------------------------------------------------
    # Receiving (called by transport layer)
    # ------------------------------------------------------------------

    def receive(self, msg: Message, sender_info: Optional[NodeInfo] = None) -> None:
        """
        Process an incoming gossip message.

        Steps:
          1. Dedup check (msg_id already seen?)
          2. Receipt threshold check (forwarded too many times?)
          3. Update Lamport clock
          4. Local delivery (if subscribed)
          5. Probabilistic forwarding to neighbours
        """
        # 1. Already processed beyond threshold?
        if not self.node.should_forward(msg.msg_id):
            logger.debug(
                "[Dissemination] Dropping msg %s (receipt threshold reached)",
                msg.msg_id[:8],
            )
            return

        # 2. Update Lamport clock
        self.node.clock.receive(msg.lamport_ts)

        # 3. Mark seen
        self.node.mark_seen(msg.msg_id)

        # 4. Local delivery (only once per message)
        if msg.msg_type == MessageType.GOSSIP:
            if self.node.is_subscribed(msg.channel) and not self._already_delivered(msg.msg_id):
                self._deliver(msg)

        # 5. Forward to neighbours (except the sender)
        count = self.node.increment_forward_count(msg.msg_id)
        if count <= self.node.config.gossip.receipt_threshold:
            sender_id = sender_info.node_id if sender_info else msg.sender_id
            self._forward(msg, exclude_id=sender_id)

    # ------------------------------------------------------------------
    # Internal: delivery
    # ------------------------------------------------------------------

    def _already_delivered(self, msg_id: str) -> bool:
        with self._delivered_lock:
            return msg_id in self._delivered_ids

    def _deliver(self, msg: Message) -> None:
        """Deliver to local callbacks."""
        with self._delivered_lock:
            if msg.msg_id in self._delivered_ids:
                return
            self._delivered_ids.add(msg.msg_id)

        self._delivered += 1
        logger.info(
            "[Dissemination] DELIVERED ch='%s' from=%s content=%s",
            msg.channel, msg.sender_id[:8], msg.content,
        )

        with self._cb_lock:
            for cb in self._callbacks.get(msg.channel, []):
                try:
                    cb(msg)
                except Exception as exc:
                    logger.error("[Dissemination] Callback error: %s", exc)

    # ------------------------------------------------------------------
    # Internal: forwarding
    # ------------------------------------------------------------------

    def _forward(self, msg: Message, exclude_id: Optional[str] = None) -> None:
        """
        Forward message to alive neighbours.

        For each neighbour:
          - Skip the sender (avoid echo)
          - Roll against edge_prob (probabilistic forwarding)
          - On success: send via transport, reinforce edge
        """
        neighbours = self.node.alive_neighbours()
        if not neighbours:
            return

        for entry in neighbours:
            nid = entry.info.node_id

            # Don't send back to whoever sent it to us
            if nid == exclude_id:
                continue

            # Probabilistic forwarding: roll against edge probability
            if entry.edge_prob < 1.0 and random.random() > entry.edge_prob:
                logger.debug(
                    "[Dissemination] Skipped %s (prob=%.2f, roll failed)",
                    nid[:8], entry.edge_prob,
                )
                continue

            # Send
            try:
                success = self.send_fn(entry.info, msg)
            except Exception as exc:
                logger.warning(
                    "[Dissemination] Send to %s failed: %s", nid[:8], exc,
                )
                # Record failure outcome for EMA reliability scoring
                if self.edge_manager:
                    self.edge_manager.record_outcome(nid, success=False)
                continue

            # Record the delivery outcome (success or failure)
            if self.edge_manager:
                self.edge_manager.record_outcome(nid, success=success)

            if success:
                self._forwarded += 1

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "published": self._published,
            "forwarded": self._forwarded,
            "delivered": self._delivered,
        }

    def __repr__(self) -> str:
        return (
            f"DisseminationEngine(published={self._published}, "
            f"forwarded={self._forwarded}, delivered={self._delivered})"
        )
