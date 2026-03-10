"""
core/heartbeat.py  —  C3b: Heartbeat-based Failure Detection
=============================================================
Implements:
  - HeartbeatSender   : periodically sends HEARTBEAT messages to all neighbours
  - HeartbeatReceiver : processes incoming HEARTBEAT / HEARTBEAT_ACK messages
  - HeartbeatManager  : coordinates both, runs the failure-detection sweep,
                        triggers re-discovery and exponential back-off

Failure detection parameters (from spec / task_division.pdf):
  Tsuspect = 3 * delta_t   → node marked SUSPECT after 3 missed heartbeats
  Tdead    = 6 * delta_t   → node declared DEAD, removed, C1 re-discovery fired

Exponential back-off:
  When an edge probability drops to 0 (or a node is declared dead) a back-off
  timer is started.  On reconnect the back-off window doubles (up to backoff_max).

Transport independence:
  HeartbeatManager calls self._send_fn(peer_info, message) — the transport
  layer injects this callable so the heartbeat logic stays transport-agnostic.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from .message import Message, MessageType, NodeStatus
from .node import LocalNode, NeighbourEntry, NodeInfo
from config.base_config import BaseConfig

if TYPE_CHECKING:
    from components.edge_manager import EdgeManager

logger = logging.getLogger(__name__)

# Type alias for the injected send function
SendFn = Callable[[NodeInfo, Message], None]


# ---------------------------------------------------------------------------
# HeartbeatSender
# ---------------------------------------------------------------------------

class HeartbeatSender(threading.Thread):
    """
    Sends a HEARTBEAT message to every known (non-dead) neighbour every
    delta_t seconds.
    """

    def __init__(self, node: LocalNode, send_fn: SendFn) -> None:
        super().__init__(name="HeartbeatSender", daemon=True)
        self.node    = node
        self.send_fn = send_fn
        self._stop   = threading.Event()

    def run(self) -> None:
        delta_t = self.node.config.heartbeat.delta_t
        logger.info("[HB-Sender] started, interval=%.2fs", delta_t)

        while not self._stop.wait(delta_t):
            self._beat()

    def _beat(self) -> None:
        msg = self.node.factory.heartbeat()
        targets = self.node.alive_neighbours()

        if not targets:
            logger.debug("[HB-Sender] no neighbours to ping")
            return

        for entry in targets:
            try:
                self.send_fn(entry.info, msg)
                logger.debug("[HB-Sender] → %s", entry.info.node_id[:8])
            except Exception as exc:
                logger.warning(
                    "[HB-Sender] failed to send to %s: %s",
                    entry.info.node_id[:8], exc
                )

    def stop(self) -> None:
        self._stop.set()


# ---------------------------------------------------------------------------
# HeartbeatReceiver
# ---------------------------------------------------------------------------

class HeartbeatReceiver:
    """
    Stateless handler — called by the transport layer whenever a HEARTBEAT
    or HEARTBEAT_ACK message arrives.

    It updates the node's neighbour table and sends back an ACK for
    HEARTBEAT messages (optional, depends on transport reliability).
    """

    def __init__(self, node: LocalNode, send_fn: SendFn) -> None:
        self.node    = node
        self.send_fn = send_fn

    def handle(self, msg: Message, sender_info: Optional[NodeInfo] = None) -> None:
        """
        Process an incoming HEARTBEAT or HEARTBEAT_ACK.
        sender_info can be supplied by the transport if it knows the peer's address.
        """
        # Update Lamport clock
        self.node.clock.receive(msg.lamport_ts)

        if msg.msg_type == MessageType.HEARTBEAT:
            self._handle_heartbeat(msg, sender_info)
        elif msg.msg_type == MessageType.HEARTBEAT_ACK:
            self._handle_ack(msg)
        else:
            logger.warning("[HB-Receiver] unexpected message type: %s", msg.msg_type)

    def _handle_heartbeat(self, msg: Message, sender_info: Optional[NodeInfo]) -> None:
        sender_id = msg.sender_id

        # If we don't know this sender yet and we have room, add them
        if sender_info and not self.node.get_neighbour(sender_id):
            if self.node.neighbour_count < self.node.config.gossip.fmax:
                self.node.add_neighbour(sender_info)
                logger.info("[HB-Receiver] auto-added new neighbour %s", sender_id[:8])

        self.node.record_heartbeat(sender_id)
        logger.debug("[HB-Receiver] ♥ from %s", sender_id[:8])

        # Send ACK back
        ack = self.node.factory.heartbeat_ack(msg)
        if sender_info:
            try:
                self.send_fn(sender_info, ack)
            except Exception as exc:
                logger.warning("[HB-Receiver] ACK send failed: %s", exc)

    def _handle_ack(self, msg: Message) -> None:
        sender_id = msg.sender_id
        self.node.record_heartbeat(sender_id)
        logger.debug("[HB-Receiver] ACK from %s", sender_id[:8])


# ---------------------------------------------------------------------------
# FailureDetector
# ---------------------------------------------------------------------------

class FailureDetector(threading.Thread):
    """
    Runs every delta_t / 2 seconds.  Sweeps the neighbour table and:

      missed < tsuspect_multiplier  → ALIVE (no action)
      missed >= tsuspect_multiplier → mark SUSPECT
      missed >= tdead_multiplier    → mark DEAD, remove, fire re-discovery callback
    """

    def __init__(
        self,
        node: LocalNode,
        on_node_dead: Callable[[NodeInfo], None],
    ) -> None:
        super().__init__(name="FailureDetector", daemon=True)
        self.node        = node
        self.on_node_dead = on_node_dead
        self._stop        = threading.Event()

    def run(self) -> None:
        interval = self.node.config.heartbeat.delta_t / 2
        logger.info("[FD] started, sweep interval=%.2fs", interval)

        while not self._stop.wait(interval):
            self._sweep()

    def _sweep(self) -> None:
        cfg = self.node.config.heartbeat
        now = time.time()

        dead_nodes: List[NeighbourEntry] = []

        for entry in self.node.all_neighbours():
            if entry.status == NodeStatus.DEAD:
                continue

            elapsed = now - entry.last_heartbeat
            missed  = elapsed / cfg.delta_t   # approximate missed heartbeats

            if missed >= cfg.tdead_multiplier:
                dead_nodes.append(entry)
            elif missed >= cfg.tsuspect_multiplier:
                self.node.mark_suspect(entry.info.node_id)

        for entry in dead_nodes:
            self.node.mark_dead(entry.info.node_id)
            # Fire the re-discovery callback (wired to C1 / PeerDiscovery)
            try:
                self.on_node_dead(entry.info)
            except Exception as exc:
                logger.error("[FD] on_node_dead callback error: %s", exc)

    def stop(self) -> None:
        self._stop.set()


# ---------------------------------------------------------------------------
# BackOffManager
# ---------------------------------------------------------------------------

class BackOffManager:
    """
    Manages per-edge exponential back-off.

    When an edge fails (probability → 0 or node declared dead):
      - Start back-off timer (initial = backoff_initial)
      - After timer expires, allow reconnect attempt
      - On reconnect: double the back-off delay (up to backoff_max)
      - On successful reconnect: reset delay to backoff_initial
    """

    def __init__(self, config: BaseConfig) -> None:
        self.config = config

    def start_backoff(self, entry: NeighbourEntry) -> None:
        """Call when an edge is detected as failed."""
        delay           = entry.backoff_delay
        entry.backoff_until = time.time() + delay
        # Pre-compute next back-off (doubles on next failure)
        entry.backoff_delay = min(
            delay * self.config.gossip.backoff_multiplier,
            self.config.gossip.backoff_max,
        )
        logger.info(
            "[BackOff] edge %s back-off for %.1fs (next=%.1fs)",
            entry.info.node_id[:8], delay, entry.backoff_delay
        )

    def reset_backoff(self, entry: NeighbourEntry) -> None:
        """Call on successful reconnect."""
        entry.backoff_delay = self.config.gossip.backoff_initial
        entry.backoff_until = 0.0
        logger.info("[BackOff] edge %s back-off reset", entry.info.node_id[:8])

    def can_reconnect(self, entry: NeighbourEntry) -> bool:
        return not entry.is_backoff_active()


# ---------------------------------------------------------------------------
# HeartbeatManager  —  top-level coordinator
# ---------------------------------------------------------------------------

class HeartbeatManager:
    """
    Wires together HeartbeatSender, HeartbeatReceiver, FailureDetector,
    and BackOffManager into a single start/stop interface.

    Usage (inside your transport layer):

        def send_fn(peer: NodeInfo, msg: Message) -> None:
            transport.send(peer.address, msg.to_bytes())

        def on_dead(peer: NodeInfo) -> None:
            discovery.trigger_rediscovery()   # C1 hook

        hb = HeartbeatManager(local_node, send_fn, on_dead)
        hb.start()
        ...
        hb.stop()

    Incoming messages should be routed to:
        hb.receiver.handle(msg, sender_info)
    """

    def __init__(
        self,
        node: LocalNode,
        send_fn: SendFn,
        on_node_dead: Optional[Callable[[NodeInfo], None]] = None,
        edge_manager: Optional["EdgeManager"] = None,
    ) -> None:
        self.node        = node
        self.backoff_mgr = BackOffManager(node.config)
        self.edge_manager = edge_manager   # injected after construction if needed
        self.send_fn     = send_fn

        # Congestion tracking (TCP Vegas style)
        # pending_pings: timestamps of sent heartbeats awaiting ACK
        # ema_rtt: smoothed baseline RTT per peer
        # congestion_scores: continuous score per peer, 0.0=congested → 1.0=clear
        self.pending_pings: Dict[str, float] = {}
        self.ema_rtt: Dict[str, float] = {}
        self.congestion_scores: Dict[str, float] = {}
        self.rtt_alpha = 0.125
        self._rtt_lock = threading.Lock()

        # Default no-op if no C1 hook provided yet
        _on_dead = on_node_dead or (lambda peer: logger.warning(
            "[HBM] node dead, no re-discovery hook registered: %s", peer
        ))

        self.sender   = HeartbeatSender(node, self._wrapped_send_fn)
        self.receiver = HeartbeatReceiver(node, send_fn)
        self.detector = FailureDetector(node, _on_dead)

        self._running = False

    def _wrapped_send_fn(self, peer: NodeInfo, msg: Message) -> None:
        """Intercept outgoing heartbeats to record send time for RTT calculation."""
        if msg.msg_type == MessageType.HEARTBEAT:
            with self._rtt_lock:
                self.pending_pings[peer.node_id] = time.time()
        self.send_fn(peer, msg)

    def get_congestion(self, peer_id: str) -> float:
        """
        Return the current congestion score for a peer.

        Returns a value in [0.0, 1.0]:
            1.0 = link is clear (RTT matches baseline)
            0.5 = link is moderately congested (RTT = 2x baseline)
            0.1 = link is severely congested (RTT = 10x baseline)
            1.0 = default if no data yet (assume clear)

        The EdgeManager calls this during record_outcome() to blend
        congestion into the probability update.
        """
        with self._rtt_lock:
            return self.congestion_scores.get(peer_id, 1.0)

    def set_edge_manager(self, edge_manager: "EdgeManager") -> None:
        """
        Wire in an EdgeManager after construction.
        Allows HeartbeatManager and EdgeManager to be built independently and
        then connected, avoiding any circular-dependency at import time.
        """
        self.edge_manager = edge_manager

    def start(self) -> None:
        if self._running:
            return
        self.sender.start()
        self.detector.start()
        self._running = True
        logger.info("[HBM] HeartbeatManager started for %s", self.node.address)

    def stop(self) -> None:
        self.sender.stop()
        self.detector.stop()
        self._running = False
        logger.info("[HBM] HeartbeatManager stopped")

    def handle_incoming(self, msg: Message, sender_info: Optional[NodeInfo] = None) -> None:
        """Convenience passthrough — transports call this on every heartbeat message."""
        self.receiver.handle(msg, sender_info)

        # RTT Tracking on ACK → update congestion_score
        if msg.msg_type == MessageType.HEARTBEAT_ACK:
            sender_id = msg.sender_id
            now = time.time()
            with self._rtt_lock:
                sent_time = self.pending_pings.pop(sender_id, None)
                if sent_time is not None:
                    rtt = max(0.001, now - sent_time)

                    if sender_id not in self.ema_rtt:
                        # First measurement — set baseline, assume clear
                        self.ema_rtt[sender_id] = rtt
                        self.congestion_scores[sender_id] = 1.0
                    else:
                        base_rtt = self.ema_rtt[sender_id]

                        # Congestion score = baseline / current
                        # clear:  rtt ≈ base → score ≈ 1.0
                        # congested: rtt >> base → score → 0.0
                        score = max(0.0, min(1.0, base_rtt / rtt))
                        self.congestion_scores[sender_id] = score

                        if score < 0.67:  # RTT > 1.5x baseline
                            logger.warning(
                                "[HBM] Congestion on %s: score=%.2f (RTT %.3fs vs EMA %.3fs)",
                                sender_id[:8], score, rtt, base_rtt,
                            )

                        # Update EMA baseline
                        self.ema_rtt[sender_id] = self.rtt_alpha * rtt + (1 - self.rtt_alpha) * base_rtt

    def on_edge_failure(self, node_id: str) -> None:
        """
        Called by the forwarding engine (C3a / EdgeManager) when an edge
        probability drops to 0.  Starts back-off for that neighbour and
        notifies EdgeManager of the failure event.
        """
        entry = self.node.get_neighbour(node_id)
        if entry:
            self.backoff_mgr.start_backoff(entry)
        # No call back into EdgeManager here — EdgeManager drives this event,
        # so we only touch BackOff to avoid double-firing.

    def on_edge_recovery(self, node_id: str) -> None:
        """
        Called when an edge probability rises from 0 → positive again
        (reconnect / revival event).  Resets back-off for that neighbour.
        """
        entry = self.node.get_neighbour(node_id)
        if entry:
            self.backoff_mgr.reset_backoff(entry)
        # EdgeManager fires reinforce() which already advances the probability,
        # so no further action needed here.