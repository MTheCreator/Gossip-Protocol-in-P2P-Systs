"""
components/dissemination.py  --  C3a: Gossip Dissemination Engine
==================================================================
Mounia Baddou & Oussama Laaroussi -- Gossip Protocol Project

Push-based epidemic message dissemination with total-order support.

Forwarding logic (per incoming message):
  1. Deduplication    -- skip if msg_id already seen
  2. Receipt threshold -- skip if forwarded >= threshold times
  3. Local delivery   -- deliver if subscribed to the channel
  4. Probabilistic forward -- for each alive neighbour (except sender),
     roll against edge_prob; on success send and reinforce the edge

Total-order support (when channels have a configured sequencer):
  - publish() sends GOSSIP to the sequencer instead of gossipping directly
  - SEQUENCE_ASSIGN messages are fed into per-channel OrderedDeliveryBuffers
  - Gaps trigger HISTORY_REQUEST to core nodes
  - HISTORY_RESPONSE fills gaps in the delivery buffer

Integration:
  - send_fn           injected by the transport layer
  - edge_manager      for probability gating + reinforcement
  - node (LocalNode)  for neighbour table, dedup, forward counts, subs
  - store (optional)  for disk persistence of delivered messages
"""

from __future__ import annotations

import logging
import random
import threading
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

from core.node import LocalNode, NeighbourEntry, NodeInfo
from core.message import Message, MessageType, NodeStatus

if TYPE_CHECKING:
    from components.sequencer import ChannelSequencer
    from components.ordered_delivery import OrderedDeliveryBuffer
    from components.storage import MessageStore
    from components.core_manager import CoreNodeManager

logger = logging.getLogger(__name__)

# Type alias: callable that sends a message to a peer and returns True on success
SendFn = Callable[[NodeInfo, Message], bool]


class DisseminationEngine:
    """
    Push-based gossip forwarding engine with total-order channel support.

    Usage:
        engine = DisseminationEngine(node, send_fn, edge_manager)
        engine.on_message("sensors", my_callback)
        engine.publish("sensors", {"temp": 22.5})

    For total-order channels:
        engine.register_sequencer("sensors", sequencer)
        engine.subscribe_channel("sensors", my_callback, request_history=True)

    Incoming messages (from the transport layer) are fed via:
        engine.receive(msg, sender_info)
    """

    def __init__(
        self,
        node: LocalNode,
        send_fn: SendFn,
        edge_manager: Optional[Any] = None,  # EdgeManager (optional)
        store: Optional["MessageStore"] = None,
    ) -> None:
        self.node = node
        self.send_fn = send_fn
        self.edge_manager = edge_manager
        self.store = store

        # Per-channel delivery callbacks
        self._callbacks: Dict[str, List[Callable[[Message], None]]] = {}
        self._cb_lock = threading.Lock()

        # Per-message delivery tracking (instance-level)
        self._delivered_ids: Set[str] = set()
        self._delivered_lock = threading.Lock()

        # --- Total order support ---
        # Channels where this node acts as sequencer
        self._sequencers: Dict[str, "ChannelSequencer"] = {}

        # Per-channel ordered delivery buffers (for subscribed channels)
        self._delivery_buffers: Dict[str, "OrderedDeliveryBuffer"] = {}

        # Per-channel core node lists (for history requests)
        self._core_nodes: Dict[str, List[NodeInfo]] = {}

        # Per-channel sequencer addresses (for routing publishes)
        self._sequencer_nodes: Dict[str, NodeInfo] = {}

        # Background gap checker thread
        self._gap_checker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Dynamic core node managers
        self._core_managers: Dict[str, "CoreNodeManager"] = {}

        # Delivered message history (for monitoring — last N per channel)
        self._delivered_messages: Dict[str, List[Dict[str, Any]]] = {}
        self._max_delivered_history: int = 200

        # Callback when sequencer is unreachable (wired by server)
        self._on_sequencer_unreachable: Optional[Callable[[str], None]] = None

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
    # Total order: registration
    # ------------------------------------------------------------------

    def register_sequencer(self, channel: str, sequencer: "ChannelSequencer") -> None:
        """Register this node as the sequencer for a channel."""
        self._sequencers[channel] = sequencer
        logger.info(
            "[Dissemination] Registered as sequencer for '%s'", channel
        )

    def register_sequencer_node(self, channel: str, seq_info: NodeInfo) -> None:
        """Register the remote sequencer node for a channel."""
        self._sequencer_nodes[channel] = seq_info
        logger.info(
            "[Dissemination] Sequencer for '%s' is %s",
            channel, seq_info.node_id[:8],
        )

    def register_core_manager(
        self, channel: str, manager: "CoreNodeManager"
    ) -> None:
        """Register a CoreNodeManager for a channel."""
        self._core_managers[channel] = manager
        # Sync core nodes from manager
        self._core_nodes[channel] = manager.core_node_infos
        logger.info(
            "[Dissemination] Registered core manager for '%s' (%d cores)",
            channel, len(self._core_nodes.get(channel, [])),
        )

    def update_core_nodes_from_manager(self, channel: str) -> None:
        """Sync the _core_nodes list from the channel's CoreNodeManager."""
        mgr = self._core_managers.get(channel)
        if mgr:
            self._core_nodes[channel] = mgr.core_node_infos

    def set_on_sequencer_unreachable(
        self, callback: Callable[[str], None]
    ) -> None:
        """Set callback for when publish() fails to reach the sequencer."""
        self._on_sequencer_unreachable = callback

    def get_delivered_messages(
        self, channel: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return the last `limit` delivered messages for a channel, with content."""
        msgs = self._delivered_messages.get(channel, [])
        return msgs[-limit:]

    def register_core_nodes(self, channel: str, core_infos: List[NodeInfo]) -> None:
        """Register core nodes for a channel (used for history requests)."""
        self._core_nodes[channel] = core_infos
        logger.info(
            "[Dissemination] Registered %d core nodes for '%s'",
            len(core_infos), channel,
        )

    def subscribe_channel(
        self,
        channel: str,
        callback: Callable[[Message], None],
        request_history: bool = True,
    ) -> None:
        """
        Subscribe to a channel with total-order delivery.

        Creates an OrderedDeliveryBuffer for the channel.  Optionally
        requests full history from a core node so the subscriber starts
        with the complete message log.
        """
        from components.ordered_delivery import OrderedDeliveryBuffer

        self.node.subscribe(channel)
        self.on_message(channel, callback)

        # Determine initial seq from storage if available
        initial_seq = 0
        if self.store:
            state = self.store.load_channel_state(channel)
            if state:
                initial_seq = state["last_delivered_seq"]

        buf = OrderedDeliveryBuffer(
            channel=channel,
            deliver_callback=lambda msg: self._ordered_deliver(channel, msg),
            request_history_fn=self._request_history_from_core,
            initial_seq=initial_seq,
        )
        self._delivery_buffers[channel] = buf

        # Wire the delivery buffer into the core manager so it can
        # check eligibility (pending_count, last_delivered_seq)
        mgr = self._core_managers.get(channel)
        if mgr:
            mgr.set_delivery_buffer(buf)

        logger.info(
            "[Dissemination] Subscribed to '%s' (initial_seq=%d)",
            channel, initial_seq,
        )

        if request_history and initial_seq == 0:
            # New subscriber — request full history
            self._request_history_from_core(channel, 1, -1)

    # ------------------------------------------------------------------
    # Publishing (originate a new message)
    # ------------------------------------------------------------------

    def publish(self, channel: str, content: Any) -> Message:
        """
        Create and disseminate a new gossip message.

        If this channel has a configured sequencer:
          - If WE are the sequencer → assign seq_num and broadcast
          - Otherwise → send GOSSIP directly to the sequencer node

        If no sequencer is configured → classic probabilistic gossip.

        Returns the created Message.
        """
        msg = self.node.factory.gossip(channel, content)
        self.node.mark_seen(msg.msg_id)
        self.node.increment_forward_count(msg.msg_id)

        # Check if this channel has a sequencer
        if channel in self._sequencers:
            # We ARE the sequencer — assign seq_num and broadcast
            seq_msg = self._sequencers[channel].handle_gossip(msg)
            # Also deliver to ourselves if subscribed
            if channel in self._delivery_buffers:
                seq_num = seq_msg.metadata.get("seq_num")
                if seq_num is not None:
                    self._delivery_buffers[channel].receive(seq_num, seq_msg)
            self._published += 1
            return msg

        if channel in self._sequencer_nodes:
            # Route to the remote sequencer
            seq_node = self._sequencer_nodes[channel]
            try:
                success = self.send_fn(seq_node, msg)
                if not success:
                    raise ConnectionError("Send returned False")
                logger.info(
                    "[Dissemination] Sent msg %s to sequencer for '%s'",
                    msg.msg_id[:8], channel,
                )
            except Exception as exc:
                logger.warning(
                    "[Dissemination] Failed to send to sequencer: %s", exc
                )
                # Trigger sequencer discovery for non-core nodes
                if self._on_sequencer_unreachable:
                    self._on_sequencer_unreachable(channel)
            self._published += 1
            return msg

        # No sequencer — classic gossip
        if self.node.is_subscribed(channel):
            self._deliver(msg)

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
        Process an incoming message.

        Handles both GOSSIP and SEQUENCE_ASSIGN message types.
        """
        if msg.msg_type == MessageType.SEQUENCE_ASSIGN:
            self._handle_sequence_assign(msg, sender_info)
            return

        if msg.msg_type != MessageType.GOSSIP:
            return

        # Duplicate suppression: once a message is seen locally, do not
        # re-process or re-forward it. This prevents forwarding loops from
        # starving heartbeat handling in dense topologies.
        if self.node.is_seen(msg.msg_id):
            return

        # --- Standard GOSSIP handling ---

        # 1. Already processed beyond threshold?
        if not self.node.should_forward(msg.msg_id):
            return

        # 2. Update Lamport clock
        self.node.clock.receive(msg.lamport_ts)

        # 3. Mark seen
        self.node.mark_seen(msg.msg_id)

        # 4. If we are the sequencer for this channel, sequence it
        channel = msg.channel
        if channel in self._sequencers:
            seq_msg = self._sequencers[channel].handle_gossip(msg)
            # Also deliver to ourselves
            if channel in self._delivery_buffers:
                seq_num = seq_msg.metadata.get("seq_num")
                if seq_num is not None:
                    self._delivery_buffers[channel].receive(seq_num, seq_msg)
            return  # sequencer handles broadcasting

        # 5. Local delivery (only once per message)
        if self.node.is_subscribed(channel) and not self._already_delivered(msg.msg_id):
            # If this channel has ordered delivery, DON'T deliver unsequenced messages
            if channel not in self._delivery_buffers:
                self._deliver(msg)

        # 6. Forward to neighbours (except the sender)
        count = self.node.increment_forward_count(msg.msg_id)
        if count <= self.node.config.gossip.receipt_threshold:
            sender_id = sender_info.node_id if sender_info else msg.sender_id
            self._forward(msg, exclude_id=sender_id)

    # ------------------------------------------------------------------
    # SEQUENCE_ASSIGN handling
    # ------------------------------------------------------------------

    def _handle_sequence_assign(
        self, msg: Message, sender_info: Optional[NodeInfo] = None
    ) -> None:
        """
        Handle a SEQUENCE_ASSIGN message from the sequencer.

        1. Dedup check
        2. Feed into the channel's OrderedDeliveryBuffer
        3. Forward to neighbours (standard gossip)
        """
        # Duplicate suppression first: SEQUENCE_ASSIGN already fan-outs from
        # the sequencer, so re-gossiping duplicates causes avoidable storms.
        if self.node.is_seen(msg.msg_id):
            return

        if not self.node.should_forward(msg.msg_id):
            return

        self.node.clock.receive(msg.lamport_ts)
        self.node.mark_seen(msg.msg_id)

        channel = msg.channel
        seq_num = msg.metadata.get("seq_num")

        # Feed into delivery buffer if we're subscribed
        if seq_num is not None and channel in self._delivery_buffers:
            self._delivery_buffers[channel].receive(seq_num, msg)

        # Forward to neighbours
        count = self.node.increment_forward_count(msg.msg_id)
        if count <= self.node.config.gossip.receipt_threshold:
            sender_id = sender_info.node_id if sender_info else msg.sender_id
            self._forward(msg, exclude_id=sender_id)

    # ------------------------------------------------------------------
    # History request / response
    # ------------------------------------------------------------------

    def handle_history_request(
        self, msg: Message, sender_info: Optional[NodeInfo] = None
    ) -> None:
        """Handle HISTORY_REQUEST — delegate to sequencer if we have one."""
        channel = msg.channel
        if channel in self._sequencers and sender_info:
            self._sequencers[channel].handle_history_request(msg, sender_info)
        elif self.store and sender_info:
            # We're a core node with storage — serve from disk
            from_seq = msg.metadata.get("from_seq", 1)
            to_seq = msg.metadata.get("to_seq", -1)
            if to_seq < 0:
                to_seq = self.store.get_last_seq(channel)
            msg_dicts = self.store.get_messages(channel, from_seq, to_seq)

            batch_size = 100
            for ch_cfg in self.node.config.channels:
                if ch_cfg.channel_name == channel:
                    batch_size = ch_cfg.history_batch_size
                    break

            for i in range(0, len(msg_dicts), batch_size):
                batch = msg_dicts[i : i + batch_size]
                response = self.node.factory.history_response(channel, batch)
                try:
                    self.send_fn(sender_info, response)
                except Exception as exc:
                    logger.warning(
                        "[Dissemination] Failed to send history: %s", exc
                    )

    def handle_history_response(self, msg: Message) -> None:
        """Handle HISTORY_RESPONSE — feed recovered messages into delivery buffer."""
        channel = msg.channel
        if not isinstance(msg.content, list):
            return

        buf = self._delivery_buffers.get(channel)
        if buf is None:
            return

        recovered = 0
        for msg_dict in msg.content:
            try:
                recovered_msg = Message.from_dict(msg_dict)
                seq_num = msg_dict.get("metadata", {}).get("seq_num")
                if seq_num is None:
                    seq_num = recovered_msg.metadata.get("seq_num")
                if seq_num is not None:
                    buf.receive(seq_num, recovered_msg)
                    recovered += 1
            except Exception as exc:
                logger.debug("[Dissemination] Skipping bad history entry: %s", exc)

        logger.info(
            "[Dissemination] Recovered %d messages for '%s' from history",
            recovered, channel,
        )

    def _request_history_from_core(
        self, channel: str, from_seq: int, to_seq: int
    ) -> None:
        """Send HISTORY_REQUEST to a core node (or sequencer) for the channel."""
        # Try core nodes first, then the sequencer
        targets = list(self._core_nodes.get(channel, []))
        if channel in self._sequencer_nodes:
            seq_node = self._sequencer_nodes[channel]
            if seq_node not in targets:
                targets.append(seq_node)

        if not targets:
            logger.warning(
                "[Dissemination] No core nodes for '%s' to request history", channel
            )
            return

        target = random.choice(targets)
        req = self.node.factory.history_request(channel, from_seq, to_seq)
        try:
            self.send_fn(target, req)
            logger.info(
                "[Dissemination] Sent HISTORY_REQUEST [%d..%d] to %s for '%s'",
                from_seq, to_seq, target.node_id[:8], channel,
            )
        except Exception as exc:
            logger.warning(
                "[Dissemination] Failed to request history: %s", exc
            )

    # ------------------------------------------------------------------
    # Ordered delivery callback
    # ------------------------------------------------------------------

    def _ordered_deliver(self, channel: str, msg: Message) -> None:
        """
        Called by OrderedDeliveryBuffer when a message is ready for
        in-order delivery.
        """
        # Store to disk if enabled
        seq_num = msg.metadata.get("seq_num")
        if self.store and seq_num is not None:
            self.store.store_message(channel, seq_num, msg.to_dict())

        self._delivered += 1
        logger.info(
            "[Dissemination] ORDERED DELIVER ch='%s' seq=%s from=%s",
            channel, seq_num, msg.sender_id[:8],
        )

        # Track delivered message with content (for monitoring)
        msg_record: Dict[str, Any] = {
            "seq_num": seq_num,
            "sender_id": msg.sender_id,
            "timestamp": msg.timestamp,
            "msg_id": msg.msg_id,
        }
        # Extract original content from SEQUENCE_ASSIGN wrapper
        if msg.msg_type == MessageType.SEQUENCE_ASSIGN and isinstance(msg.content, dict):
            msg_record["content"] = msg.content.get("content", msg.content)
            msg_record["original_sender"] = msg.content.get("sender_id", msg.sender_id)
        else:
            msg_record["content"] = msg.content
        self._delivered_messages.setdefault(channel, []).append(msg_record)
        if len(self._delivered_messages[channel]) > self._max_delivered_history:
            self._delivered_messages[channel] = \
                self._delivered_messages[channel][-self._max_delivered_history:]

        # Fire callbacks
        with self._cb_lock:
            for cb in self._callbacks.get(channel, []):
                try:
                    cb(msg)
                except Exception as exc:
                    logger.error("[Dissemination] Callback error: %s", exc)

        # Check if this node should request to join core group
        mgr = self._core_managers.get(channel)
        if mgr and not mgr.is_core and not mgr._join_requested:
            # Get sequencer info to send the request
            seq_info = self._sequencer_nodes.get(channel)
            if seq_info:
                mgr.request_join(sequencer_info=seq_info)

    # ------------------------------------------------------------------
    # Internal: delivery (unsequenced)
    # ------------------------------------------------------------------

    def _already_delivered(self, msg_id: str) -> bool:
        with self._delivered_lock:
            return msg_id in self._delivered_ids

    def _deliver(self, msg: Message) -> None:
        """Deliver to local callbacks (for unsequenced gossip)."""
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
    # Gap checker background thread
    # ------------------------------------------------------------------

    def start_gap_checker(self, interval: float = 2.0) -> None:
        """Start the background thread that checks for delivery gaps."""
        if self._gap_checker_thread is not None:
            return
        self._stop_event.clear()
        self._gap_checker_thread = threading.Thread(
            target=self._gap_checker_loop,
            args=(interval,),
            name="GapChecker",
            daemon=True,
        )
        self._gap_checker_thread.start()
        logger.info("[Dissemination] Gap checker started (interval=%.1fs)", interval)

    def _gap_checker_loop(self, interval: float) -> None:
        while not self._stop_event.wait(interval):
            for ch, buf in self._delivery_buffers.items():
                buf.check_gaps()
                # Check if core nodes need to leave due to gaps
                mgr = self._core_managers.get(ch)
                if mgr:
                    mgr.check_and_leave_if_gap()

    def stop_gap_checker(self) -> None:
        self._stop_event.set()
        if self._gap_checker_thread:
            self._gap_checker_thread.join(timeout=2.0)
            self._gap_checker_thread = None

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "published": self._published,
            "forwarded": self._forwarded,
            "delivered": self._delivered,
        }
        # Add per-channel delivery buffer stats
        for ch, buf in self._delivery_buffers.items():
            result[f"channel_{ch}_last_seq"] = buf.last_delivered_seq
            result[f"channel_{ch}_pending"] = buf.pending_count
        return result

    @property
    def delivery_buffers(self) -> Dict[str, "OrderedDeliveryBuffer"]:
        return self._delivery_buffers

    def __repr__(self) -> str:
        return (
            f"DisseminationEngine(published={self._published}, "
            f"forwarded={self._forwarded}, delivered={self._delivered})"
        )
