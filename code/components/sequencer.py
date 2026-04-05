"""
components/sequencer.py  —  Per-Channel Total-Order Sequencer
==============================================================
Assigns globally sequential numbers (1, 2, 3 …) to messages on a
specific channel.  This is the core mechanism that provides total order:
every subscriber sees messages in the same seq_num order.

Message flow
------------
  1. Publisher sends GOSSIP → reaches sequencer (directly or via gossip)
  2. Sequencer assigns next seq_num
  3. Sequencer creates SEQUENCE_ASSIGN with seq_num in metadata + original
     message in content, and broadcasts it to all neighbours
  4. Sequencer stores the sequenced message (in-memory + optional disk)

History serving
---------------
Core nodes (including the sequencer) store all sequenced messages and
respond to HISTORY_REQUEST with HISTORY_RESPONSE containing the
requested range.

Assumptions (from the spec):
  - Each channel has exactly one active sequencer (configured, not elected)
  - Core nodes for a channel never ALL disconnect simultaneously
  - Network partitions are temporary
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType

if TYPE_CHECKING:
    from components.storage import MessageStore

logger = logging.getLogger(__name__)

SendFn = Callable[[NodeInfo, Message], bool]


class ChannelSequencer:
    """
    Assigns global sequence numbers to messages on a single channel.

    Usage:
        seq = ChannelSequencer("sensors", node, send_fn, store=my_store)
        seq.load_state()           # recover seq_counter from disk
        seq.handle_gossip(msg)     # called when a GOSSIP arrives for this channel
        seq.handle_history_request(msg, requester)
    """

    def __init__(
        self,
        channel: str,
        node: LocalNode,
        send_fn: SendFn,
        store: Optional["MessageStore"] = None,
    ) -> None:
        self._channel = channel
        self._node = node
        self._send_fn = send_fn
        self._store = store

        self._seq_counter: int = 0
        self._seq_lock = threading.Lock()

        # In-memory ordered history (for serving requests when no store)
        self._history: List[Dict[str, Any]] = []
        self._history_lock = threading.Lock()

        logger.info(
            "[Sequencer] Initialised for channel '%s' on node %s",
            channel, node.node_id[:8],
        )

    @property
    def channel(self) -> str:
        return self._channel

    @property
    def current_seq(self) -> int:
        with self._seq_lock:
            return self._seq_counter

    # ------------------------------------------------------------------
    # Handle incoming GOSSIP → assign seq_num → broadcast
    # ------------------------------------------------------------------

    def handle_gossip(self, msg: Message) -> Message:
        """
        Receive a raw GOSSIP message for this channel, assign a sequence
        number, store it, and broadcast as SEQUENCE_ASSIGN.

        Returns the created SEQUENCE_ASSIGN message.
        """
        with self._seq_lock:
            self._seq_counter += 1
            seq_num = self._seq_counter

        # Create the sequenced message
        seq_msg = self._node.factory.sequence_assign(
            channel=self._channel,
            original_msg=msg.to_dict(),
            seq_num=seq_num,
        )

        # Store locally
        msg_dict = seq_msg.to_dict()
        with self._history_lock:
            self._history.append(msg_dict)

        if self._store:
            self._store.store_message(self._channel, seq_num, msg_dict)

        logger.info(
            "[Sequencer] Assigned seq=%d to msg %s on '%s'",
            seq_num, msg.msg_id[:8], self._channel,
        )

        # Broadcast SEQUENCE_ASSIGN to all neighbours
        for entry in self._node.alive_neighbours():
            try:
                self._send_fn(entry.info, seq_msg)
            except Exception as exc:
                logger.warning(
                    "[Sequencer] Failed to send seq=%d to %s: %s",
                    seq_num, entry.info.node_id[:8], exc,
                )

        return seq_msg

    # ------------------------------------------------------------------
    # Handle HISTORY_REQUEST
    # ------------------------------------------------------------------

    def handle_history_request(self, msg: Message, requester: NodeInfo) -> None:
        """
        Serve a HISTORY_REQUEST from a subscriber or recovering node.

        The request metadata contains:
          from_seq : int — first seq_num to return (inclusive)
          to_seq   : int — last seq_num to return (inclusive), or -1 for all
        """
        from_seq = msg.metadata.get("from_seq", 1)
        to_seq = msg.metadata.get("to_seq", -1)
        if to_seq < 0:
            to_seq = self.current_seq

        batch_size = 100  # default
        for ch_cfg in self._node.config.channels:
            if ch_cfg.channel_name == self._channel:
                batch_size = ch_cfg.history_batch_size
                break

        # Retrieve from storage or in-memory history
        if self._store:
            msg_dicts = self._store.get_messages(self._channel, from_seq, to_seq)
        else:
            with self._history_lock:
                msg_dicts = [
                    m for m in self._history
                    if from_seq <= m.get("metadata", {}).get("seq_num", 0) <= to_seq
                ]

        # Send in batches to respect message size limits
        for i in range(0, len(msg_dicts), batch_size):
            batch = msg_dicts[i : i + batch_size]
            response = self._node.factory.history_response(self._channel, batch)
            try:
                self._send_fn(requester, response)
            except Exception as exc:
                logger.warning(
                    "[Sequencer] Failed to send history batch to %s: %s",
                    requester.node_id[:8], exc,
                )

        logger.info(
            "[Sequencer] Served history [%d..%d] (%d msgs) to %s",
            from_seq, to_seq, len(msg_dicts), requester.node_id[:8],
        )

    # ------------------------------------------------------------------
    # State recovery
    # ------------------------------------------------------------------

    def load_state(self) -> None:
        """On startup, load the last seq_num from storage so we don't reset."""
        if self._store:
            last = self._store.get_last_seq(self._channel)
            with self._seq_lock:
                self._seq_counter = last
            logger.info(
                "[Sequencer] Restored seq_counter=%d for '%s' from storage",
                last, self._channel,
            )
