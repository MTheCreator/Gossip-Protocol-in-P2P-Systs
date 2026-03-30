"""
components/election.py  --  Dynamic Sequencer Election
========================================================
Manages sequencer state and runs elections for a single channel.

The sequencer sends a fast heartbeat (SEQUENCER_HEARTBEAT) to all core
nodes every `sequencer_heartbeat_interval` seconds.  Core nodes monitor
this heartbeat; if `sequencer_heartbeat_timeout` elapses without a beat,
they suspect the sequencer has failed and initiate an election.

Election algorithm (assuming synchrony among core nodes):
  1. Suspecting core node sends ELECTION_REQUEST to all core nodes
  2. Each core node records the vote and cascades if not already voting
  3. When quorum (n/2+1) is reached, deterministic election:
     - Sort surviving core addresses
     - New sequencer = sorted[0]
     - next_seq = max(all_last_known_seq) + 1
  4. All core nodes broadcast NEW_SEQUENCER to the entire network
  5. All nodes update their sequencer reference

For non-core nodes: if publish() fails → broadcast FIND_SEQUENCER_REQUEST;
any node that knows the sequencer responds with FIND_SEQUENCER_RESPONSE.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType

if TYPE_CHECKING:
    from components.core_manager import CoreNodeManager
    from components.sequencer import ChannelSequencer
    from components.storage import MessageStore

logger = logging.getLogger(__name__)

SendFn = Callable[[NodeInfo, Message], bool]


class ElectionManager:
    """
    Manages sequencer election for a single channel.

    Usage:
        em = ElectionManager("demo", node, send_fn, core_mgr, store,
                             on_new_sequencer=callback)
        em.load_state()
        # ... wire into server message handlers ...
    """

    def __init__(
        self,
        channel: str,
        node: LocalNode,
        send_fn: SendFn,
        core_manager: "CoreNodeManager",
        store: Optional["MessageStore"] = None,
        on_new_sequencer: Optional[Callable[[str, str, int], None]] = None,
        sequencer_heartbeat_interval: float = 0.3,
        sequencer_heartbeat_timeout: float = 1.5,
        election_timeout: float = 3.0,
    ) -> None:
        self._channel = channel
        self._node = node
        self._send_fn = send_fn
        self._core_manager = core_manager
        self._store = store
        # Callback: on_new_sequencer(channel, address, next_seq)
        self._on_new_sequencer = on_new_sequencer

        # Timing
        self._seq_hb_interval = sequencer_heartbeat_interval
        self._seq_hb_timeout = sequencer_heartbeat_timeout
        self._election_timeout = election_timeout

        # Current sequencer state
        self._sequencer_address: Optional[str] = None
        self._last_known_seq: int = 0

        # Sequencer heartbeat tracking (for core nodes monitoring the sequencer)
        self._last_seq_heartbeat: float = 0.0

        # Election state
        self._election_in_progress: bool = False
        self._election_votes: Set[str] = set()  # node_ids that voted
        self._election_timer: Optional[threading.Timer] = None

        # Sequencer heartbeat sender thread (runs only on the sequencer)
        self._seq_hb_thread: Optional[threading.Thread] = None
        self._seq_hb_stop = threading.Event()

        # Sequencer alive checker thread (runs only on core nodes)
        self._seq_checker_thread: Optional[threading.Thread] = None
        self._seq_checker_stop = threading.Event()

        self._lock = threading.Lock()

        logger.info(
            "[Election/%s] Initialised on node %s",
            channel, node.node_id[:8],
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def sequencer_address(self) -> Optional[str]:
        with self._lock:
            return self._sequencer_address

    @property
    def is_sequencer(self) -> bool:
        with self._lock:
            return (
                self._sequencer_address is not None
                and self._sequencer_address == self._node.address
            )

    @property
    def last_known_seq(self) -> int:
        with self._lock:
            return self._last_known_seq

    @property
    def election_in_progress(self) -> bool:
        with self._lock:
            return self._election_in_progress

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def set_sequencer(self, address: str, next_seq: int = 0) -> None:
        """Set the current sequencer address and persist."""
        with self._lock:
            self._sequencer_address = address
            if next_seq > 0:
                self._last_known_seq = next_seq - 1
        self.save_state()
        logger.info(
            "[Election/%s] Sequencer set to %s (last_seq=%d)",
            self._channel, address, self._last_known_seq,
        )

    def load_state(self) -> None:
        """Load sequencer address and last_known_seq from store."""
        if not self._store:
            return
        data = self._store.load_state(f"sequencer_{self._channel}")
        if data and isinstance(data, dict):
            with self._lock:
                self._sequencer_address = data.get("address")
                self._last_known_seq = data.get("last_known_seq", 0)
            logger.info(
                "[Election/%s] Loaded sequencer=%s, last_seq=%d from storage",
                self._channel, self._sequencer_address,
                self._last_known_seq,
            )

    def save_state(self) -> None:
        """Persist sequencer state."""
        if not self._store:
            return
        with self._lock:
            self._store.save_state(f"sequencer_{self._channel}", {
                "address": self._sequencer_address,
                "last_known_seq": self._last_known_seq,
            })

    # ------------------------------------------------------------------
    # Sequencer heartbeat SENDER (runs on the SEQUENCER node)
    # ------------------------------------------------------------------

    def start_sequencer_heartbeat(
        self, sequencer_ref: "ChannelSequencer"
    ) -> None:
        """Start sending fast heartbeats to all core nodes."""
        self._seq_hb_stop.clear()
        self._seq_hb_thread = threading.Thread(
            target=self._seq_hb_loop,
            args=(sequencer_ref,),
            name=f"SeqHB-{self._channel}",
            daemon=True,
        )
        self._seq_hb_thread.start()
        logger.info(
            "[Election/%s] Sequencer heartbeat started (interval=%.2fs)",
            self._channel, self._seq_hb_interval,
        )

    def _seq_hb_loop(self, sequencer_ref: "ChannelSequencer") -> None:
        while not self._seq_hb_stop.wait(self._seq_hb_interval):
            last_seq = sequencer_ref.current_seq
            msg = self._node.factory.sequencer_heartbeat(
                self._channel, last_seq
            )
            for info in self._core_manager.core_node_infos:
                if info.node_id == self._node.node_id:
                    continue  # don't send to self
                try:
                    self._send_fn(info, msg)
                except Exception:
                    pass

    def stop_sequencer_heartbeat(self) -> None:
        self._seq_hb_stop.set()
        if self._seq_hb_thread:
            self._seq_hb_thread.join(timeout=2.0)
            self._seq_hb_thread = None

    # ------------------------------------------------------------------
    # Sequencer heartbeat RECEPTION (runs on CORE nodes)
    # ------------------------------------------------------------------

    def handle_sequencer_heartbeat(self, msg: Message) -> None:
        """Process a SEQUENCER_HEARTBEAT from the current sequencer."""
        last_seq = msg.metadata.get("last_seq", 0)
        with self._lock:
            self._last_seq_heartbeat = time.time()
            self._last_known_seq = max(self._last_known_seq, last_seq)

    # ------------------------------------------------------------------
    # Sequencer alive checker (runs on CORE nodes)
    # ------------------------------------------------------------------

    def start_sequencer_checker(self) -> None:
        """Start monitoring the sequencer's heartbeat (core nodes only)."""
        self._seq_checker_stop.clear()
        # Initialize heartbeat timestamp so we don't immediately trigger
        with self._lock:
            self._last_seq_heartbeat = time.time()
        self._seq_checker_thread = threading.Thread(
            target=self._seq_checker_loop,
            name=f"SeqCheck-{self._channel}",
            daemon=True,
        )
        self._seq_checker_thread.start()
        logger.info(
            "[Election/%s] Sequencer checker started (timeout=%.2fs)",
            self._channel, self._seq_hb_timeout,
        )

    def _seq_checker_loop(self) -> None:
        interval = self._seq_hb_interval / 2
        while not self._seq_checker_stop.wait(interval):
            self.check_sequencer_alive()

    def stop_sequencer_checker(self) -> None:
        self._seq_checker_stop.set()
        if self._seq_checker_thread:
            self._seq_checker_thread.join(timeout=2.0)
            self._seq_checker_thread = None

    def check_sequencer_alive(self) -> None:
        """
        Called periodically on core nodes.
        If sequencer heartbeat timed out, initiate election.
        """
        if self.is_sequencer:
            return  # we ARE the sequencer

        if not self._core_manager.is_core:
            return  # only core nodes participate in election

        with self._lock:
            if self._election_in_progress:
                return
            if self._last_seq_heartbeat == 0:
                return  # not yet initialized
            elapsed = time.time() - self._last_seq_heartbeat
            if elapsed < self._seq_hb_timeout:
                return

        logger.warning(
            "[Election/%s] Sequencer heartbeat timeout (%.1fs > %.1fs) — "
            "initiating election",
            self._channel, elapsed, self._seq_hb_timeout,
        )
        self.initiate_election()

    # ------------------------------------------------------------------
    # Election protocol
    # ------------------------------------------------------------------

    def initiate_election(self) -> None:
        """Initiate an election for a new sequencer."""
        with self._lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
            self._election_votes = {self._node.node_id}
            last_seq = self._last_known_seq

        logger.info(
            "[Election/%s] Election initiated (last_known_seq=%d)",
            self._channel, last_seq,
        )

        # Send ELECTION_REQUEST to all core nodes
        msg = self._node.factory.election_request(self._channel, last_seq)
        for info in self._core_manager.core_node_infos:
            if info.node_id == self._node.node_id:
                continue
            try:
                self._send_fn(info, msg)
            except Exception as exc:
                logger.debug(
                    "[Election/%s] Failed to send ELECTION_REQUEST to %s: %s",
                    self._channel, info.node_id[:8], exc,
                )

        # Start election timeout
        with self._lock:
            if self._election_timer:
                self._election_timer.cancel()
            self._election_timer = threading.Timer(
                self._election_timeout, self._election_timeout_handler
            )
            self._election_timer.daemon = True
            self._election_timer.start()

        # Check if we already have quorum (e.g., only 1 core node)
        self._check_quorum()

    def handle_election_request(
        self, msg: Message, sender_info: NodeInfo
    ) -> None:
        """Handle ELECTION_REQUEST from another core node."""
        if not self._core_manager.is_core:
            return  # only core nodes vote

        remote_last_seq = msg.metadata.get("last_known_seq", 0)

        with self._lock:
            self._last_known_seq = max(self._last_known_seq, remote_last_seq)
            self._election_votes.add(sender_info.node_id)
            was_in_progress = self._election_in_progress

        logger.info(
            "[Election/%s] Received ELECTION_REQUEST from %s (votes=%d)",
            self._channel, sender_info.node_id[:8],
            len(self._election_votes),
        )

        # Cascade: if we haven't started our own election yet, start one
        if not was_in_progress:
            self.initiate_election()

        self._check_quorum()

    def _check_quorum(self) -> None:
        """Check if we have enough votes to proceed with election."""
        core_addrs = self._core_manager.core_addresses
        n = len(core_addrs)
        quorum = n // 2 + 1

        with self._lock:
            votes = len(self._election_votes)
            if not self._election_in_progress:
                return
            if votes < quorum:
                return

        logger.info(
            "[Election/%s] Quorum reached (%d/%d votes, need %d)",
            self._channel, votes, n, quorum,
        )
        self.proceed_with_election()

    def proceed_with_election(self) -> None:
        """Quorum reached — deterministically elect the new sequencer."""
        with self._lock:
            if not self._election_in_progress:
                return
            last_seq = self._last_known_seq

            # Cancel timeout timer
            if self._election_timer:
                self._election_timer.cancel()
                self._election_timer = None

        # Deterministic: sort core addresses, remove dead sequencer, pick first
        core_addrs = self._core_manager.core_addresses
        old_seq_addr = self.sequencer_address

        candidates = sorted(
            addr for addr in core_addrs if addr != old_seq_addr
        )
        if not candidates:
            # All cores are dead? Fall back to ourselves
            candidates = [self._node.address]

        new_seq_addr = candidates[0]
        next_seq_num = last_seq + 1

        logger.info(
            "[Election/%s] Elected new sequencer: %s (next_seq=%d)",
            self._channel, new_seq_addr, next_seq_num,
        )

        # Update our own state
        self.set_sequencer(new_seq_addr, next_seq_num)

        with self._lock:
            self._election_in_progress = False
            self._election_votes.clear()

        # Broadcast NEW_SEQUENCER to all neighbours
        announce = self._node.factory.new_sequencer(
            self._channel, new_seq_addr, next_seq_num,
            self._core_manager.core_addresses,
        )
        for entry in self._node.alive_neighbours():
            try:
                self._send_fn(entry.info, announce)
            except Exception:
                pass

        # Fire callback
        if self._on_new_sequencer:
            self._on_new_sequencer(self._channel, new_seq_addr, next_seq_num)

    def handle_new_sequencer(self, msg: Message) -> None:
        """
        Called on ALL nodes when NEW_SEQUENCER is received.
        Updates sequencer reference.
        """
        content = msg.content if isinstance(msg.content, dict) else {}
        seq_addr = content.get("sequencer_address", "")
        next_seq = content.get("next_seq_num", 0)
        core_list = content.get("core_list", [])

        if not seq_addr:
            return

        logger.info(
            "[Election/%s] Received NEW_SEQUENCER: %s (next_seq=%d)",
            self._channel, seq_addr, next_seq,
        )

        self.set_sequencer(seq_addr, next_seq)

        # Update core list
        self._core_manager.set_initial_core(core_list)

        # Reset election state
        with self._lock:
            self._election_in_progress = False
            self._election_votes.clear()
            if self._election_timer:
                self._election_timer.cancel()
                self._election_timer = None

        # Fire callback so the server can create/update the sequencer
        if self._on_new_sequencer:
            self._on_new_sequencer(self._channel, seq_addr, next_seq)

    def _election_timeout_handler(self) -> None:
        """Election timed out without quorum.  Reset and let next check retry."""
        with self._lock:
            self._election_in_progress = False
            self._election_votes.clear()
            self._election_timer = None
        logger.warning(
            "[Election/%s] Election timed out — will retry on next check",
            self._channel,
        )

    # ------------------------------------------------------------------
    # FIND_SEQUENCER (for non-core nodes and bootstrap)
    # ------------------------------------------------------------------

    def handle_find_sequencer_request(
        self, msg: Message, sender_info: NodeInfo
    ) -> None:
        """Respond if we know the current sequencer."""
        seq_addr = self.sequencer_address
        if not seq_addr:
            return
        resp = self._node.factory.find_sequencer_response(
            self._channel, seq_addr, self._core_manager.core_addresses
        )
        try:
            self._send_fn(sender_info, resp)
            logger.info(
                "[Election/%s] Responded to FIND_SEQUENCER from %s",
                self._channel, sender_info.node_id[:8],
            )
        except Exception:
            pass

    def handle_find_sequencer_response(self, msg: Message) -> None:
        """Process a response to our FIND_SEQUENCER_REQUEST."""
        content = msg.content if isinstance(msg.content, dict) else {}
        seq_addr = content.get("sequencer_address", "")
        core_list = content.get("core_list", [])
        if not seq_addr:
            return

        logger.info(
            "[Election/%s] Discovered sequencer: %s (cores=%d)",
            self._channel, seq_addr, len(core_list),
        )
        self.set_sequencer(seq_addr)
        if core_list:
            self._core_manager.set_initial_core(core_list)

        # Fire callback to wire the sequencer
        if self._on_new_sequencer:
            self._on_new_sequencer(
                self._channel, seq_addr, self._last_known_seq + 1
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Stop all background threads."""
        self.stop_sequencer_heartbeat()
        self.stop_sequencer_checker()
        with self._lock:
            if self._election_timer:
                self._election_timer.cancel()
                self._election_timer = None
