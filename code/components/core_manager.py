"""
components/core_manager.py  --  Dynamic Core Node Management
=============================================================
Manages the core node group for a single channel.  Core nodes are
subscribers that have fully caught up (no gaps) and been approved
by the sequencer.

Core join protocol:
  1. Node has no gaps (pending_count == 0, last_delivered_seq >= threshold)
  2. Node sends CORE_JOIN_REQUEST to sequencer
  3. Sequencer approves and broadcasts CORE_JOIN_APPROVED to all nodes
  4. All nodes update their core node list

Core leave protocol:
  1. Core node detects a gap (pending_count > 0)
  2. Core node broadcasts CORE_LEAVE to all nodes
  3. All nodes remove it from the core list

All core nodes know each other.  All nodes know all core nodes.
Core node list is persisted via MessageStore.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType

if TYPE_CHECKING:
    from components.ordered_delivery import OrderedDeliveryBuffer
    from components.storage import MessageStore

logger = logging.getLogger(__name__)

SendFn = Callable[[NodeInfo, Message], bool]


class CoreNodeManager:
    """
    Manages the dynamic core node group for a single channel.

    Usage:
        mgr = CoreNodeManager("demo", node, send_fn, delivery_buffer, store)
        mgr.load_state()
        # ... wire into server message handlers ...
    """

    def __init__(
        self,
        channel: str,
        node: LocalNode,
        send_fn: SendFn,
        delivery_buffer: Optional["OrderedDeliveryBuffer"] = None,
        store: Optional["MessageStore"] = None,
        core_join_min_seq: int = 5,
    ) -> None:
        self._channel = channel
        self._node = node
        self._send_fn = send_fn
        self._delivery_buffer = delivery_buffer
        self._store = store
        self._core_join_min_seq = core_join_min_seq

        self._core_addresses: List[str] = []  # sorted list of "host:port"
        self._is_core: bool = False
        self._join_requested: bool = False  # prevent duplicate requests
        self._get_view_number: Callable[[], int] = lambda: 0
        self._lock = threading.Lock()

        logger.info(
            "[CoreMgr/%s] Initialised on node %s",
            channel,
            node.node_id[:8],
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_core(self) -> bool:
        with self._lock:
            return self._is_core

    @property
    def core_addresses(self) -> List[str]:
        with self._lock:
            return list(self._core_addresses)

    @property
    def core_node_infos(self) -> List[NodeInfo]:
        """Convert core_addresses to NodeInfo objects."""
        with self._lock:
            result = []
            for addr in self._core_addresses:
                try:
                    host, port_str = addr.rsplit(":", 1)
                    result.append(NodeInfo.from_address(host, int(port_str)))
                except (ValueError, AttributeError):
                    pass
            return result

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_state(self) -> None:
        """Load persisted core node list from store."""
        if not self._store:
            return
        data = self._store.load_state(f"core_nodes_{self._channel}")
        if data and isinstance(data, list):
            with self._lock:
                self._core_addresses = sorted(data)
                self._is_core = self._node.address in self._core_addresses
            logger.info(
                "[CoreMgr/%s] Loaded %d core nodes from storage (is_core=%s)",
                self._channel,
                len(self._core_addresses),
                self._is_core,
            )

    def save_state(self) -> None:
        """Persist core node list to store."""
        if not self._store:
            return
        with self._lock:
            self._store.save_state(
                f"core_nodes_{self._channel}", list(self._core_addresses)
            )

    # ------------------------------------------------------------------
    # Core join: request (called by any node wanting to join)
    # ------------------------------------------------------------------

    def request_join(self, sequencer_info: Optional[NodeInfo] = None) -> bool:
        """
        If this node is eligible (no gaps, enough delivered), send
        CORE_JOIN_REQUEST to the sequencer.  Returns True if request was sent.
        """
        with self._lock:
            if self._is_core:
                return False  # already core
            if self._join_requested:
                return False  # already pending

        if not self._delivery_buffer:
            return False

        # Check eligibility
        if self._delivery_buffer.pending_count > 0:
            return False
        if self._delivery_buffer.last_delivered_seq < self._core_join_min_seq:
            return False

        if sequencer_info is None:
            return False

        with self._lock:
            self._join_requested = True

        msg = self._node.factory.core_join_request(self._channel)
        try:
            self._send_fn(sequencer_info, msg)
            logger.info(
                "[CoreMgr/%s] Sent CORE_JOIN_REQUEST to sequencer %s",
                self._channel,
                sequencer_info.node_id[:8],
            )
            return True
        except Exception as exc:
            logger.warning(
                "[CoreMgr/%s] Failed to send CORE_JOIN_REQUEST: %s",
                self._channel,
                exc,
            )
            with self._lock:
                self._join_requested = False
            return False

    # ------------------------------------------------------------------
    # Core join: handle request (called on SEQUENCER only)
    # ------------------------------------------------------------------

    def handle_join_request(self, msg: Message, sender_info: NodeInfo) -> None:
        """
        Called on the SEQUENCER when a CORE_JOIN_REQUEST arrives.
        Approves the request and broadcasts CORE_JOIN_APPROVED.
        """
        new_core_addr = sender_info.address

        with self._lock:
            if new_core_addr in self._core_addresses:
                logger.debug(
                    "[CoreMgr/%s] %s already in core group",
                    self._channel,
                    new_core_addr,
                )
                return

            self._core_addresses.append(new_core_addr)
            self._core_addresses.sort()
            core_list = list(self._core_addresses)

        logger.info(
            "[CoreMgr/%s] Approved core join for %s (total=%d)",
            self._channel,
            new_core_addr,
            len(core_list),
        )

        # Broadcast CORE_JOIN_APPROVED to all neighbours
        approved_msg = self._node.factory.core_join_approved(
            self._channel,
            new_core_addr,
            core_list,
            view_number=self._current_view_number(),
        )
        self._broadcast(approved_msg)
        self.save_state()

    # ------------------------------------------------------------------
    # Core join: handle approved (called on ALL nodes)
    # ------------------------------------------------------------------

    def handle_join_approved(self, msg: Message) -> None:
        """
        Called on ALL nodes when CORE_JOIN_APPROVED is received via gossip.
        Updates the core node list.
        """
        content = msg.content if isinstance(msg.content, dict) else {}
        new_core_addr = content.get("new_core_address", "")
        core_list = content.get("core_list", [])

        with self._lock:
            self._core_addresses = sorted(core_list)
            if new_core_addr == self._node.address:
                self._is_core = True
                self._join_requested = False
                logger.info("[CoreMgr/%s] WE are now a core node!", self._channel)
            elif self._node.address in self._core_addresses:
                self._is_core = True

        self.save_state()
        logger.info(
            "[CoreMgr/%s] Updated core list: %d nodes (new: %s)",
            self._channel,
            len(core_list),
            new_core_addr[:20],
        )

    # ------------------------------------------------------------------
    # Core leave: handle (called on ALL nodes)
    # ------------------------------------------------------------------

    def handle_core_leave(self, msg: Message) -> None:
        """
        Called on ALL nodes when CORE_LEAVE is received via gossip.
        """
        content = msg.content if isinstance(msg.content, dict) else {}
        leaving_addr = content.get("leaving_address", "")
        core_list = content.get("core_list", [])

        with self._lock:
            self._core_addresses = sorted(core_list)
            if leaving_addr == self._node.address:
                self._is_core = False
                logger.info("[CoreMgr/%s] WE left the core group", self._channel)
            elif self._node.address not in self._core_addresses:
                self._is_core = False

        self.save_state()
        logger.info(
            "[CoreMgr/%s] Core node %s left (remaining=%d)",
            self._channel,
            leaving_addr[:20],
            len(core_list),
        )

    # ------------------------------------------------------------------
    # Core leave: check and auto-leave if gap detected
    # ------------------------------------------------------------------

    def check_and_leave_if_gap(self) -> bool:
        """
        Called periodically.  If this node is a core node AND has a gap
        in the delivery buffer, leave the core group.
        Returns True if we left.
        """
        with self._lock:
            if not self._is_core:
                return False

        if not self._delivery_buffer:
            return False

        if self._delivery_buffer.pending_count == 0:
            return False

        # We have a gap — leave the core group
        with self._lock:
            self._is_core = False
            if self._node.address in self._core_addresses:
                self._core_addresses.remove(self._node.address)
            self._core_addresses.sort()
            core_list = list(self._core_addresses)
            self._join_requested = False  # allow re-join later

        logger.warning(
            "[CoreMgr/%s] Leaving core group (gap detected, pending=%d)",
            self._channel,
            self._delivery_buffer.pending_count,
        )

        leave_msg = self._node.factory.core_leave(
            self._channel,
            self._node.address,
            core_list,
            view_number=self._current_view_number(),
        )
        self._broadcast(leave_msg)
        self.save_state()
        return True

    # ------------------------------------------------------------------
    # Set initial core (bootstrap: first node in network)
    # ------------------------------------------------------------------

    def set_initial_core(self, addresses: List[str]) -> None:
        """
        Called during bootstrap when this node is the first/sole core.
        """
        with self._lock:
            self._core_addresses = sorted(addresses)
            self._is_core = self._node.address in self._core_addresses
        self.save_state()
        logger.info(
            "[CoreMgr/%s] Set initial core: %s (is_core=%s)",
            self._channel,
            self._core_addresses,
            self._is_core,
        )

    # ------------------------------------------------------------------
    # Set delivery buffer (may not be available at construction time)
    # ------------------------------------------------------------------

    def set_delivery_buffer(self, buf: "OrderedDeliveryBuffer") -> None:
        self._delivery_buffer = buf

    def set_view_number_provider(self, provider: Callable[[], int]) -> None:
        self._get_view_number = provider

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _current_view_number(self) -> int:
        try:
            return int(self._get_view_number())
        except Exception:
            return 0

    def _broadcast(self, msg: Message) -> None:
        """Send a message to all alive neighbours."""
        for entry in self._node.alive_neighbours():
            try:
                self._send_fn(entry.info, msg)
            except Exception as exc:
                logger.debug(
                    "[CoreMgr/%s] Failed to send to %s: %s",
                    self._channel,
                    entry.info.node_id[:8],
                    exc,
                )
