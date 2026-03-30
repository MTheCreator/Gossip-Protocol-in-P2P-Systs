"""
components/status_reporter.py  —  Node-side Status Reporting
==============================================================
Daemon thread that periodically sends STATUS_REPORT messages to
the monitoring server.  Each report contains a snapshot of the
node's current state: neighbours, edge probabilities, channel
subscriptions, delivery buffer stats, and dissemination counters.

The monitor address is hardcoded in the config (MonitorConfig).
The reporter does NOT participate in gossip — it sends a single
unicast message to the monitor on each interval.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Callable, Dict, Optional

from core.node import LocalNode, NodeInfo
from core.message import Message

if TYPE_CHECKING:
    from components.dissemination import DisseminationEngine
    from components.core_manager import CoreNodeManager
    from components.election import ElectionManager

logger = logging.getLogger(__name__)

SendFn = Callable[[NodeInfo, Message], bool]


class StatusReporter(threading.Thread):
    """
    Periodically sends STATUS_REPORT to the monitoring server.

    Usage:
        reporter = StatusReporter(node, send_fn, "127.0.0.1:9090", interval=5.0)
        reporter.set_engine(engine)
        reporter.start()
        ...
        reporter.stop()
    """

    def __init__(
        self,
        node: LocalNode,
        send_fn: SendFn,
        monitor_address: str,
        interval: float = 5.0,
    ) -> None:
        super().__init__(daemon=True, name=f"StatusReporter-{node.node_id[:8]}")
        self._node = node
        self._send_fn = send_fn
        self._interval = interval
        self._stop_event = threading.Event()
        self._engine: Optional["DisseminationEngine"] = None
        self._core_managers: Dict[str, "CoreNodeManager"] = {}
        self._election_managers: Dict[str, "ElectionManager"] = {}

        # Parse monitor address
        try:
            host, port_str = monitor_address.rsplit(":", 1)
            self._monitor_info = NodeInfo.from_address(host, int(port_str))
        except (ValueError, AttributeError):
            logger.error("[StatusReporter] Invalid monitor address: %s", monitor_address)
            self._monitor_info = None

    def set_engine(self, engine: "DisseminationEngine") -> None:
        """Wire in the DisseminationEngine for stats reporting."""
        self._engine = engine

    def set_core_managers(self, managers: Dict[str, "CoreNodeManager"]) -> None:
        self._core_managers = managers

    def set_election_managers(self, managers: Dict[str, "ElectionManager"]) -> None:
        self._election_managers = managers

    def run(self) -> None:
        logger.info(
            "[StatusReporter] Started (interval=%.1fs, monitor=%s)",
            self._interval,
            self._monitor_info.address if self._monitor_info else "NONE",
        )
        while not self._stop_event.wait(self._interval):
            self._report()

    def _report(self) -> None:
        """Build and send a status report to the monitor."""
        if self._monitor_info is None:
            return

        status_data = {
            "node_id": self._node.node_id,
            "address": self._node.address,
            "is_seed": self._node.is_seed_node,
            "neighbour_count": self._node.neighbour_count,
            "neighbours": [
                {
                    "node_id": e.info.node_id,
                    "address": e.info.address,
                    "edge_prob": round(e.edge_prob, 4),
                    "status": e.status.value,
                    "reliability_score": round(e.reliability_score, 4),
                }
                for e in self._node.all_neighbours()
            ],
            "channels": list(self._node._subscriptions),
            "stats": self._engine.stats if self._engine else {},
            "status": "alive",
        }

        # Add delivery buffer details
        if self._engine:
            buffers = {}
            for ch, buf in self._engine.delivery_buffers.items():
                buffers[ch] = {
                    "last_delivered_seq": buf.last_delivered_seq,
                    "pending_count": buf.pending_count,
                    "next_expected": buf.next_expected,
                }
            status_data["delivery_buffers"] = buffers

        # Core node status per channel
        core_status = {}
        for ch, mgr in self._core_managers.items():
            core_status[ch] = {
                "is_core": mgr.is_core,
                "core_addresses": mgr.core_addresses,
            }
        if core_status:
            status_data["core_status"] = core_status

        # Sequencer / election status per channel
        sequencer_status = {}
        for ch, em in self._election_managers.items():
            sequencer_status[ch] = {
                "is_sequencer": em.is_sequencer,
                "sequencer_address": em.sequencer_address or "",
                "last_known_seq": em.last_known_seq,
                "election_in_progress": em.election_in_progress,
            }
        if sequencer_status:
            status_data["sequencer_status"] = sequencer_status

        # Delivered messages per channel (last 50 with content)
        if self._engine:
            delivered_msgs = {}
            for ch in self._node._subscriptions:
                msgs = self._engine.get_delivered_messages(ch, limit=50)
                if msgs:
                    delivered_msgs[ch] = msgs
            if delivered_msgs:
                status_data["delivered_messages"] = delivered_msgs

        report_msg = self._node.factory.status_report(status_data)
        try:
            self._send_fn(self._monitor_info, report_msg)
        except Exception as exc:
            logger.debug("[StatusReporter] Failed to send report: %s", exc)

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("[StatusReporter] Stopped")
