"""
components/status_reporter.py  —  Node-side Status Reporting
==============================================================
Daemon thread that periodically sends STATUS_REPORT messages to
the monitoring server over TCP.  Each report contains a snapshot of
the node's current state: neighbours, edge probabilities, channel
subscriptions, delivery buffer stats, and dissemination counters.

Reports are sent as length-prefixed JSON over a TCP connection,
bypassing the UDP transport entirely so large payloads are not
constrained by datagram size limits.
"""

from __future__ import annotations

import json
import logging
import socket
import struct
import threading
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from core.node import LocalNode, NodeInfo
from core.message import Message

if TYPE_CHECKING:
    from components.dissemination import DisseminationEngine
    from components.core_manager import CoreNodeManager
    from components.election import ElectionManager

logger = logging.getLogger(__name__)

SendFn = Callable[[NodeInfo, Message], bool]
TrafficFn = Callable[[], List[Dict[str, Any]]]


class StatusReporter(threading.Thread):
    """
    Periodically sends STATUS_REPORT to the monitoring server over TCP.

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
        get_traffic_events: Optional[TrafficFn] = None,
    ) -> None:
        super().__init__(daemon=True, name=f"StatusReporter-{node.node_id[:8]}")
        self._node = node
        self._send_fn = send_fn
        self._interval = interval
        self._stop_event = threading.Event()
        self._engine: Optional["DisseminationEngine"] = None
        self._core_managers: Dict[str, "CoreNodeManager"] = {}
        self._election_managers: Dict[str, "ElectionManager"] = {}
        self._get_traffic_events = get_traffic_events

        # Parse monitor address — TCP reports go to monitor_port + 2
        self._monitor_tcp_host: Optional[str] = None
        self._monitor_tcp_port: Optional[int] = None
        try:
            host, port_str = monitor_address.rsplit(":", 1)
            self._monitor_tcp_host = host
            self._monitor_tcp_port = int(port_str) + 2  # TCP on port+2
        except (ValueError, AttributeError):
            logger.error("[StatusReporter] Invalid monitor address: %s", monitor_address)

        # Also keep NodeInfo for backward compat (unused now but kept for STATUS_REQUEST)
        try:
            host, port_str = monitor_address.rsplit(":", 1)
            self._monitor_info = NodeInfo.from_address(host, int(port_str))
        except (ValueError, AttributeError):
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
            try:
                self._report()
            except Exception as exc:
                logger.warning("[StatusReporter] Report failed: %s", exc)

    def _send_tcp(self, data: dict) -> None:
        """Send a JSON payload to the monitor over TCP with a 4-byte length prefix."""
        if self._monitor_tcp_host is None or self._monitor_tcp_port is None:
            return
        try:
            payload = json.dumps(data).encode("utf-8")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((self._monitor_tcp_host, self._monitor_tcp_port))
            sock.sendall(struct.pack("!I", len(payload)) + payload)
            sock.close()
        except Exception as exc:
            logger.debug("[StatusReporter] TCP send failed: %s", exc)

    def _report(self) -> None:
        """Build and send a status report to the monitor over TCP."""
        if self._monitor_tcp_host is None:
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
            "channels": list(self._node._subscriptions.copy()),
            "stats": dict(self._engine.stats) if self._engine else {},
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

        # Lightweight per-send traffic events (for edge flow animations).
        if self._get_traffic_events:
            events = self._get_traffic_events()
            if events:
                status_data["traffic_events"] = events

        # Send over TCP (length-prefixed JSON)
        self._send_tcp(status_data)

    def stop(self) -> None:
        self._stop_event.set()
        logger.info("[StatusReporter] Stopped")
