"""
monitoring/monitor_server.py  —  Network-wide Monitoring Aggregator
=====================================================================
A standalone monitoring node that collects STATUS_REPORT messages from
all gossip nodes.  It does NOT participate in gossip forwarding — it
simply receives reports, aggregates topology/metrics, and exposes them
via a REST API (FastAPI) for the web dashboard.

Usage:
    from monitoring.monitor_server import MonitorServer

    monitor = MonitorServer(host="0.0.0.0", port=9090, config=my_config)
    monitor.start()   # starts transport listener + REST API
    ...
    monitor.stop()
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.node import NodeInfo
from core.message import Message, MessageType
from config.base_config import BaseConfig

logger = logging.getLogger(__name__)


class MonitorServer:
    """
    Aggregates network-wide state from STATUS_REPORT messages.

    Data model:
      _nodes:  {node_id: {node_id, address, timestamp, data: {...}}}
      The "data" dict contains whatever the StatusReporter sends:
        neighbours, channels, stats, is_seed, delivery_buffers, etc.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9090,
        config: Optional[BaseConfig] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._config = config or BaseConfig()

        # Aggregated state
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

        # Transport (initialized in start())
        self._transport = None

        # REST API thread
        self._api_thread: Optional[threading.Thread] = None
        self._api_server = None

    # ------------------------------------------------------------------
    # Handle incoming STATUS_REPORT
    # ------------------------------------------------------------------

    def handle_message(self, msg: Message, sender_info: Optional[NodeInfo]) -> None:
        """Process any incoming message.  We only care about STATUS_REPORT."""
        if msg.msg_type == MessageType.STATUS_REPORT:
            self._handle_status_report(msg, sender_info)
        elif msg.msg_type == MessageType.STATUS_REQUEST:
            pass  # We don't respond to requests — we send them
        else:
            logger.debug("[Monitor] Ignoring %s from %s", msg.msg_type.name,
                         sender_info.node_id[:8] if sender_info else "?")

    def _handle_status_report(
        self, msg: Message, sender_info: Optional[NodeInfo]
    ) -> None:
        """Store the latest status report from a gossip node."""
        data = msg.content if isinstance(msg.content, dict) else {}
        node_id = msg.sender_id
        address = data.get("address", "")
        if sender_info:
            address = address or sender_info.address

        with self._lock:
            self._nodes[node_id] = {
                "node_id": node_id,
                "address": address,
                "timestamp": msg.timestamp,
                "last_seen": time.time(),
                "data": data,
            }

        logger.debug("[Monitor] Received status from %s (%s)", node_id[:8], address)

    # ------------------------------------------------------------------
    # Data access (used by REST API)
    # ------------------------------------------------------------------

    def get_topology(self) -> Dict[str, Any]:
        """Return current network topology as JSON-serializable dict."""
        with self._lock:
            nodes = []
            edges = []
            seen_edges = set()

            for nid, info in self._nodes.items():
                data = info.get("data", {})
                nodes.append({
                    "id": nid,
                    "label": nid[:8],
                    "address": info.get("address", ""),
                    "status": data.get("status", "unknown"),
                    "is_seed": data.get("is_seed", False),
                    "neighbour_count": data.get("neighbour_count", 0),
                    "channels": data.get("channels", []),
                    "stats": data.get("stats", {}),
                    "core_status": data.get("core_status", {}),
                    "sequencer_status": data.get("sequencer_status", {}),
                    "delivery_buffers": data.get("delivery_buffers", {}),
                })

                for nb in data.get("neighbours", []):
                    nb_id = nb.get("node_id", "")
                    edge_key = tuple(sorted([nid, nb_id]))
                    if edge_key not in seen_edges:
                        seen_edges.add(edge_key)
                        edges.append({
                            "source": nid,
                            "target": nb_id,
                            "probability": nb.get("edge_prob", 1.0),
                            "reliability": nb.get("reliability_score", 1.0),
                            "status": nb.get("status", "alive"),
                        })

            return {"nodes": nodes, "edges": edges}

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Return list of all known node reports."""
        with self._lock:
            return list(self._nodes.values())

    def get_node_detail(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Return full detail for a specific node."""
        with self._lock:
            return self._nodes.get(node_id)

    def get_channels(self) -> List[Dict[str, Any]]:
        """Aggregate channel information from all node reports."""
        channels: Dict[str, Dict[str, Any]] = {}

        with self._lock:
            for nid, info in self._nodes.items():
                data = info.get("data", {})
                for ch_name in data.get("channels", []):
                    if ch_name not in channels:
                        channels[ch_name] = {
                            "name": ch_name,
                            "subscribers": [],
                            "total_subscribers": 0,
                        }
                    channels[ch_name]["subscribers"].append(nid)
                    channels[ch_name]["total_subscribers"] += 1

                # Add delivery buffer stats
                stats = data.get("stats", {})
                for key, val in stats.items():
                    if key.startswith("channel_") and key.endswith("_last_seq"):
                        ch = key[len("channel_"):-len("_last_seq")]
                        if ch in channels:
                            channels[ch].setdefault("delivery_stats", {})[nid] = {
                                "last_seq": val,
                                "pending": stats.get(f"channel_{ch}_pending", 0),
                            }

        return list(channels.values())

    def get_channel_messages(self, channel: str) -> List[Dict[str, Any]]:
        """
        Return message delivery status per node for a channel.
        Shows which seq_nums each node has delivered (green) vs pending (red).
        """
        result = []
        with self._lock:
            for nid, info in self._nodes.items():
                data = info.get("data", {})
                stats = data.get("stats", {})
                last_seq = stats.get(f"channel_{channel}_last_seq", 0)
                pending = stats.get(f"channel_{channel}_pending", 0)
                if channel in data.get("channels", []):
                    result.append({
                        "node_id": nid,
                        "address": info.get("address", ""),
                        "last_delivered_seq": last_seq,
                        "pending_count": pending,
                    })
        return result

    def get_channel_detail(self, channel: str) -> Dict[str, Any]:
        """
        Full detail for a channel: subscribers with roles, delivered messages,
        delivery buffer stats, and sequencer/core info.
        """
        subscribers = []
        all_delivered: List[Dict[str, Any]] = []
        sequencer_address = ""
        core_addresses: List[str] = []

        with self._lock:
            for nid, info in self._nodes.items():
                data = info.get("data", {})
                if channel not in data.get("channels", []):
                    continue

                # Core / sequencer role for this channel
                core_st = data.get("core_status", {}).get(channel, {})
                seq_st = data.get("sequencer_status", {}).get(channel, {})
                buf_st = data.get("delivery_buffers", {}).get(channel, {})

                sub_entry: Dict[str, Any] = {
                    "node_id": nid,
                    "address": info.get("address", ""),
                    "is_core": core_st.get("is_core", False),
                    "is_sequencer": seq_st.get("is_sequencer", False),
                    "sequencer_address": seq_st.get("sequencer_address", ""),
                    "last_known_seq": seq_st.get("last_known_seq", 0),
                    "election_in_progress": seq_st.get("election_in_progress", False),
                    "last_delivered_seq": buf_st.get("last_delivered_seq", 0),
                    "pending_count": buf_st.get("pending_count", 0),
                }
                subscribers.append(sub_entry)

                # Track global sequencer/core info (take from any node that reports it)
                if seq_st.get("sequencer_address"):
                    sequencer_address = seq_st["sequencer_address"]
                if core_st.get("core_addresses"):
                    core_addresses = core_st["core_addresses"]

                # Collect delivered messages from this node
                node_msgs = data.get("delivered_messages", {}).get(channel, [])
                for m in node_msgs:
                    m_copy = dict(m)
                    m_copy["reporting_node"] = nid
                    all_delivered.append(m_copy)

        # Deduplicate delivered messages by seq_num (keep first reporter)
        seen_seqs: Dict[int, Dict[str, Any]] = {}
        for m in all_delivered:
            seq = m.get("seq_num")
            if seq is not None and seq not in seen_seqs:
                seen_seqs[seq] = m
        unique_msgs = sorted(seen_seqs.values(), key=lambda x: x.get("seq_num", 0))

        return {
            "channel": channel,
            "subscribers": subscribers,
            "sequencer_address": sequencer_address,
            "core_addresses": core_addresses,
            "delivered_messages": unique_msgs[-100:],  # last 100
            "total_subscribers": len(subscribers),
        }

    def get_node_messages(
        self, node_id: str, channel: str
    ) -> List[Dict[str, Any]]:
        """Return delivered messages for a specific node and channel."""
        with self._lock:
            info = self._nodes.get(node_id)
            if not info:
                return []
            data = info.get("data", {})
            return data.get("delivered_messages", {}).get(channel, [])

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the monitor's transport listener and REST API."""
        # Determine transport type from config
        transport_type = self._config.transport

        if transport_type == "grpc":
            from transport.grpc.transport import GRPCTransport
            from config.grpc_config import GRPCConfig
            grpc_cfg = GRPCConfig()
            self._transport = GRPCTransport(
                grpc_cfg, host=self._host, port=self._port,
                local_node_id="monitor",
            )
        else:
            from transport.udp.transport import UDPTransport
            from config.udp_config import UDPConfig
            udp_cfg = UDPConfig()
            self._transport = UDPTransport(
                udp_cfg, host=self._host, port=self._port,
            )

        self._transport.start(on_message=self.handle_message)
        logger.info("[Monitor] Transport listening on %s:%d", self._host, self._port)

        # Start REST API in a background thread
        self._start_api()

    def _start_api(self) -> None:
        """Start the FastAPI REST server in a background thread."""
        try:
            from monitoring.api import create_app
            import uvicorn

            app = create_app(self)
            api_port = self._port + 1  # REST API on port+1

            self._api_thread = threading.Thread(
                target=uvicorn.run,
                kwargs={
                    "app": app,
                    "host": self._host,
                    "port": api_port,
                    "log_level": "warning",
                },
                name="MonitorAPI",
                daemon=True,
            )
            self._api_thread.start()
            logger.info("[Monitor] REST API running on http://%s:%d", self._host, api_port)
        except ImportError as exc:
            logger.warning("[Monitor] FastAPI/uvicorn not available: %s", exc)
            logger.warning("[Monitor] REST API disabled — install with: pip install fastapi uvicorn")

    def stop(self) -> None:
        """Shut down transport and API."""
        if self._transport:
            self._transport.stop()
        logger.info("[Monitor] Stopped")
