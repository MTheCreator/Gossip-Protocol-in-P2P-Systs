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

import json
import logging
import socket
import struct
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
        self._traffic_events: List[Dict[str, Any]] = []
        self._next_traffic_id = 1
        self._traffic_ttl_sec = 12.0
        self._traffic_max_events = 10000
        self._lock = threading.Lock()

        # Transport (initialized in start()) — kept for STATUS_REQUEST
        self._transport = None

        # TCP listener for status reports
        self._tcp_thread: Optional[threading.Thread] = None
        self._tcp_server: Optional[socket.socket] = None
        self._tcp_stop = threading.Event()

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
            self._ingest_traffic_events_unlocked(node_id=node_id, report=data)

        logger.debug("[Monitor] Received status from %s (%s)", node_id[:8], address)

    # ------------------------------------------------------------------
    # Data access (used by REST API)
    # ------------------------------------------------------------------

    def get_topology(self) -> Dict[str, Any]:
        """Return current network topology as JSON-serializable dict."""
        now = time.time()
        stale_threshold = 2.0  # seconds without a report → mark dead

        with self._lock:
            nodes = []
            edges = []
            node_status: Dict[str, str] = {}

            for nid, info in self._nodes.items():
                data = info.get("data", {})
                last_seen = info.get("last_seen", 0)
                is_stale = (now - last_seen) > stale_threshold

                status = "dead" if is_stale else data.get("status", "unknown")
                node_status[nid] = status

                nodes.append({
                    "id": nid,
                    "label": nid[:8],
                    "address": info.get("address", ""),
                    "status": status,
                    "is_seed": data.get("is_seed", False),
                    "neighbour_count": data.get("neighbour_count", 0) if not is_stale else 0,
                    "channels": data.get("channels", []),
                    "stats": data.get("stats", {}),
                    "core_status": {} if is_stale else data.get("core_status", {}),
                    "sequencer_status": {} if is_stale else data.get("sequencer_status", {}),
                    "delivery_buffers": {} if is_stale else data.get("delivery_buffers", {}),
                })

            # Keep directed neighbour links so the UI can render per-direction
            # heartbeat/broadcast activity.
            for nid, info in self._nodes.items():
                data = info.get("data", {})
                src_status = node_status.get(nid, "unknown")
                if src_status == "dead":
                    continue

                for nb in data.get("neighbours", []):
                    nb_id = nb.get("node_id", "")
                    if not nb_id or nb_id == nid:
                        continue

                    edges.append({
                        "id": f"{nid}->{nb_id}",
                        "source": nid,
                        "target": nb_id,
                        "directed": True,
                        "probability": nb.get("edge_prob", 1.0),
                        "reliability": nb.get("reliability_score", 1.0),
                        "status": nb.get("status", "alive"),
                        "source_status": src_status,
                        "target_status": node_status.get(nb_id, "unknown"),
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

    def get_traffic(self, since_id: int = 0) -> Dict[str, Any]:
        """
        Return recent directed send events used by edge-flow animations.
        Events are ephemeral and trimmed by age + max size.
        """
        with self._lock:
            self._prune_traffic_unlocked()
            events = [
                dict(ev) for ev in self._traffic_events
                if ev.get("id", 0) > since_id
            ]
            latest_id = self._next_traffic_id - 1
            return {"events": events, "latest_id": latest_id}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the monitor's TCP listener, transport, and REST API."""
        # Start TCP listener for status reports on port+2
        self._start_tcp_listener()

        # Determine transport type from config (kept for STATUS_REQUEST)
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

    def _start_tcp_listener(self) -> None:
        """Start a TCP server that receives length-prefixed JSON status reports."""
        tcp_port = self._port + 2
        self._tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._tcp_server.settimeout(1.0)
        self._tcp_server.bind((self._host, tcp_port))
        self._tcp_server.listen(32)

        def _tcp_accept_loop():
            while not self._tcp_stop.is_set():
                try:
                    conn, addr = self._tcp_server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(
                    target=self._handle_tcp_conn, args=(conn,),
                    daemon=True, name="MonitorTCP"
                ).start()

        self._tcp_thread = threading.Thread(
            target=_tcp_accept_loop, daemon=True, name="MonitorTCPAccept"
        )
        self._tcp_thread.start()
        logger.info("[Monitor] TCP status listener on %s:%d", self._host, tcp_port)

    def _handle_tcp_conn(self, conn: socket.socket) -> None:
        """Read one length-prefixed JSON report from a TCP connection."""
        try:
            conn.settimeout(5.0)
            # Read 4-byte length prefix
            header = b""
            while len(header) < 4:
                chunk = conn.recv(4 - len(header))
                if not chunk:
                    return
                header += chunk
            length = struct.unpack("!I", header)[0]
            if length > 1_000_000:  # sanity limit: 1 MB
                return

            # Read payload
            data = b""
            while len(data) < length:
                chunk = conn.recv(min(length - len(data), 65536))
                if not chunk:
                    return
                data += chunk

            report = json.loads(data.decode("utf-8"))
            self._ingest_status_report(report)
        except Exception as exc:
            logger.debug("[Monitor] TCP recv error: %s", exc)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _ingest_status_report(self, data: dict) -> None:
        """Store a status report received over TCP (same logic as _handle_status_report)."""
        node_id = data.get("node_id", "")
        if not node_id:
            return
        address = data.get("address", "")

        with self._lock:
            self._nodes[node_id] = {
                "node_id": node_id,
                "address": address,
                "timestamp": time.time(),
                "last_seen": time.time(),
                "data": data,
            }
            self._ingest_traffic_events_unlocked(node_id=node_id, report=data)

        logger.debug("[Monitor] TCP status from %s (%s)", node_id[:8], address)

    def _ingest_traffic_events_unlocked(self, node_id: str, report: Dict[str, Any]) -> None:
        """Ingest transient send events from one status report. Caller holds _lock."""
        raw_events = report.get("traffic_events", [])
        if not isinstance(raw_events, list) or not raw_events:
            self._prune_traffic_unlocked()
            return

        now = time.time()
        for ev in raw_events:
            if not isinstance(ev, dict):
                continue

            source_id = str(ev.get("source_id") or node_id)
            target_id = str(ev.get("target_id") or "")
            msg_type = str(ev.get("msg_type") or "")
            channel = str(ev.get("channel") or "")
            if not source_id or not target_id or not msg_type:
                continue

            ts_raw = ev.get("ts")
            try:
                ts = float(ts_raw) if ts_raw is not None else now
            except (TypeError, ValueError):
                ts = now
            if ts <= 0 or ts > now + 30:
                ts = now

            self._traffic_events.append({
                "id": self._next_traffic_id,
                "ts": ts,
                "source_id": source_id,
                "target_id": target_id,
                "msg_type": msg_type,
                "channel": channel,
            })
            self._next_traffic_id += 1

        self._prune_traffic_unlocked()

    def _prune_traffic_unlocked(self) -> None:
        """Drop stale traffic events. Caller holds _lock."""
        cutoff = time.time() - self._traffic_ttl_sec
        if self._traffic_events:
            self._traffic_events = [
                ev for ev in self._traffic_events if ev.get("ts", 0) >= cutoff
            ]
            if len(self._traffic_events) > self._traffic_max_events:
                self._traffic_events = self._traffic_events[-self._traffic_max_events:]

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
        """Shut down TCP listener, transport, and API."""
        self._tcp_stop.set()
        if self._tcp_server:
            try:
                self._tcp_server.close()
            except Exception:
                pass
        if self._transport:
            self._transport.stop()
        logger.info("[Monitor] Stopped")
