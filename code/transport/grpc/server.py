"""
transport/grpc/server.py  --  gRPC Node Server
=================================================
Wires together all components into a single runnable gossip node
using gRPC as the transport backend.

Usage:
    from config.grpc_config import get_grpc_config
    from transport.grpc.server import GRPCServer

    server = GRPCServer(host="0.0.0.0", port=50051, config=get_grpc_config())
    server.start()
    server.engine.publish("sensors", {"temp": 22.5})
    ...
    server.stop()
"""

from __future__ import annotations

import logging
from typing import Optional

from config.grpc_config import GRPCConfig, get_grpc_config
from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType
from core.heartbeat import HeartbeatManager
from components.discovery import PeerDiscovery
from components.dissemination import DisseminationEngine
from components.edge_manager import EdgeManager
from transport.grpc.transport import GRPCTransport

logger = logging.getLogger(__name__)


class GRPCServer:
    """
    Top-level node server using gRPC transport.

    Creates and wires:
      - LocalNode          (identity + neighbour table)
      - GRPCTransport      (send/recv over gRPC)
      - HeartbeatManager   (failure detection)
      - EdgeManager        (forwarding probabilities)
      - PeerDiscovery      (join + re-discovery)
      - DisseminationEngine(gossip forwarding)
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 50051,
        config: Optional[GRPCConfig] = None,
    ) -> None:
        self.config = config or get_grpc_config()

        # Core node
        self.node = LocalNode(host=host, port=port, config=self.config)

        # Transport
        self.transport = GRPCTransport(
            self.config,
            host=host,
            port=port,
            local_node_id=self.node.node_id,
        )

        # send_fn: the universal callable used by all components
        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            return self.transport.send(peer, msg)

        self._send_fn = send_fn

        # Heartbeat manager (created first — provides congestion data)
        self.heartbeat = HeartbeatManager(
            node=self.node,
            send_fn=send_fn,
            on_node_dead=lambda peer: self.discovery.trigger_rediscovery(peer),
        )

        # Edge manager (wired to heartbeat congestion + discovery callback)
        self.edge_manager = EdgeManager(
            node=self.node,
            on_edge_died=lambda nid: self.discovery.trigger_rediscovery(),
            get_congestion_fn=self.heartbeat.get_congestion,
        )

        # Now wire edge_manager back into heartbeat
        self.heartbeat.edge_manager = self.edge_manager

        # Discovery
        self.discovery = PeerDiscovery(
            node=self.node,
            send_fn=send_fn,
        )

        # Dissemination engine
        self.engine = DisseminationEngine(
            node=self.node,
            send_fn=send_fn,
            edge_manager=self.edge_manager,
        )

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    def _on_message(self, msg: Message, sender_info: Optional[NodeInfo]) -> None:
        """
        Central message dispatcher.
        Routes incoming messages to the appropriate handler based on msg_type.
        """
        if msg.msg_type in (MessageType.HEARTBEAT, MessageType.HEARTBEAT_ACK):
            self.heartbeat.handle_incoming(msg, sender_info)

        elif msg.msg_type == MessageType.PEER_LIST_REQUEST:
            if sender_info:
                self.discovery.handle_peer_list_request(msg, sender_info)

        elif msg.msg_type == MessageType.PEER_LIST_RESPONSE:
            self.discovery.handle_peer_list_response(msg)

        elif msg.msg_type == MessageType.GOSSIP:
            self.engine.receive(msg, sender_info)

        elif msg.msg_type in (MessageType.JOIN, MessageType.LEAVE):
            logger.info("[GRPCServer] Received %s from %s", msg.msg_type.name,
                        sender_info.node_id[:8] if sender_info else "unknown")

        else:
            logger.warning("[GRPCServer] Unknown message type: %s", msg.msg_type)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all components and join the network."""
        logger.info("[GRPCServer] Starting node %s", self.node.info)

        # Start transport listener
        self.transport.start(on_message=self._on_message)

        # Join the network
        self.discovery.join_network()

        # Start background services
        self.heartbeat.start()
        self.edge_manager.start()
        self.discovery.start()

        logger.info("[GRPCServer] Node %s is running on port %d", self.node.address, self.transport.port)

    def stop(self) -> None:
        """Gracefully shut down all components."""
        logger.info("[GRPCServer] Stopping node %s", self.node.address)

        self.discovery.shutdown()
        self.edge_manager.stop()
        self.heartbeat.stop()
        self.transport.stop()

        logger.info("[GRPCServer] Node %s stopped", self.node.address)

    def __repr__(self) -> str:
        return (
            f"GRPCServer(addr={self.node.address}, "
            f"neighbours={self.node.neighbour_count})"
        )
