"""
transport/grpc/transport.py  --  gRPC Transport Implementation
================================================================
Uses gRPC unary RPCs for message delivery.

The proto defines a simple GossipService with a single
SendMessage(GossipEnvelope) -> Ack RPC.  The envelope contains
the JSON-serialised Message bytes (same format as UDP), so the
upper layers are transport-agnostic.
"""

from __future__ import annotations

import logging
import threading
from concurrent import futures
from typing import Optional

import grpc

from core.node import NodeInfo
from core.message import Message
from transport.base import TransportBase, OnMessageFn
from config.grpc_config import GRPCConfig

# Generated proto stubs
from transport.grpc.proto import gossip_pb2
from transport.grpc.proto import gossip_pb2_grpc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# gRPC Service Implementation (server side)
# ---------------------------------------------------------------------------

class _GossipServicer(gossip_pb2_grpc.GossipServiceServicer):
    """Handles incoming SendMessage RPCs."""

    def __init__(self, on_message: OnMessageFn) -> None:
        self._on_message = on_message

    def SendMessage(self, request, context):
        """Receive a GossipEnvelope, parse it, and dispatch."""
        try:
            msg = Message.from_bytes(request.payload)
            sender_info = NodeInfo(
                node_id=request.sender_id,
                host=request.sender_host,
                port=request.sender_port,
            )
            self._on_message(msg, sender_info)
            return gossip_pb2.Ack(success=True)
        except Exception as exc:
            logger.error("[gRPC] Error handling message: %s", exc)
            return gossip_pb2.Ack(success=False, error=str(exc))


# ---------------------------------------------------------------------------
# GRPCTransport
# ---------------------------------------------------------------------------

class GRPCTransport(TransportBase):
    """
    gRPC-based transport for the gossip overlay.

    Usage:
        transport = GRPCTransport(config, host="0.0.0.0", port=50051)
        transport.start(on_message=my_handler)
        transport.send(peer_info, msg)
        ...
        transport.stop()
    """

    def __init__(
        self,
        config: GRPCConfig,
        host: str = "0.0.0.0",
        port: int = 50051,
        local_node_id: str = "",
    ) -> None:
        self.config = config
        self.host = host
        self.port = port
        self.local_node_id = local_node_id

        self._server: Optional[grpc.Server] = None
        self._channel_cache: dict[str, grpc.Channel] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send(self, peer: NodeInfo, msg: Message) -> bool:
        """Send a message via a unary gRPC call."""
        target = f"{peer.host}:{peer.port}"

        try:
            channel = self._get_channel(target)
            stub = gossip_pb2_grpc.GossipServiceStub(channel)

            envelope = gossip_pb2.GossipEnvelope(
                payload=msg.to_bytes(),
                sender_id=self.local_node_id,
                sender_host=self.host,
                sender_port=self.port,
            )

            response = stub.SendMessage(
                envelope,
                timeout=self.config.keepalive_timeout_ms / 1000.0,
            )

            if response.success:
                logger.debug("[gRPC] Sent %s to %s", msg.msg_type.name, target)
                return True
            else:
                logger.warning(
                    "[gRPC] Send to %s returned error: %s",
                    target, response.error,
                )
                return False

        except grpc.RpcError as exc:
            logger.warning("[gRPC] Send to %s failed: %s", target, exc)
            # Remove stale channel
            self._remove_channel(target)
            return False

    def _get_channel(self, target: str) -> grpc.Channel:
        """Get or create a cached channel for the target."""
        with self._lock:
            if target not in self._channel_cache:
                options = [
                    ("grpc.max_send_message_length", self.config.max_message_size_bytes),
                    ("grpc.max_receive_message_length", self.config.max_message_size_bytes),
                    ("grpc.keepalive_time_ms", self.config.keepalive_time_ms),
                    ("grpc.keepalive_timeout_ms", self.config.keepalive_timeout_ms),
                ]
                self._channel_cache[target] = grpc.insecure_channel(target, options=options)
            return self._channel_cache[target]

    def _remove_channel(self, target: str) -> None:
        """Close and remove a cached channel."""
        with self._lock:
            channel = self._channel_cache.pop(target, None)
            if channel:
                try:
                    channel.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Server (receive side)
    # ------------------------------------------------------------------

    def start(self, on_message: OnMessageFn) -> None:
        """Start the gRPC server to receive messages."""
        if self._server is not None:
            return

        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=self.config.max_workers),
            options=[
                ("grpc.max_send_message_length", self.config.max_message_size_bytes),
                ("grpc.max_receive_message_length", self.config.max_message_size_bytes),
            ],
        )

        servicer = _GossipServicer(on_message)
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(servicer, self._server)

        bind_addr = f"{self.host}:{self.port}"
        self._server.add_insecure_port(bind_addr)
        self._server.start()

        logger.info("[gRPC] Server listening on %s", bind_addr)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Gracefully shut down the server and close all channels."""
        if self._server:
            self._server.stop(grace=2.0)
            self._server = None
            logger.info("[gRPC] Server stopped")

        with self._lock:
            for target, channel in self._channel_cache.items():
                try:
                    channel.close()
                except Exception:
                    pass
            self._channel_cache.clear()

        logger.info("[gRPC] All channels closed")
