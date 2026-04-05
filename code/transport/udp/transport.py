"""
transport/udp/transport.py  --  UDP Transport Implementation
==============================================================
Connectionless UDP transport for the gossip overlay.

Messages are serialised as JSON-over-UDP (one message per datagram).
The receive loop runs in a daemon thread and dispatches incoming
messages to a user-supplied on_message callback.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Optional

from core.node import NodeInfo
from core.message import Message
from transport.base import TransportBase, OnMessageFn
from config.udp_config import UDPConfig

logger = logging.getLogger(__name__)


def _is_windows_udp_connreset(exc: OSError) -> bool:
    """Windows may raise WSAECONNRESET on UDP recv after sending to a closed port."""
    return os.name == "nt" and getattr(exc, "winerror", None) == 10054


def _is_expected_socket_close(exc: OSError) -> bool:
    """Socket was closed while recv loop was active (normal during shutdown)."""
    winerror = getattr(exc, "winerror", None)
    return winerror in {10038} or exc.errno in {9}


class UDPTransport(TransportBase):
    """
    UDP-based transport.

    Usage:
        transport = UDPTransport(config, host="0.0.0.0", port=6001)
        transport.start(on_message=my_handler)
        transport.send(peer_info, msg)
        ...
        transport.stop()
    """

    def __init__(
        self,
        config: UDPConfig,
        host: str = "0.0.0.0",
        port: int = 6001,
    ) -> None:
        self.config = config
        self.host = host
        self.port = port

        self._sock: Optional[socket.socket] = None
        self._recv_thread: Optional[threading.Thread] = None
        self._running = False
        self._on_message: Optional[OnMessageFn] = None

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send(self, peer: NodeInfo, msg: Message) -> bool:
        """Send a message as a single UDP datagram."""
        if self._sock is None:
            logger.error("[UDPTransport] Socket not initialised")
            return False

        payload = msg.to_bytes()
        if len(payload) > self.config.max_datagram_size:
            logger.warning(
                "[UDPTransport] Message too large (%d > %d), sending anyway",
                len(payload),
                self.config.max_datagram_size,
            )

        try:
            self._sock.sendto(payload, (peer.host, peer.port))
            logger.debug(
                "[UDPTransport] Sent %s to %s:%d (%d bytes)",
                msg.msg_type.name,
                peer.host,
                peer.port,
                len(payload),
            )
            return True
        except OSError as exc:
            logger.warning(
                "[UDPTransport] Send to %s:%d failed: %s",
                peer.host,
                peer.port,
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    def start(self, on_message: OnMessageFn) -> None:
        """Bind the socket and start the receive loop."""
        if self._running:
            return

        self._on_message = on_message

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # On Windows, UDP recv can surface ICMP port-unreachable as a fatal
        # WSAECONNRESET unless this ioctl is disabled.
        if os.name == "nt" and hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self._sock.ioctl(socket.SIO_UDP_CONNRESET, 0)
            except Exception:
                logger.debug("[UDPTransport] Failed to disable SIO_UDP_CONNRESET")

        self._sock.settimeout(self.config.socket_timeout)
        self._sock.bind((self.host, self.port))

        self._running = True
        self._recv_thread = threading.Thread(
            target=self._recv_loop,
            name=f"UDPRecv-{self.port}",
            daemon=True,
        )
        self._recv_thread.start()
        logger.info("[UDPTransport] Listening on %s:%d", self.host, self.port)

    def _recv_loop(self) -> None:
        """Continuously receive datagrams and dispatch them."""
        while self._running:
            try:
                data, addr = self._sock.recvfrom(self.config.recv_buffer_size)
            except socket.timeout:
                continue
            except OSError as exc:
                if _is_windows_udp_connreset(exc):
                    logger.debug("[UDPTransport] Ignoring expected UDP reset: %s", exc)
                    continue
                if not self._running or _is_expected_socket_close(exc):
                    break
                if self._running:
                    logger.warning(
                        "[UDPTransport] Socket recv error (continuing): %s", exc
                    )
                time.sleep(0.05)
                continue

            # Parse message
            try:
                msg = Message.from_bytes(data)
            except Exception as exc:
                logger.warning(
                    "[UDPTransport] Failed to parse datagram from %s: %s",
                    addr,
                    exc,
                )
                continue

            # Resolve sender info from the datagram source address.
            # Use msg.sender_id as the identity key so ID bookkeeping stays
            # consistent even when bind address differs (e.g., 0.0.0.0 vs 127.0.0.1).
            sender_host, sender_port = addr
            sender_id = (
                msg.sender_id or NodeInfo.from_address(sender_host, sender_port).node_id
            )
            sender_info = NodeInfo(
                node_id=sender_id, host=sender_host, port=sender_port
            )

            # Dispatch
            if self._on_message:
                try:
                    self._on_message(msg, sender_info)
                except Exception as exc:
                    logger.error(
                        "[UDPTransport] on_message handler error: %s",
                        exc,
                    )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Close the socket and stop the receive loop."""
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        if self._recv_thread:
            self._recv_thread.join(timeout=2.0)
            self._recv_thread = None
        logger.info("[UDPTransport] Stopped")
