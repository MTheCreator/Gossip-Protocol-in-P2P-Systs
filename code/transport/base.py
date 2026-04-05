"""
transport/base.py  --  Transport Base Interface
=================================================
Abstract base class defining the transport contract.

All higher-level components (HeartbeatManager, PeerDiscovery,
DisseminationEngine) depend only on a `send_fn` callable, never
on a concrete transport.  This ABC exists for documentation and
type-checking.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Optional

from core.node import NodeInfo
from core.message import Message


# The universal send signature used across all components
SendFn = Callable[[NodeInfo, Message], bool]

# The universal incoming-message handler signature
OnMessageFn = Callable[[Message, Optional[NodeInfo]], None]


class TransportBase(ABC):
    """
    Contract for a transport backend (UDP, gRPC, simulation, etc.).

    Implementations must:
      - Send serialised messages to a remote peer via `send()`
      - Listen for incoming messages and dispatch them to `on_message`
      - Support clean startup/shutdown
    """

    @abstractmethod
    def send(self, peer: NodeInfo, msg: Message) -> bool:
        """
        Send a message to a remote peer.

        Returns True if the send was successful (or at least dispatched),
        False on immediate failure (e.g. socket error, timeout).
        """
        ...

    @abstractmethod
    def start(self, on_message: OnMessageFn) -> None:
        """
        Start the transport listener.

        Parameters
        ----------
        on_message : callable(Message, Optional[NodeInfo])
            Function called for every incoming message.
            The transport should parse the raw bytes into a Message
            and (if possible) resolve the sender's NodeInfo.
        """
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop the transport listener and release resources."""
        ...
