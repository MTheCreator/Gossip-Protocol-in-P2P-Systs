"""
core/message.py  —  C2: Message Modeling
=========================================
Defines:
  - LamportClock  : thread-safe monotonic counter for total ordering
  - MessageType   : enum covering all message kinds in the system
  - Message       : the canonical message struct exchanged between nodes
  - MessageFactory: convenience factory that stamps messages with clock values

Design decisions implemented (see task_division.pdf):
  * Total ordering enforced via Lamport timestamps.
  * Extensible 'metadata' dict for additional fields without breaking the interface.
  * Serialisation to/from plain dict so both gRPC (protobuf payload) and UDP
    (JSON-over-datagram) transports can encode as they see fit.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Lamport Clock
# ---------------------------------------------------------------------------

class LamportClock:
    """
    Thread-safe Lamport logical clock.

    Rules:
      - send()    : increment before sending, attach value to message
      - receive() : clock = max(local, received) + 1
    """

    def __init__(self, initial: int = 0) -> None:
        self._time: int = initial
        self._lock = threading.Lock()

    @property
    def time(self) -> int:
        with self._lock:
            return self._time

    def tick(self) -> int:
        """Increment on a local event. Returns new value."""
        with self._lock:
            self._time += 1
            return self._time

    def send(self) -> int:
        """Call before sending a message. Returns timestamp to attach."""
        return self.tick()

    def receive(self, received_time: int) -> int:
        """
        Call on message receipt.
        Updates clock to max(local, received) + 1.
        Returns updated clock value.
        """
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def __repr__(self) -> str:
        return f"LamportClock(time={self._time})"


# ---------------------------------------------------------------------------
# Message Types
# ---------------------------------------------------------------------------

class MessageType(Enum):
    # Application-level gossip message
    GOSSIP        = auto()

    # Heartbeat protocol (C3b)
    HEARTBEAT     = auto()
    HEARTBEAT_ACK = auto()

    # Peer discovery protocol (C1)
    PEER_LIST_REQUEST  = auto()   # "Give me your known peers"
    PEER_LIST_RESPONSE = auto()   # Response with list of peer dicts

    # Control / management
    JOIN          = auto()
    LEAVE         = auto()


# ---------------------------------------------------------------------------
# Node status (used in heartbeat neighbour table)
# ---------------------------------------------------------------------------

class NodeStatus(Enum):
    ALIVE    = "alive"
    SUSPECT  = "suspect"
    DEAD     = "dead"


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------

@dataclass
class Message:
    """
    Canonical message exchanged between nodes.

    Fields (from spec):
      msg_id     : globally unique ID (UUID4 hex) for deduplication
      msg_type   : MessageType enum value
      sender_id  : hex node ID of the originating node
      timestamp  : wall-clock time at creation (float, seconds since epoch)
      lamport_ts : Lamport logical timestamp for total ordering
      counter    : monotonic per-sender sequence number (extra ordering aid)
      channel    : pub/sub topic string (empty string for control messages)
      content    : application payload (arbitrary JSON-serialisable value)
      metadata   : extensible dict for additional fields (e.g. TTL, hop count)
    """

    msg_id:     str
    msg_type:   MessageType
    sender_id:  str
    timestamp:  float
    lamport_ts: int
    counter:    int
    channel:    str
    content:    Any                          = None
    metadata:   Dict[str, Any]              = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["msg_type"] = self.msg_type.name
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_bytes(self) -> bytes:
        return self.to_json().encode("utf-8")

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Message":
        d = dict(d)
        d["msg_type"] = MessageType[d["msg_type"]]
        return cls(**d)

    @classmethod
    def from_json(cls, s: str) -> "Message":
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_bytes(cls, b: bytes) -> "Message":
        return cls.from_json(b.decode("utf-8"))

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def fingerprint(self) -> str:
        """SHA-1 of (sender_id, counter) — stable dedup key across hops."""
        raw = f"{self.sender_id}:{self.counter}".encode()
        return hashlib.sha1(raw).hexdigest()

    def __repr__(self) -> str:
        return (
            f"Message(id={self.msg_id[:8]}…, type={self.msg_type.name}, "
            f"sender={self.sender_id[:8]}…, lamport={self.lamport_ts}, "
            f"channel={self.channel!r})"
        )


# ---------------------------------------------------------------------------
# Message Factory
# ---------------------------------------------------------------------------

class MessageFactory:
    """
    Stamps outgoing messages with Lamport clock values and sequence numbers.
    One factory instance per node.
    """

    def __init__(self, node_id: str, clock: LamportClock) -> None:
        self.node_id = node_id
        self.clock   = clock
        self._counter_lock = threading.Lock()
        self._counter: int = 0

    def _next_counter(self) -> int:
        with self._counter_lock:
            self._counter += 1
            return self._counter

    def create(
        self,
        msg_type: MessageType,
        channel: str = "",
        content: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Message:
        lamport_ts = self.clock.send()
        return Message(
            msg_id     = uuid.uuid4().hex,
            msg_type   = msg_type,
            sender_id  = self.node_id,
            timestamp  = time.time(),
            lamport_ts = lamport_ts,
            counter    = self._next_counter(),
            channel    = channel,
            content    = content,
            metadata   = metadata or {},
        )

    def gossip(self, channel: str, content: Any, **meta) -> Message:
        return self.create(MessageType.GOSSIP, channel=channel, content=content, metadata=meta)

    def heartbeat(self) -> Message:
        return self.create(MessageType.HEARTBEAT)

    def heartbeat_ack(self, original: Message) -> Message:
        return self.create(
            MessageType.HEARTBEAT_ACK,
            metadata={"ack_for": original.msg_id},
        )

    def peer_list_request(self) -> Message:
        """Request the peer list from a remote node."""
        return self.create(MessageType.PEER_LIST_REQUEST)

    def peer_list_response(self, peers: List[Dict[str, Any]]) -> Message:
        """Respond with our known peer list."""
        return self.create(
            MessageType.PEER_LIST_RESPONSE,
            content=peers,
        )