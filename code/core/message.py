"""
core/message.py  —  C2: Message Modeling
=========================================
Defines:
  - LamportClock      : thread-safe monotonic counter for total ordering
  - MessageType       : enum covering all message kinds in the system
  - Message           : the canonical message struct exchanged between nodes
  - MessageFactory    : convenience factory that stamps messages with clock values
  - OutOfOrderBuffer  : per-sender buffer that holds and delivers messages
                        in total (Lamport) order

Design decisions implemented (see task_division.pdf):
  * Total ordering enforced via Lamport timestamps.
  * Out-of-order messages are held in a min-heap until the expected counter
    is available, then flushed in order.
  * Extensible 'metadata' dict for additional fields without breaking the interface.
  * Serialisation to/from plain dict so both gRPC (protobuf payload) and UDP
    (JSON-over-datagram) transports can encode as they see fit.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional


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

    # Total order — sequencing
    SEQUENCE_ASSIGN     = auto()  # Sequencer → network: assigns seq_num to a message
    SEQUENCER_ANNOUNCE  = auto()  # Sequencer announces itself for a channel

    # History recovery
    HISTORY_REQUEST     = auto()  # Node → core: request messages [from_seq..to_seq]
    HISTORY_RESPONSE    = auto()  # Core → node: batch of sequenced messages

    # Monitoring
    STATUS_REPORT       = auto()  # Node → monitor: periodic status payload
    STATUS_REQUEST      = auto()  # Monitor → node: request immediate status

    # Dynamic core nodes
    CORE_JOIN_REQUEST   = auto()  # Node → Sequencer: request to join core group
    CORE_JOIN_APPROVED  = auto()  # Sequencer → all: new core node announcement
    CORE_LEAVE          = auto()  # Core node → all: leaving core group (gap detected)

    # Dynamic sequencer election
    ELECTION_REQUEST    = auto()  # Core node → core nodes: sequencer suspected dead
    NEW_SEQUENCER       = auto()  # Core nodes → all: new sequencer elected
    SEQUENCER_HEARTBEAT = auto()  # Sequencer → core nodes: fast heartbeat with last_seq

    # Sequencer discovery (bootstrap)
    FIND_SEQUENCER_REQUEST  = auto()  # Node → broadcast: who is the sequencer?
    FIND_SEQUENCER_RESPONSE = auto()  # Any node → requester: sequencer info


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

    # Needed so Message objects can be stored in a heapq (min-heap).
    # We order by (lamport_ts, counter) to break ties deterministically.
    def __lt__(self, other: "Message") -> bool:
        return (self.lamport_ts, self.counter) < (other.lamport_ts, other.counter)

    def __le__(self, other: "Message") -> bool:
        return (self.lamport_ts, self.counter) <= (other.lamport_ts, other.counter)


# ---------------------------------------------------------------------------
# Out-of-Order Buffer
# ---------------------------------------------------------------------------

class OutOfOrderBuffer:
    """
    Per-sender buffer that enforces total-order delivery using Lamport
    timestamps and per-sender sequence counters.

    How it works
    ------------
    Every sender produces messages with a strictly increasing ``counter``
    (1, 2, 3, …).  The buffer tracks the *next expected counter* for each
    sender.  Incoming messages whose counter matches the expectation are
    delivered immediately; messages that arrive early are held in a
    per-sender min-heap and flushed once the gap is filled.

    Delivery order within a single sender is counter-order.
    Across senders, the ``deliver_callback`` is called in the order
    messages become deliverable (i.e. as gaps close).

    Parameters
    ----------
    deliver_callback:
        Called with each ``Message`` in delivery order.
        Must be thread-safe if ``receive()`` is called from multiple threads.
    max_buffer_size:
        Maximum number of buffered (out-of-order) messages per sender before
        the buffer force-flushes to avoid unbounded memory growth.
        Defaults to 1 000.

    Thread safety
    -------------
    A single lock serialises all mutations.  The deliver_callback is invoked
    while the lock is held, so it must not call back into the buffer.

    Usage example
    -------------
    >>> def on_deliver(msg):
    ...     print("delivered", msg.counter)
    >>> buf = OutOfOrderBuffer(on_deliver)
    >>> buf.receive(msg3)   # buffered — gap at 1 and 2
    >>> buf.receive(msg1)   # delivered immediately; msg2 still missing
    >>> buf.receive(msg2)   # delivered; then msg3 flushed automatically
    """

    def __init__(
        self,
        deliver_callback: Callable[[Message], None],
        max_buffer_size: int = 1_000,
    ) -> None:
        self._deliver = deliver_callback
        self._max_buf = max_buffer_size
        self._lock    = threading.Lock()

        # next expected counter per sender_id
        self._next_expected: Dict[str, int] = {}

        # min-heap of buffered messages per sender_id
        self._pending: Dict[str, List[Message]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def receive(self, msg: Message) -> None:
        """
        Accept an incoming message.

        - If it is the next expected message from its sender, deliver it
          and then flush any consecutive messages that were waiting.
        - Otherwise, buffer it for later delivery.
        - If the buffer for this sender exceeds *max_buffer_size*, force-
          flush all buffered messages in order (gap tolerance exceeded).
        """
        with self._lock:
            sender = msg.sender_id

            # Initialise state for a new sender on first contact
            if sender not in self._next_expected:
                self._next_expected[sender] = 1
                self._pending[sender]       = []

            expected = self._next_expected[sender]

            if msg.counter == expected:
                # Happy path: deliver and flush any waiting messages
                self._deliver_and_advance(sender, msg)
                self._flush(sender)

            elif msg.counter > expected:
                # Future message: buffer it
                heapq.heappush(self._pending[sender], msg)

                # Safety valve: force-flush if buffer grows too large
                if len(self._pending[sender]) >= self._max_buf:
                    self._force_flush(sender)

            # msg.counter < expected → duplicate or already delivered; drop silently

    def reset_sender(self, sender_id: str) -> None:
        """
        Discard all buffered state for *sender_id*.

        Call this when C1 signals that a peer has been replaced (re-discovery
        after failure) so stale counters don't block the new peer's messages.
        """
        with self._lock:
            self._next_expected.pop(sender_id, None)
            self._pending.pop(sender_id, None)

    def pending_counts(self) -> Dict[str, int]:
        """Return the number of buffered (undelivered) messages per sender."""
        with self._lock:
            return {s: len(q) for s, q in self._pending.items()}

    # ------------------------------------------------------------------
    # Internal helpers  (called with self._lock already held)
    # ------------------------------------------------------------------

    def _deliver_and_advance(self, sender: str, msg: Message) -> None:
        """Deliver one message and bump the expected counter."""
        self._deliver(msg)
        self._next_expected[sender] = msg.counter + 1

    def _flush(self, sender: str) -> None:
        """
        Deliver consecutive buffered messages whose counter now matches
        the expected value.
        """
        heap = self._pending[sender]
        while heap and heap[0].counter == self._next_expected[sender]:
            nxt = heapq.heappop(heap)
            self._deliver_and_advance(sender, nxt)

    def _force_flush(self, sender: str) -> None:
        """
        Deliver *all* buffered messages for *sender* in counter order,
        skipping the gap-filling requirement.  Used as a safety valve.
        """
        heap = self._pending[sender]
        # Sort is implicit: heap invariant means heappop gives min order
        while heap:
            msg = heapq.heappop(heap)
            self._deliver(msg)
            self._next_expected[sender] = msg.counter + 1


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

    def sequence_assign(
        self, channel: str, original_msg: Dict[str, Any], seq_num: int
    ) -> Message:
        """Sequencer wraps a GOSSIP message with a global sequence number."""
        return self.create(
            MessageType.SEQUENCE_ASSIGN,
            channel=channel,
            content=original_msg,
            metadata={"seq_num": seq_num},
        )

    def sequencer_announce(self, channel: str) -> Message:
        """Announce this node as the sequencer for a channel."""
        return self.create(
            MessageType.SEQUENCER_ANNOUNCE,
            channel=channel,
        )

    def history_request(
        self, channel: str, from_seq: int, to_seq: int = -1
    ) -> Message:
        """Request sequenced messages in range [from_seq, to_seq] from a core node."""
        return self.create(
            MessageType.HISTORY_REQUEST,
            channel=channel,
            metadata={"from_seq": from_seq, "to_seq": to_seq},
        )

    def history_response(self, channel: str, messages: List[Dict[str, Any]]) -> Message:
        """Respond with a batch of sequenced messages."""
        return self.create(
            MessageType.HISTORY_RESPONSE,
            channel=channel,
            content=messages,
        )

    def status_report(self, status_data: Dict[str, Any]) -> Message:
        """Periodic status report sent to the monitoring server."""
        return self.create(MessageType.STATUS_REPORT, content=status_data)

    # --- Dynamic core nodes ---

    def core_join_request(self, channel: str) -> Message:
        """Request to join the core group for a channel."""
        return self.create(MessageType.CORE_JOIN_REQUEST, channel=channel)

    def core_join_approved(
        self, channel: str, new_core_address: str, core_list: List[str]
    ) -> Message:
        """Announce a new core node to the entire network."""
        return self.create(
            MessageType.CORE_JOIN_APPROVED,
            channel=channel,
            content={"new_core_address": new_core_address, "core_list": core_list},
        )

    def core_leave(
        self, channel: str, leaving_address: str, core_list: List[str]
    ) -> Message:
        """Announce departure from the core group."""
        return self.create(
            MessageType.CORE_LEAVE,
            channel=channel,
            content={"leaving_address": leaving_address, "core_list": core_list},
        )

    # --- Dynamic sequencer election ---

    def election_request(self, channel: str, last_known_seq: int) -> Message:
        """Request an election because the sequencer is suspected dead."""
        return self.create(
            MessageType.ELECTION_REQUEST,
            channel=channel,
            metadata={"last_known_seq": last_known_seq},
        )

    def new_sequencer(
        self,
        channel: str,
        sequencer_address: str,
        next_seq_num: int,
        core_list: List[str],
    ) -> Message:
        """Announce the newly elected sequencer."""
        return self.create(
            MessageType.NEW_SEQUENCER,
            channel=channel,
            content={
                "sequencer_address": sequencer_address,
                "next_seq_num": next_seq_num,
                "core_list": core_list,
            },
        )

    def sequencer_heartbeat(self, channel: str, last_seq: int) -> Message:
        """Fast heartbeat from sequencer to core nodes, includes last assigned seq."""
        return self.create(
            MessageType.SEQUENCER_HEARTBEAT,
            channel=channel,
            metadata={"last_seq": last_seq},
        )

    # --- Sequencer discovery ---

    def find_sequencer_request(self, channel: str) -> Message:
        """Broadcast: who is the sequencer for this channel?"""
        return self.create(MessageType.FIND_SEQUENCER_REQUEST, channel=channel)

    def find_sequencer_response(
        self, channel: str, sequencer_address: str, core_list: List[str]
    ) -> Message:
        """Response with current sequencer info."""
        return self.create(
            MessageType.FIND_SEQUENCER_RESPONSE,
            channel=channel,
            content={"sequencer_address": sequencer_address, "core_list": core_list},
        )