"""
components/ordered_delivery.py  —  Per-Channel Ordered Delivery Buffer
=======================================================================
Ensures that subscribers receive messages in strict seq_num order as
assigned by the ChannelSequencer.

This is fundamentally different from the existing OutOfOrderBuffer in
core/message.py, which tracks per-sender counters.  This buffer tracks
per-channel GLOBAL sequence numbers assigned by the sequencer.

Behaviour
---------
  - Messages arriving with seq_num == next_expected are delivered immediately.
  - Future messages (seq_num > next_expected) are buffered.
  - Past messages (seq_num < next_expected) are silently dropped (already delivered).
  - If a gap persists longer than gap_timeout, a HISTORY_REQUEST is sent
    to a core node to fill the gap.

Thread safety
-------------
All mutations are serialised through a single lock.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, Optional

from core.message import Message

logger = logging.getLogger(__name__)

# Type alias: fn(channel, from_seq, to_seq) → requests history from a core node
RequestHistoryFn = Callable[[str, int, int], None]


class OrderedDeliveryBuffer:
    """
    Per-channel buffer that delivers messages in strict seq_num order.

    Usage:
        buf = OrderedDeliveryBuffer(
            channel="sensors",
            deliver_callback=my_handler,
            request_history_fn=ask_core_node,
            initial_seq=0,
        )
        buf.receive(seq_num=1, msg=msg1)   # delivered immediately
        buf.receive(seq_num=3, msg=msg3)   # buffered (gap at 2)
        buf.receive(seq_num=2, msg=msg2)   # delivered, then msg3 flushed

        # Periodically call check_gaps() to detect stale gaps
    """

    def __init__(
        self,
        channel: str,
        deliver_callback: Callable[[Message], None],
        request_history_fn: RequestHistoryFn,
        gap_timeout: float = 5.0,
        initial_seq: int = 0,
    ) -> None:
        self._channel = channel
        self._deliver = deliver_callback
        self._request_history = request_history_fn
        self._gap_timeout = gap_timeout

        self._next_expected: int = initial_seq + 1
        self._buffer: Dict[int, Message] = {}
        self._lock = threading.Lock()
        self._gap_detected_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def receive(self, seq_num: int, msg: Message) -> None:
        """
        Accept a sequenced message.

        - If seq_num == next_expected → deliver immediately, then flush buffer.
        - If seq_num >  next_expected → buffer it (gap detected).
        - If seq_num <  next_expected → already delivered, drop silently.
        """
        with self._lock:
            if seq_num < self._next_expected:
                return  # already delivered

            if seq_num == self._next_expected:
                self._deliver_and_advance(msg)
                self._flush()
                # If buffer is now empty, gap is resolved
                if not self._buffer:
                    self._gap_detected_at = None
            else:
                # Future message — buffer it
                self._buffer[seq_num] = msg
                if self._gap_detected_at is None:
                    self._gap_detected_at = time.time()
                    logger.debug(
                        "[OrderedBuf/%s] Gap detected: expected seq=%d, got seq=%d",
                        self._channel, self._next_expected, seq_num,
                    )

    def check_gaps(self) -> None:
        """
        Called periodically (e.g. every 2s).  If a gap has persisted longer
        than gap_timeout, send a HISTORY_REQUEST to a core node to fill it.
        """
        with self._lock:
            if self._gap_detected_at is None:
                return
            elapsed = time.time() - self._gap_detected_at
            if elapsed < self._gap_timeout:
                return

            # Determine the range of missing seq_nums
            if self._buffer:
                max_buffered = max(self._buffer.keys())
            else:
                return  # nothing to fill

            from_seq = self._next_expected
            to_seq = max_buffered

            # Reset timer so we don't spam requests
            self._gap_detected_at = time.time()

        # Request outside the lock to avoid deadlock
        logger.info(
            "[OrderedBuf/%s] Gap timeout — requesting history [%d..%d]",
            self._channel, from_seq, to_seq,
        )
        try:
            self._request_history(self._channel, from_seq, to_seq)
        except Exception as exc:
            logger.warning(
                "[OrderedBuf/%s] Failed to request history: %s",
                self._channel, exc,
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def last_delivered_seq(self) -> int:
        with self._lock:
            return self._next_expected - 1

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._buffer)

    @property
    def next_expected(self) -> int:
        with self._lock:
            return self._next_expected

    # ------------------------------------------------------------------
    # Internal helpers (called with self._lock held)
    # ------------------------------------------------------------------

    def _deliver_and_advance(self, msg: Message) -> None:
        self._deliver(msg)
        self._next_expected += 1

    def _flush(self) -> None:
        """Deliver consecutive buffered messages whose seq matches expected."""
        while self._next_expected in self._buffer:
            msg = self._buffer.pop(self._next_expected)
            self._deliver_and_advance(msg)
