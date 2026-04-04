"""
components package -- high-level gossip overlay components.

Public exports:
  EdgeManager            -- per-edge forwarding probability management (C4)
  PeerDiscovery          -- peer cache, join, peer exchange, re-discovery (C1)
  DisseminationEngine    -- push-based gossip forwarding engine (C3a)
  MessageStore           -- SQLite disk persistence
  ChannelSequencer       -- per-channel total-order sequencer
  OrderedDeliveryBuffer  -- seq_num-ordered delivery buffer
  StatusReporter         -- periodic status reporting to monitor
"""

from .edge_manager import EdgeManager
from .discovery import PeerDiscovery
from .dissemination import DisseminationEngine
from .storage import MessageStore
from .sequencer import ChannelSequencer
from .ordered_delivery import OrderedDeliveryBuffer
from .status_reporter import StatusReporter
from .core_manager import CoreNodeManager
from .election import ElectionManager

__all__ = [
    "EdgeManager",
    "PeerDiscovery",
    "DisseminationEngine",
    "MessageStore",
    "ChannelSequencer",
    "OrderedDeliveryBuffer",
    "StatusReporter",
    "CoreNodeManager",
    "ElectionManager",
]
