"""
components package -- high-level gossip overlay components.

Public exports:
  EdgeManager         -- per-edge forwarding probability management (C4)
  PeerDiscovery       -- peer cache, join, peer exchange, re-discovery (C1)
  DisseminationEngine -- push-based gossip forwarding engine (C3a)
"""

from .edge_manager import EdgeManager
from .discovery import PeerDiscovery
from .dissemination import DisseminationEngine

__all__ = ["EdgeManager", "PeerDiscovery", "DisseminationEngine"]
