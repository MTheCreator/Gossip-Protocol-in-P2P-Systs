"""
components package — high-level gossip overlay components.

Public exports:
  EdgeManager  — per-edge forwarding probability management (C4)
"""

from .edge_manager import EdgeManager

__all__ = ["EdgeManager"]
