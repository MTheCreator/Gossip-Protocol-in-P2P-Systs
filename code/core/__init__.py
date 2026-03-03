"""
core package — node identity, messaging, and heartbeat/failure detection.

Public exports:
  LocalNode        — the running node (identity + neighbour table)
  NodeInfo         — lightweight peer identity record
  NeighbourEntry   — per-edge state
  Message          — canonical gossip/control message
  MessageType      — message kind enum
  MessageFactory   — stamped message factory
  LamportClock     — thread-safe Lamport clock
  NodeStatus       — ALIVE / SUSPECT / DEAD enum
  HeartbeatManager — wires HeartbeatSender, HeartbeatReceiver, FailureDetector
"""

from .message import LamportClock, Message, MessageFactory, MessageType, NodeStatus
from .node import LocalNode, NeighbourEntry, NodeInfo
from .heartbeat import HeartbeatManager

__all__ = [
    "LamportClock",
    "Message",
    "MessageFactory",
    "MessageType",
    "NodeStatus",
    "LocalNode",
    "NeighbourEntry",
    "NodeInfo",
    "HeartbeatManager",
]
