"""
monitoring package -- network-wide status aggregation and REST API.

Public exports:
  MonitorServer -- standalone monitoring node
"""

from .monitor_server import MonitorServer

__all__ = ["MonitorServer"]
