"""
monitoring/api.py  —  REST API for the Gossip Protocol Dashboard
=================================================================
Minimal FastAPI app that exposes the MonitorServer's aggregated data.

Endpoints:
  GET /api/topology              — full network graph (nodes + edges)
  GET /api/traffic               — recent directed send events for edge animations
  GET /api/nodes                 — list all known nodes
  GET /api/nodes/{node_id}       — detail for one node
  GET /api/channels              — all channels with metrics
  GET /api/channels/{name}/messages — per-node delivery status for a channel
  GET /api/channels/{name}/detail   — full channel detail with roles and messages
  GET /api/nodes/{node_id}/messages/{channel} — per-node delivered messages

The dashboard static files are served from the /dashboard directory.

Dependencies: pip install fastapi uvicorn
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

if TYPE_CHECKING:
    from monitoring.monitor_server import MonitorServer


def create_app(monitor: "MonitorServer") -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(
        title="Gossip Protocol Monitor",
        description="Real-time monitoring dashboard for the gossip overlay network",
        version="1.0.0",
    )

    # CORS — allow the dashboard (any origin for dev)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------
    # API endpoints
    # ------------------------------------------------------------------

    @app.get("/api/topology")
    def topology():
        """Full network topology: nodes + edges with probabilities."""
        return monitor.get_topology()

    @app.get("/api/traffic")
    def traffic(since_id: int = 0):
        """Recent edge traffic events (heartbeat + gossip) since optional event id."""
        return monitor.get_traffic(since_id=since_id)

    @app.get("/api/nodes")
    def nodes():
        """List all known gossip nodes with their latest status."""
        return monitor.get_all_nodes()

    @app.get("/api/nodes/{node_id}")
    def node_detail(node_id: str):
        """Detailed status for a specific node."""
        detail = monitor.get_node_detail(node_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="Node not found")
        return detail

    @app.get("/api/channels")
    def channels():
        """All channels with subscriber counts and delivery metrics."""
        return monitor.get_channels()

    @app.get("/api/channels/{name}/messages")
    def channel_messages(name: str):
        """Per-node message delivery status for a channel."""
        return monitor.get_channel_messages(name)

    @app.get("/api/channels/{name}/detail")
    def channel_detail(name: str):
        """Full channel detail: subscribers with roles, messages, gaps."""
        return monitor.get_channel_detail(name)

    @app.get("/api/nodes/{node_id}/messages/{channel}")
    def node_messages(node_id: str, channel: str):
        """Delivered messages for a specific node and channel."""
        return monitor.get_node_messages(node_id, channel)

    # ------------------------------------------------------------------
    # Serve dashboard static files
    # ------------------------------------------------------------------

    dashboard_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dashboard",
    )

    if os.path.isdir(dashboard_dir):
        @app.get("/")
        def serve_index():
            return FileResponse(os.path.join(dashboard_dir, "index.html"))

        app.mount(
            "/static",
            StaticFiles(directory=os.path.join(dashboard_dir, "static")),
            name="static",
        )

    return app
