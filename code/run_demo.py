"""
run_demo.py  —  Full Cluster Demo with Monitoring Dashboard
=============================================================
Starts a small gossip cluster and opens the monitoring dashboard.

Topology:
  - 1 seed node  (port 6001, self-elects as sequencer for "demo" channel)
  - 4 regular nodes (ports 6002–6005)
  - 1 monitor server (port 9090, REST API on 9091)

All nodes subscribe to the "demo" channel.  The sequencer and core nodes
are discovered dynamically at bootstrap (no hardcoded addresses).
One node publishes a message every 2 seconds.

Usage:
    cd code/
    pip install fastapi uvicorn   # only needed once
    python run_demo.py

Then open: http://localhost:9091
"""

import logging
import os
import sys
import time
import threading
import webbrowser

# Ensure code/ is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.base_config import (
    BaseConfig, HeartbeatConfig, GossipConfig, DiscoveryConfig,
    EdgeReliabilityConfig, StorageConfig, MonitorConfig, ChannelConfig,
    BootstrapConfig,
)
from config.udp_config import UDPConfig
from transport.udp.server import UDPServer
from monitoring.monitor_server import MonitorServer

# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("demo")

# ── Configuration ─────────────────────────────────────────────────────

SEED_PORT     = 6001
NODE_PORTS    = [6002, 6003, 6004, 6005]
MONITOR_PORT  = 9090
HOST          = "127.0.0.1"
CHANNEL       = "demo"


def make_config(port: int) -> UDPConfig:
    """Create a UDPConfig for a node at the given port."""
    seed_addr = f"{HOST}:{SEED_PORT}"
    return UDPConfig(
        heartbeat=HeartbeatConfig(delta_t=1.0),
        gossip=GossipConfig(fmin=2, fmax=6, receipt_threshold=3),
        discovery=DiscoveryConfig(
            seed_nodes=[seed_addr],
            peer_cache_file=f"peers_{port}.json",
            peer_exchange_interval=15.0,
        ),
        edge_reliability=EdgeReliabilityConfig(
            lambda_ema=0.8,
            dead_threshold=0.1,
            revival_threshold=0.2,
        ),
        channels=[
            ChannelConfig(
                channel_name=CHANNEL,
                history_batch_size=50,
            ),
        ],
        bootstrap=BootstrapConfig(
            bootstrap_wait=3.0,
            broadcast_attempts=3,
            broadcast_interval=1.0,
        ),
        storage=StorageConfig(
            enabled=True,
            db_path=f"gossip_store_{port}.db",
        ),
        monitor=MonitorConfig(
            enabled=True,
            monitor_address=f"{HOST}:{MONITOR_PORT}",
            report_interval=3.0,
        ),
    )


def main():
    log.info("=" * 60)
    log.info("  Gossip Protocol — Full Cluster Demo")
    log.info("=" * 60)

    servers = []

    # ── Start Monitor ─────────────────────────────────────────
    log.info("Starting monitor on %s:%d ...", HOST, MONITOR_PORT)
    monitor_cfg = BaseConfig(transport="udp")
    monitor = MonitorServer(host=HOST, port=MONITOR_PORT, config=monitor_cfg)
    monitor.start()

    # ── Start Seed Node ───────────────────────────────────────
    log.info("Starting seed node on %s:%d ...", HOST, SEED_PORT)
    seed_cfg = make_config(SEED_PORT)
    seed_server = UDPServer(host=HOST, port=SEED_PORT, config=seed_cfg)
    seed_server.start()
    servers.append(seed_server)

    time.sleep(1)

    # ── Start Regular Nodes ───────────────────────────────────
    for port in NODE_PORTS:
        log.info("Starting node on %s:%d ...", HOST, port)
        cfg = make_config(port)
        server = UDPServer(host=HOST, port=port, config=cfg)
        server.start()
        servers.append(server)
        time.sleep(0.5)

    # ── Subscribe all nodes to the demo channel ───────────────
    log.info("Subscribing all nodes to '%s' channel ...", CHANNEL)
    for server in servers:
        server.engine.subscribe_channel(
            CHANNEL,
            callback=lambda msg, s=server: log.info(
                "  [%s] Received: seq=%s content=%s",
                s.node.address,
                msg.metadata.get("seq_num", "?"),
                msg.content,
            ),
            request_history=True,
        )

    time.sleep(2)

    # ── Open dashboard ────────────────────────────────────────
    dashboard_url = f"http://{HOST}:{MONITOR_PORT + 1}"
    log.info("Opening dashboard at %s", dashboard_url)
    try:
        webbrowser.open(dashboard_url)
    except Exception:
        log.info("Could not open browser. Navigate to: %s", dashboard_url)

    # ── Publish messages periodically ─────────────────────────
    log.info("Publishing messages every 2 seconds. Press Ctrl+C to stop.\n")

    msg_count = 0
    try:
        while True:
            msg_count += 1
            content = {"msg_num": msg_count, "data": f"Hello #{msg_count}"}
            # Publish from the seed node (which is also the sequencer)
            seed_server.engine.publish(CHANNEL, content)
            log.info("Published message #%d", msg_count)
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("\nShutting down ...")

    # ── Cleanup ───────────────────────────────────────────────
    for server in reversed(servers):
        server.stop()
    monitor.stop()

    # Clean up temp files
    for port in [SEED_PORT] + NODE_PORTS:
        for f in [f"peers_{port}.json", f"gossip_store_{port}.db"]:
            try:
                os.remove(f)
            except OSError:
                pass

    log.info("Demo stopped.")


if __name__ == "__main__":
    main()
