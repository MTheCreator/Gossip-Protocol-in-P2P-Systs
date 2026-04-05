"""
run_demo.py  —  Simple 3-Node Gossip Demo
============================================
Minimal demo: 1 seed (sequencer) + 2 regular nodes, 1 channel.

Usage:
    cd code/
    pip install fastapi uvicorn
    python run_demo.py

Dashboard: http://localhost:9091
"""

import logging
import os
import sys
import time

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
    format="%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("demo")

HOST         = "127.0.0.1"
SEED_PORT    = 7001
MONITOR_PORT = 9090
CHANNEL      = "demo"


def make_config(port: int) -> UDPConfig:
    seed_addr = f"{HOST}:{SEED_PORT}"
    return UDPConfig(
        heartbeat=HeartbeatConfig(delta_t=1.0),
        gossip=GossipConfig(fmin=2, fmax=4, receipt_threshold=3),
        discovery=DiscoveryConfig(
            seed_nodes=[seed_addr],
            peer_cache_file=f"demo_peers_{port}.json",
            peer_exchange_interval=10.0,
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
                sequencer_heartbeat_interval=0.5,
                sequencer_heartbeat_timeout=2.0,
                election_timeout=3.0,
                core_join_min_seq=3,
            ),
        ],
        bootstrap=BootstrapConfig(
            bootstrap_wait=5.0,
            broadcast_attempts=5,
            broadcast_interval=1.0,
        ),
        storage=StorageConfig(
            enabled=True,
            db_path=f"demo_store_{port}.db",
        ),
        monitor=MonitorConfig(
            enabled=True,
            monitor_address=f"{HOST}:{MONITOR_PORT}",
            report_interval=1.0,
        ),
    )


def main():
    log.info("=" * 50)
    log.info("  Simple 3-Node Demo")
    log.info("=" * 50)

    # Clean leftover files from previous runs
    for port in [7001, 7002, 7003]:
        for f in [
            f"demo_peers_{port}.json",
            f"demo_store_{port}.db",
            f"demo_store_{port}.db-wal",
            f"demo_store_{port}.db-shm",
        ]:
            try:
                os.remove(f)
            except OSError:
                pass

    servers = {}

    # ── Monitor ───────────────────────────────────────────────
    log.info("Starting monitor on :%d", MONITOR_PORT)
    monitor = MonitorServer(
        host=HOST, port=MONITOR_PORT,
        config=BaseConfig(transport="udp"),
    )
    monitor.start()
    time.sleep(5)

    # ── Node 1: Seed (will self-elect as sequencer) ───────────
    log.info("")
    log.info("--- Starting SEED node on :%d ---", SEED_PORT)
    s1 = UDPServer(host=HOST, port=SEED_PORT, config=make_config(SEED_PORT))
    s1.start()
    servers[SEED_PORT] = s1

    # Subscribe seed to channel
    s1.engine.subscribe_channel(
        CHANNEL,
        callback=lambda msg, p=SEED_PORT: log.info(
            "  [:%d] DELIVER seq=%s", p, msg.metadata.get("seq_num", "?")
        ),
        request_history=True,
    )

    # Print seed's sequencer status
    em1 = s1._election_managers.get(CHANNEL)
    log.info("  Seed sequencer_address = %s", em1.sequencer_address if em1 else "N/A")
    log.info("  Seed is_sequencer = %s", em1.is_sequencer if em1 else "N/A")

    # Give seed time to stabilize
    log.info("  Waiting 3s for seed to stabilize...")
    time.sleep(20)

    # ── Node 2 ────────────────────────────────────────────────
    log.info("")
    log.info("--- Starting node 2 on :7002 ---")
    s2 = UDPServer(host=HOST, port=7002, config=make_config(7002))
    s2.start()
    servers[7002] = s2

    s2.engine.subscribe_channel(
        CHANNEL,
        callback=lambda msg, p=7002: log.info(
            "  [:%d] DELIVER seq=%s", p, msg.metadata.get("seq_num", "?")
        ),
        request_history=True,
    )

    em2 = s2._election_managers.get(CHANNEL)
    log.info("  Node2 sequencer_address = %s", em2.sequencer_address if em2 else "N/A")
    log.info("  Node2 is_sequencer = %s", em2.is_sequencer if em2 else "N/A")
    log.info("  Node2 neighbours = %d", s2.node.neighbour_count)

    time.sleep(10)

    # ── Node 3 ────────────────────────────────────────────────
    log.info("")
    log.info("--- Starting node 3 on :7003 ---")
    s3 = UDPServer(host=HOST, port=7003, config=make_config(7003))
    s3.start()
    servers[7003] = s3

    s3.engine.subscribe_channel(
        CHANNEL,
        callback=lambda msg, p=7003: log.info(
            "  [:%d] DELIVER seq=%s", p, msg.metadata.get("seq_num", "?")
        ),
        request_history=True,
    )

    em3 = s3._election_managers.get(CHANNEL)
    log.info("  Node3 sequencer_address = %s", em3.sequencer_address if em3 else "N/A")
    log.info("  Node3 is_sequencer = %s", em3.is_sequencer if em3 else "N/A")
    log.info("  Node3 neighbours = %d", s3.node.neighbour_count)

    time.sleep(10)

    # ── Summary ───────────────────────────────────────────────
    log.info("")
    log.info("=" * 50)
    log.info("  CLUSTER STATE")
    log.info("=" * 50)
    for port, srv in sorted(servers.items()):
        em = srv._election_managers.get(CHANNEL)
        cm = srv._core_managers.get(CHANNEL)
        log.info(
            "  :%d  seq_addr=%s  is_seq=%s  is_core=%s  neighbours=%d",
            port,
            em.sequencer_address if em else "?",
            em.is_sequencer if em else "?",
            cm.is_core if cm else "?",
            srv.node.neighbour_count,
        )
    log.info("=" * 50)

    # ── Publish loop ──────────────────────────────────────────
    log.info("")
    log.info("Publishing every 2s. Dashboard at http://localhost:9091")
    log.info("Press Ctrl+C to stop.")


    msg_count = 0
    try:
        while True:
            if msg_count == 30:
                s1.stop()
                msg_count += 1
            if msg_count < 30:
                msg_count += 1
                # Publish from seed
                s1.engine.publish(CHANNEL, {"n": msg_count, "data": f"msg-{msg_count}"})
                log.info("Published #%d", msg_count)
                time.sleep(1)
    except KeyboardInterrupt:
        log.info("\nShutting down...")
    


    for port in sorted(servers.keys(), reverse=True):
        servers[port].stop()
    monitor.stop()

    for port in [7001, 7002, 7003]:
        for f in [
            f"demo_peers_{port}.json",
            f"demo_store_{port}.db",
            f"demo_store_{port}.db-wal",
            f"demo_store_{port}.db-shm",
        ]:
            try:
                os.remove(f)
            except OSError:
                pass

    log.info("Done.")


if __name__ == "__main__":
    main()
