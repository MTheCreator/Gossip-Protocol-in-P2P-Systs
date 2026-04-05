"""
run_demo_10_nodes.py  -  10-Node UDP Gossip Demo With Runtime Node Control
=========================================================================
Starts 10 nodes (ports 7001..7010) and lets you stop/restart any node
while the demo is running by typing commands with a port number.

Usage:
    cd code/
    pip install fastapi uvicorn
    python run_demo_10_nodes.py

Dashboard:
    http://localhost:9091

Runtime commands:
    stop <port>      Stop a running node by port (example: stop 7004)
    restart <port>   Restart a node by port (example: restart 7004)
    start <port>     Start a node if it is currently stopped
    status           Show node status for all demo ports
    help             Show command help
    quit             Stop all nodes and exit
"""

import logging
import os
import sys
import threading
import time
from typing import Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.base_config import (
    BaseConfig,
    BootstrapConfig,
    ChannelConfig,
    DiscoveryConfig,
    EdgeReliabilityConfig,
    GossipConfig,
    HeartbeatConfig,
    MonitorConfig,
    StorageConfig,
)
from config.udp_config import UDPConfig
from monitoring.monitor_server import MonitorServer
from transport.udp.server import UDPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("demo-10")

HOST = "127.0.0.1"
MONITOR_PORT = 9090
CHANNEL = "demo"
START_PORT = 7001
NODE_COUNT = 10
ALL_PORTS = list(range(START_PORT, START_PORT + NODE_COUNT))


class DemoCluster:
    def __init__(self) -> None:
        self._servers: Dict[int, UDPServer] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._publisher_thread: Optional[threading.Thread] = None
        self.monitor: Optional[MonitorServer] = None

    def make_config(self, port: int) -> UDPConfig:
        seed_addr = f"{HOST}:{START_PORT}"
        return UDPConfig(
            heartbeat=HeartbeatConfig(delta_t=1.0),
            gossip=GossipConfig(fmin=2, fmax=4, receipt_threshold=3),
            discovery=DiscoveryConfig(
                seed_nodes=[seed_addr],
                peer_cache_file=f"demo10_peers_{port}.json",
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
                db_path=f"demo10_store_{port}.db",
            ),
            monitor=MonitorConfig(
                enabled=True,
                monitor_address=f"{HOST}:{MONITOR_PORT}",
                report_interval=1.0,
            ),
        )

    def cleanup_files(self) -> None:
        for port in ALL_PORTS:
            for f in [
                f"demo10_peers_{port}.json",
                f"demo10_store_{port}.db",
                f"demo10_store_{port}.db-wal",
                f"demo10_store_{port}.db-shm",
            ]:
                try:
                    os.remove(f)
                except OSError:
                    pass

    def start_monitor(self) -> None:
        log.info("Starting monitor on :%d", MONITOR_PORT)
        self.monitor = MonitorServer(
            host=HOST,
            port=MONITOR_PORT,
            config=BaseConfig(transport="udp"),
        )
        self.monitor.start()
        time.sleep(2)

    def start_node(self, port: int) -> bool:
        with self._lock:
            if port in self._servers:
                log.info("Node :%d is already running", port)
                return False

            server = UDPServer(host=HOST, port=port, config=self.make_config(port))
            server.start()
            server.engine.subscribe_channel(
                CHANNEL,
                callback=lambda msg, p=port: log.info(
                    "  [:%d] DELIVER seq=%s", p, msg.metadata.get("seq_num", "?")
                ),
                request_history=True,
            )
            self._servers[port] = server

        log.info("Started node :%d", port)
        return True

    def stop_node(self, port: int) -> bool:
        with self._lock:
            server = self._servers.get(port)
            if server is None:
                log.info("Node :%d is not running", port)
                return False
            del self._servers[port]

        try:
            server.stop()
            log.info("Stopped node :%d", port)
            return True
        except Exception:
            log.exception("Failed while stopping node :%d", port)
            return False

    def restart_node(self, port: int) -> bool:
        self.stop_node(port)
        time.sleep(0.3)
        return self.start_node(port)

    def running_ports(self):
        with self._lock:
            return sorted(self._servers.keys())

    def print_status(self) -> None:
        running = set(self.running_ports())
        stopped = [p for p in ALL_PORTS if p not in running]
        print(f"Running nodes {len(running)}: { sorted(running)}")
        print(f"Stopped nodes {len(stopped)}: { stopped}")

    def _publisher_loop(self) -> None:
        msg_count = 0
        while not self._stop_event.is_set():
            with self._lock:
                candidate_ports = sorted(self._servers.keys())
                server = (
                    self._servers.get(candidate_ports[0]) if candidate_ports else None
                )

            if server is None:
                time.sleep(1.0)
                continue

            msg_count += 1
            try:
                server.engine.publish(
                    CHANNEL, {"n": msg_count, "data": f"msg-{msg_count}"}
                )
                log.info("Published #%d from :%d", msg_count, server.node.port)
            except Exception:
                log.exception("Publish failed for message #%d", msg_count)
            time.sleep(2.0)

    def start_publisher(self) -> None:
        self._publisher_thread = threading.Thread(
            target=self._publisher_loop, daemon=True
        )
        self._publisher_thread.start()

    def stop_all(self) -> None:
        self._stop_event.set()
        if self._publisher_thread is not None:
            self._publisher_thread.join(timeout=3.0)

        for port in sorted(self.running_ports(), reverse=True):
            self.stop_node(port)

        if self.monitor is not None:
            self.monitor.stop()


def parse_port(token: str) -> Optional[int]:
    try:
        port = int(token)
    except ValueError:
        return None
    if port not in ALL_PORTS:
        return None
    return port


def print_help() -> None:
    print("\nCommands:")
    print("  stop <port>      Stop a running node")
    print("  restart <port>   Restart a node")
    print("  start <port>     Start a stopped node")
    print("  status           Show running/stopped nodes")
    print("  help             Show this help")
    print("  quit             Exit demo")


def main() -> None:
    log.info("=" * 60)
    log.info("  MONITOR SERVER ")
    log.info("=" * 60)

    cluster = DemoCluster()
    cluster.cleanup_files()
    cluster.start_monitor()
    while True:
        continue


if __name__ == "__main__":
    main()
