"""
simulation.py  —  Integration test: Heartbeat + EdgeManager (code/ framework)
==============================================================================
Elghali Benjelloun — Gossip Protocol Project

This script is the counterpart to src/simulation_stub.py, but uses the full
code/ framework (LocalNode, HeartbeatManager, EdgeManager, BaseConfig) instead
of the standalone src/ classes.

Scenario
--------
  node-A  monitors two neighbours: node-B (active) and node-C (silent/crashed).

  HeartbeatManager (failure detection):
    - Sends PING to both; node-B replies, node-C never does
    - node-C is marked SUSPECTED after 3 * delta_t, then DEAD after 6 * delta_t

  EdgeManager (forwarding probability):
    - Simulates a forwarding engine that reinforces edge A→B every 1.5 * delta_t
    - Edge A→C gets no reinforcement → decays to 0 → on_edge_died fires

Run with:
    cd code/
    python simulation.py
"""

import logging
import sys
import time
import threading

# Ensure the code/ package root is on the path
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.base_config import BaseConfig, HeartbeatConfig, GossipConfig
from core.node import LocalNode, NodeInfo
from core.heartbeat import HeartbeatManager
from core.message import Message, MessageType
from components.edge_manager import EdgeManager

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Simulation parameters ─────────────────────────────────────────────────────
DELTA_T   = 0.5    # heartbeat interval (fast for demo)
SIM_TIME  = (6 * DELTA_T) + 3   # long enough to see the DEAD transition

def run_simulation() -> None:
    log.info("=" * 60)
    log.info("   Gossip Protocol — Integrated Simulation (code/ framework)")
    log.info(f"   ∆t={DELTA_T}s  T_suspect={3*DELTA_T}s  T_dead={6*DELTA_T}s")
    log.info("=" * 60)

    events: list = []   # record state transitions for assertions

    # ── Shared config ──────────────────────────────────────────────────
    cfg = BaseConfig(
        heartbeat=HeartbeatConfig(
            delta_t=DELTA_T,
            tsuspect_multiplier=3,
            tdead_multiplier=6,
        ),
        gossip=GossipConfig(
            fmin=1,
            fmax=6,
            backoff_initial=1.0,
            backoff_max=60.0,
            backoff_multiplier=2.0,
        ),
    )

    # ── Build node-A ───────────────────────────────────────────────────
    node_a = LocalNode(host="127.0.0.1", port=7001, config=cfg)

    # Peer identity objects for B and C
    peer_b = NodeInfo.from_address("127.0.0.1", 7002)
    peer_c = NodeInfo.from_address("127.0.0.1", 7003)

    node_a.add_neighbour(peer_b, edge_prob=0.5)
    node_a.add_neighbour(peer_c, edge_prob=0.5)

    # ── Callbacks ──────────────────────────────────────────────────────
    def on_node_dead(peer: NodeInfo) -> None:
        log.error("  💀 CALLBACK: %s is DEAD → trigger C1 re-discovery!", peer.node_id[:8])
        events.append(("dead", peer.node_id))
        # A dead node means its edge is also gone.  Since LocalNode.mark_dead()
        # removes the NeighbourEntry immediately, EdgeManager's decay loop
        # will never see it again — so we fire on_edge_died here explicitly.
        log.error("  🔌 CALLBACK: Edge → %s DIED (peer declared dead)", peer.node_id[:8])
        events.append(("edge_died", peer.node_id))
        hb.on_edge_failure(peer.node_id)   # → BackOffManager.start_backoff()

    def on_edge_died(node_id: str) -> None:
        log.error("  🔌 CALLBACK: Edge → %s DIED → back-off + re-discovery", node_id[:8])
        events.append(("edge_died", node_id))
        hb.on_edge_failure(node_id)   # notify HeartbeatManager → BackOff

    def on_edge_revived(node_id: str) -> None:
        log.info("  ⚡ CALLBACK: Edge → %s REVIVED", node_id[:8])
        events.append(("edge_revived", node_id))
        hb.on_edge_recovery(node_id)  # notify HeartbeatManager → reset BackOff

    # ── No-op send function (no real socket here) ──────────────────────
    def send_fn(peer: NodeInfo, msg: Message) -> None:
        log.debug("  SEND %s → %s", msg.msg_type.name, peer.node_id[:8])

    # ── Build HeartbeatManager ─────────────────────────────────────────
    hb = HeartbeatManager(
        node=node_a,
        send_fn=send_fn,
        on_node_dead=on_node_dead,
    )

    # ── Build EdgeManager ──────────────────────────────────────────────
    em = EdgeManager(
        node=node_a,
        reinforce_step=0.08,
        decay_factor=0.90,      # aggressive decay so we see effect quickly
        dead_threshold=0.05,
        enable_noise=False,
        on_edge_died=on_edge_died,
        on_edge_revived=on_edge_revived,
    )
    # Set initial probabilities (node already in neighbour table)
    em.add_edge(peer_b.node_id, initial_prob=0.5)
    em.add_edge(peer_c.node_id, initial_prob=0.5)

    # Cross-wire (optional — for back-off calls from EdgeManager to HeartbeatManager)
    hb.set_edge_manager(em)

    # ── Start both managers ────────────────────────────────────────────
    hb.start()
    em.start()

    # ── Simulate node-B sending heartbeats (stays alive) ──────────────
    def node_b_heartbeat_loop() -> None:
        while True:
            msg = node_a.factory.heartbeat()
            # Override sender_id to look like it came from B
            object.__setattr__(msg, "sender_id", peer_b.node_id)  # type: ignore[arg-type]
            node_a.record_heartbeat(peer_b.node_id)
            time.sleep(DELTA_T)

    b_thread = threading.Thread(target=node_b_heartbeat_loop, daemon=True)
    b_thread.start()

    # ── Simulate forwarding engine reinforcing edge A→B ────────────────
    def forward_engine_sim() -> None:
        while True:
            em.reinforce(peer_b.node_id)   # B is active
            # node-C gets no reinforcement → will decay and die
            time.sleep(DELTA_T * 1.5)

    fwd_thread = threading.Thread(target=forward_engine_sim, daemon=True)
    fwd_thread.start()

    log.info("  [node-C crashes immediately — no heartbeats will arrive]")
    log.info(f"\n  Running for {SIM_TIME:.1f}s …\n")
    time.sleep(SIM_TIME)

    # ── Final state ────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("  FINAL NEIGHBOUR STATES (node-A perspective)")
    for entry in node_a.all_neighbours():
        log.info("    %s: %s", entry.info.node_id[:8], entry.status.value)

    log.info("\n  FINAL EDGE PROBABILITIES")
    for nid, prob in em.all_edges().items():
        log.info("    %s: p = %.3f", nid[:8], prob)

    log.info("\n  EVENT LOG")
    for ev in events:
        log.info("    %s", ev)

    # ── Assertions ────────────────────────────────────────────────────
    log.info("\n  RUNNING ASSERTIONS …")

    event_types = [e[0] for e in events]
    event_ids   = [e[1] for e in events]

    assert "dead" in event_types, "FAIL: node-C should have been declared DEAD"
    assert peer_c.node_id in event_ids, f"FAIL: dead event should name node-C ({peer_c.node_id[:8]})"
    assert "edge_died" in event_types, "FAIL: edge A→C should have died"

    p_b = em.get_probability(peer_b.node_id)
    assert p_b is not None and p_b > 0.5, \
        f"FAIL: edge A→B should be reinforced above 0.5, got {p_b}"

    log.info("  ✅ ALL ASSERTIONS PASSED\n")

    hb.stop()
    em.stop()


if __name__ == "__main__":
    run_simulation()
