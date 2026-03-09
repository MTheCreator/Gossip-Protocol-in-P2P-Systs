"""
simulation.py  —  Integration test: Heartbeat + EMA EdgeManager (code/ framework)
==================================================================================
Elghali Benjelloun — Gossip Protocol Project

Scenario
--------
  node-A  monitors two neighbours: node-B (active) and node-C (silent/crashed).

  HeartbeatManager (failure detection):
    - Sends PING to both; node-B replies, node-C never does
    - node-C is marked SUSPECTED after 3 * delta_t, then DEAD after 6 * delta_t

  EdgeManager (EMA reliability scoring):
    - Simulates message sends: B succeeds, C always fails
    - Edge A→B reliability stays high (reinforced by successes)
    - Edge A→C reliability drops to 0 (penalised by failures) → on_edge_died fires

  EMA formula:
    reliability_score = λ * old_score + (1 - λ) * outcome
    where outcome = 1.0 (success) or 0.0 (failure)

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

from config.base_config import BaseConfig, HeartbeatConfig, GossipConfig, EdgeReliabilityConfig
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
LAMBDA    = 0.7    # EMA smoothing (lower = reacts faster = better for short sim)
SIM_TIME  = (6 * DELTA_T) + 3   # long enough to see the DEAD transition

def run_simulation() -> None:
    log.info("=" * 60)
    log.info("   Gossip Protocol — EMA Reliability Simulation")
    log.info(f"   ∆t={DELTA_T}s  λ={LAMBDA}  T_suspect={3*DELTA_T}s  T_dead={6*DELTA_T}s")
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
        edge_reliability=EdgeReliabilityConfig(
            lambda_ema=LAMBDA,
            dead_threshold=0.1,
            revival_threshold=0.2,
            initial_score=0.5,
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
        hb.on_edge_failure(peer.node_id)

    def on_edge_died(node_id: str) -> None:
        log.error("  🔌 CALLBACK: Edge → %s DIED (reliability too low)", node_id[:8])
        events.append(("edge_died", node_id))
        hb.on_edge_failure(node_id)

    def on_edge_revived(node_id: str) -> None:
        log.info("  ⚡ CALLBACK: Edge → %s REVIVED", node_id[:8])
        events.append(("edge_revived", node_id))
        hb.on_edge_recovery(node_id)

    # ── No-op send function (no real socket here) ──────────────────────
    def send_fn(peer: NodeInfo, msg: Message) -> None:
        log.debug("  SEND %s → %s", msg.msg_type.name, peer.node_id[:8])

    # ── Build HeartbeatManager ─────────────────────────────────────────
    hb = HeartbeatManager(
        node=node_a,
        send_fn=send_fn,
        on_node_dead=on_node_dead,
    )

    # ── Build EdgeManager (EMA-based) ──────────────────────────────────
    em = EdgeManager(
        node=node_a,
        lambda_ema=LAMBDA,
        dead_threshold=0.1,
        revival_threshold=0.2,
        initial_score=0.5,
        on_edge_died=on_edge_died,
        on_edge_revived=on_edge_revived,
    )
    em.add_edge(peer_b.node_id, initial_prob=0.5)
    em.add_edge(peer_c.node_id, initial_prob=0.5)

    # Cross-wire
    hb.set_edge_manager(em)

    # ── Start both managers ────────────────────────────────────────────
    hb.start()
    em.start()

    # ── Simulate node-B sending heartbeats (stays alive) ──────────────
    def node_b_heartbeat_loop() -> None:
        while True:
            node_a.record_heartbeat(peer_b.node_id)
            time.sleep(DELTA_T)

    b_thread = threading.Thread(target=node_b_heartbeat_loop, daemon=True)
    b_thread.start()

    # ── Simulate forwarding engine: B succeeds, C fails ────────────────
    def forward_engine_sim() -> None:
        while True:
            # B: successful delivery → reinforce
            em.record_outcome(peer_b.node_id, success=True)

            # C: failed delivery → penalise
            entry_c = node_a.get_neighbour(peer_c.node_id)
            if entry_c:
                em.record_outcome(peer_c.node_id, success=False)

            time.sleep(DELTA_T * 0.5)

    fwd_thread = threading.Thread(target=forward_engine_sim, daemon=True)
    fwd_thread.start()

    log.info("  [node-C crashes immediately — no heartbeats will arrive]")
    log.info(f"\n  Running for {SIM_TIME:.1f}s …\n")
    time.sleep(SIM_TIME)

    # ── Final state ────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("  FINAL NEIGHBOUR STATES (node-A perspective)")
    for entry in node_a.all_neighbours():
        log.info("    %s: status=%s, reliability=%.3f, edge_prob=%.3f",
                 entry.info.node_id[:8], entry.status.value,
                 entry.reliability_score, entry.edge_prob)

    log.info("\n  FINAL EDGE PROBABILITIES (via EdgeManager)")
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
    assert "edge_died" in event_types, "FAIL: edge A→C should have died (reliability < threshold)"

    p_b = em.get_probability(peer_b.node_id)
    assert p_b is not None and p_b > 0.5, \
        f"FAIL: edge A→B should be reinforced above 0.5, got {p_b}"

    log.info("  ✅ ALL ASSERTIONS PASSED\n")

    hb.stop()
    em.stop()


if __name__ == "__main__":
    run_simulation()
