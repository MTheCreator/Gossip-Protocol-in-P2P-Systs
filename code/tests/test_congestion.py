"""
tests/test_congestion.py -- Unit tests for C3b+C4: 3-Variable Edge Probability
================================================================================
Run with:
    cd code/
    python tests/test_congestion.py

Tests the interaction between HeartbeatManager (congestion_score) and
EdgeManager (record_outcome with weighted reliability + congestion).
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from config.base_config import BaseConfig, HeartbeatConfig, GossipConfig, DiscoveryConfig, EdgeReliabilityConfig
from core.node import LocalNode, NodeInfo
from core.message import MessageType

# Import directly from module to avoid circular imports in components/__init__
import components.edge_manager as _em_mod
EdgeManager = _em_mod.EdgeManager

import core.heartbeat as _hb_mod
HeartbeatManager = _hb_mod.HeartbeatManager


def _make_config():
    return BaseConfig(
        heartbeat=HeartbeatConfig(delta_t=0.5),
        gossip=GossipConfig(fmin=1, fmax=6),
        discovery=DiscoveryConfig(peer_cache_file="test_peers.json"),
    )


# ------------------------------------------------------------------ #
# Test 1: Success + clear link → probability stays at 1.0            #
# ------------------------------------------------------------------ #

def test_success_clear_link():
    """When delivery succeeds and there's no congestion, prob should stay 1.0"""
    print("\n--- test_success_clear_link ---")
    node = LocalNode(host="127.0.0.1", port=7300, config=_make_config())

    # EdgeManager with no congestion fn → defaults to 1.0 (clear)
    em = EdgeManager(node=node, lambda_ema=0.8, w_rel=0.7, w_cong=0.3)

    peer = NodeInfo.from_address("127.0.0.1", 8300)
    node.add_neighbour(peer, edge_prob=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    # 3 successful deliveries with clear congestion
    em.reinforce(peer.node_id)
    em.reinforce(peer.node_id)
    em.reinforce(peer.node_id)

    p = em.get_probability(peer.node_id)
    # combined = 0.7*1.0 + 0.3*1.0 = 1.0 → EMA stays at 1.0
    assert abs(p - 1.0) < 0.001, f"Expected 1.0, got {p:.3f}"
    print(f"  p = {p:.3f} (expected 1.000)")
    print("[PASS] test_success_clear_link\n")


# ------------------------------------------------------------------ #
# Test 2: Success + congested link → prob STILL drops!               #
# ------------------------------------------------------------------ #

def test_success_but_congested():
    """Even successful delivery should lower prob if the link is congested."""
    print("--- test_success_but_congested ---")
    node = LocalNode(host="127.0.0.1", port=7301, config=_make_config())

    # Mock congestion: always return 0.2 (heavy congestion)
    em = EdgeManager(
        node=node, lambda_ema=0.8, w_rel=0.7, w_cong=0.3,
        get_congestion_fn=lambda nid: 0.2,
    )

    peer = NodeInfo.from_address("127.0.0.1", 8301)
    node.add_neighbour(peer, edge_prob=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    # Deliver successfully, but link is congested
    em.reinforce(peer.node_id)
    p1 = em.get_probability(peer.node_id)
    # combined = 0.7*1.0 + 0.3*0.2 = 0.76
    # new = 0.8*1.0 + 0.2*0.76 = 0.952
    expected1 = 0.8 * 1.0 + 0.2 * (0.7 * 1.0 + 0.3 * 0.2)
    print(f"  After 1 success+congestion: p = {p1:.3f} (expected {expected1:.3f})")
    assert abs(p1 - expected1) < 0.001, f"Expected {expected1:.3f}, got {p1:.3f}"

    em.reinforce(peer.node_id)
    p2 = em.get_probability(peer.node_id)
    expected2 = 0.8 * expected1 + 0.2 * 0.76
    print(f"  After 2 success+congestion: p = {p2:.3f} (expected {expected2:.3f})")
    assert p2 < 1.0, "Prob should be below 1.0 due to congestion"

    print("[PASS] test_success_but_congested\n")


# ------------------------------------------------------------------ #
# Test 3: Failure + congestion → prob drops fast                     #
# ------------------------------------------------------------------ #

def test_failure_plus_congestion():
    """Delivery failure on a congested link should drop prob aggressively."""
    print("--- test_failure_plus_congestion ---")
    node = LocalNode(host="127.0.0.1", port=7302, config=_make_config())

    em = EdgeManager(
        node=node, lambda_ema=0.8, w_rel=0.7, w_cong=0.3,
        get_congestion_fn=lambda nid: 0.1,  # severe congestion
    )

    peer = NodeInfo.from_address("127.0.0.1", 8302)
    node.add_neighbour(peer, edge_prob=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    # Failure + severe congestion
    em.penalise(peer.node_id)
    p = em.get_probability(peer.node_id)
    # combined = 0.7*0.0 + 0.3*0.1 = 0.03
    # new = 0.8*1.0 + 0.2*0.03 = 0.806
    expected = 0.8 * 1.0 + 0.2 * (0.7 * 0.0 + 0.3 * 0.1)
    print(f"  After failure+congestion: p = {p:.3f} (expected {expected:.3f})")
    assert abs(p - expected) < 0.001, f"Expected {expected:.3f}, got {p:.3f}"

    print("[PASS] test_failure_plus_congestion\n")


# ------------------------------------------------------------------ #
# Test 4: End-to-end with HeartbeatManager RTT measurement           #
# ------------------------------------------------------------------ #

def test_end_to_end_rtt_congestion():
    """Full integration: HeartbeatManager measures RTT, EdgeManager reads it."""
    print("--- test_end_to_end_rtt_congestion ---")
    node = LocalNode(host="127.0.0.1", port=7303, config=_make_config())

    def mock_send(peer_info, msg):
        pass

    hbm = HeartbeatManager(node, mock_send)

    # Wire EdgeManager with hbm.get_congestion as the data source
    em = EdgeManager(
        node=node, lambda_ema=0.8, w_rel=0.7, w_cong=0.3,
        get_congestion_fn=hbm.get_congestion,
    )
    hbm.set_edge_manager(em)

    peer = NodeInfo.from_address("127.0.0.1", 8303)
    node.add_neighbour(peer, edge_prob=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    # Step 1: establish baseline RTT (fast)
    hb1 = node.factory.heartbeat()
    hbm._wrapped_send_fn(peer, hb1)
    time.sleep(0.02)  # 20ms
    ack1 = node.factory.heartbeat_ack(hb1)
    ack1.sender_id = peer.node_id
    hbm.handle_incoming(ack1, peer)

    cong1 = hbm.get_congestion(peer.node_id)
    print(f"  After fast ACK: congestion = {cong1:.3f} (should be 1.0)")
    assert cong1 == 1.0  # first measurement = baseline

    # Successful delivery with clear congestion → stays at 1.0
    em.reinforce(peer.node_id)
    p1 = em.get_probability(peer.node_id)
    print(f"  After reinforce (clear): p = {p1:.3f}")
    assert abs(p1 - 1.0) < 0.001

    # Step 2: simulate a SLOW ACK (congestion!)
    hb2 = node.factory.heartbeat()
    hbm._wrapped_send_fn(peer, hb2)
    time.sleep(0.3)   # 300ms >> 1.5 * 20ms baseline
    ack2 = node.factory.heartbeat_ack(hb2)
    ack2.sender_id = peer.node_id
    hbm.handle_incoming(ack2, peer)

    cong2 = hbm.get_congestion(peer.node_id)
    print(f"  After slow ACK: congestion = {cong2:.3f} (should be < 0.2)")
    assert cong2 < 0.2, f"Expected < 0.2, got {cong2:.3f}"

    # Successful delivery BUT congested → prob should drop
    em.reinforce(peer.node_id)
    p2 = em.get_probability(peer.node_id)
    print(f"  After reinforce (congested): p = {p2:.3f}")
    assert p2 < 1.0, "Prob must drop even on success when congested!"

    # Step 3: simulate fast ACK (recovery)
    hb3 = node.factory.heartbeat()
    hbm._wrapped_send_fn(peer, hb3)
    time.sleep(0.02)
    ack3 = node.factory.heartbeat_ack(hb3)
    ack3.sender_id = peer.node_id
    hbm.handle_incoming(ack3, peer)

    cong3 = hbm.get_congestion(peer.node_id)
    print(f"  After fast ACK (recovery): congestion = {cong3:.3f}")
    assert cong3 > 0.5, "Congestion score should recover"

    print("[PASS] test_end_to_end_rtt_congestion\n")


# ------------------------------------------------------------------ #
# Runner                                                              #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    print("\n====== C3b+C4: 3-Variable Edge Probability Tests ======")
    test_success_clear_link()
    test_success_but_congested()
    test_failure_plus_congestion()
    test_end_to_end_rtt_congestion()
    print("====== All 4 tests passed ======\n")
