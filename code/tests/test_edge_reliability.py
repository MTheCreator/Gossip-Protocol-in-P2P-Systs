"""
tests/test_edge_reliability.py -- Unit tests for EMA-based reliability scoring
================================================================================
Run with:
    cd code/
    python tests/test_edge_reliability.py
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from config.base_config import BaseConfig, GossipConfig, HeartbeatConfig, EdgeReliabilityConfig
from core.node import LocalNode, NodeInfo
from components.edge_manager import EdgeManager


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def _make_config(**rel_kw):
    return BaseConfig(
        heartbeat=HeartbeatConfig(delta_t=0.5),
        gossip=GossipConfig(fmin=1, fmax=6),
        edge_reliability=EdgeReliabilityConfig(**rel_kw),
    )


def _make_node(port=7000, **rel_kw):
    return LocalNode(host="127.0.0.1", port=port, config=_make_config(**rel_kw))


def _make_peer(port):
    return NodeInfo.from_address("127.0.0.1", port)


# ------------------------------------------------------------------ #
# Test 1: EMA formula correctness on success                          #
# ------------------------------------------------------------------ #

def test_ema_success():
    """score = λ * old + (1-λ) * 1.0"""
    lam = 0.8
    node = _make_node(port=7200, lambda_ema=lam, initial_score=0.5)
    peer = _make_peer(8200)
    node.add_neighbour(peer, edge_prob=0.5)

    em = EdgeManager(node=node, lambda_ema=lam, initial_score=0.5)
    em.add_edge(peer.node_id, initial_prob=0.5)

    # First success: 0.8 * 0.5 + 0.2 * 1.0 = 0.6
    em.record_outcome(peer.node_id, success=True)
    entry = node.get_neighbour(peer.node_id)
    expected = lam * 0.5 + (1 - lam) * 1.0  # 0.6
    assert abs(entry.reliability_score - expected) < 1e-9, \
        f"Expected {expected}, got {entry.reliability_score}"
    assert abs(entry.edge_prob - expected) < 1e-9, \
        "edge_prob should stay in sync with reliability_score"

    # Second success: 0.8 * 0.6 + 0.2 * 1.0 = 0.68
    em.record_outcome(peer.node_id, success=True)
    expected2 = lam * expected + (1 - lam) * 1.0  # 0.68
    assert abs(entry.reliability_score - expected2) < 1e-9, \
        f"Expected {expected2}, got {entry.reliability_score}"

    print("[PASS] test_ema_success")


# ------------------------------------------------------------------ #
# Test 2: EMA formula correctness on failure                          #
# ------------------------------------------------------------------ #

def test_ema_failure():
    """score = λ * old + (1-λ) * 0.0"""
    lam = 0.8
    node = _make_node(port=7201, lambda_ema=lam, initial_score=1.0)
    peer = _make_peer(8201)
    node.add_neighbour(peer, edge_prob=1.0)

    em = EdgeManager(node=node, lambda_ema=lam, initial_score=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    # First failure: 0.8 * 1.0 + 0.2 * 0.0 = 0.8
    em.record_outcome(peer.node_id, success=False)
    entry = node.get_neighbour(peer.node_id)
    expected = lam * 1.0  # 0.8
    assert abs(entry.reliability_score - expected) < 1e-9, \
        f"Expected {expected}, got {entry.reliability_score}"

    # Second failure: 0.8 * 0.8 = 0.64
    em.record_outcome(peer.node_id, success=False)
    expected2 = lam * expected
    assert abs(entry.reliability_score - expected2) < 1e-9, \
        f"Expected {expected2}, got {entry.reliability_score}"

    print("[PASS] test_ema_failure")


# ------------------------------------------------------------------ #
# Test 3: Edge death detection via repeated failures                  #
# ------------------------------------------------------------------ #

def test_edge_death():
    lam = 0.5  # fast-reacting
    dead_thresh = 0.1
    node = _make_node(port=7202, lambda_ema=lam, dead_threshold=dead_thresh, initial_score=0.5)
    peer = _make_peer(8202)
    node.add_neighbour(peer, edge_prob=0.5)

    died_events = []
    em = EdgeManager(
        node=node,
        lambda_ema=lam,
        dead_threshold=dead_thresh,
        initial_score=0.5,
        on_edge_died=lambda nid: died_events.append(nid),
    )
    em.add_edge(peer.node_id, initial_prob=0.5)

    # Repeatedly fail until score < 0.1
    # 0.5 → 0.25 → 0.125 → 0.0625 (< 0.1! death)
    for _ in range(10):
        em.record_outcome(peer.node_id, success=False)

    assert len(died_events) == 1, f"Edge should die exactly once, got {len(died_events)}"
    assert died_events[0] == peer.node_id

    print("[PASS] test_edge_death")


# ------------------------------------------------------------------ #
# Test 4: Edge revival after death                                    #
# ------------------------------------------------------------------ #

def test_edge_revival():
    lam = 0.5
    node = _make_node(port=7203, lambda_ema=lam, dead_threshold=0.1,
                      revival_threshold=0.3, initial_score=0.5)
    peer = _make_peer(8203)
    node.add_neighbour(peer, edge_prob=0.5)

    died = []
    revived = []
    em = EdgeManager(
        node=node,
        lambda_ema=lam,
        dead_threshold=0.1,
        revival_threshold=0.3,
        initial_score=0.5,
        on_edge_died=lambda nid: died.append(nid),
        on_edge_revived=lambda nid: revived.append(nid),
    )
    em.add_edge(peer.node_id, initial_prob=0.5)

    # Drive to death
    for _ in range(10):
        em.record_outcome(peer.node_id, success=False)
    assert len(died) == 1, "Edge should die"

    # Now revive with successes
    for _ in range(10):
        em.record_outcome(peer.node_id, success=True)
    assert len(revived) == 1, f"Edge should revive, got {len(revived)}"

    print("[PASS] test_edge_revival")


# ------------------------------------------------------------------ #
# Test 5: edge_prob stays in sync with reliability_score               #
# ------------------------------------------------------------------ #

def test_sync():
    lam = 0.7
    node = _make_node(port=7204, lambda_ema=lam, initial_score=0.5)
    peer = _make_peer(8204)
    node.add_neighbour(peer, edge_prob=0.5)

    em = EdgeManager(node=node, lambda_ema=lam, initial_score=0.5)
    em.add_edge(peer.node_id, initial_prob=0.5)

    # Mix of successes and failures
    for success in [True, False, True, True, False, False, True]:
        em.record_outcome(peer.node_id, success=success)
        entry = node.get_neighbour(peer.node_id)
        assert abs(entry.edge_prob - entry.reliability_score) < 1e-9, \
            f"edge_prob ({entry.edge_prob}) ≠ reliability_score ({entry.reliability_score})"

    print("[PASS] test_sync")


# ------------------------------------------------------------------ #
# Test 6: reinforce() and penalise() convenience aliases              #
# ------------------------------------------------------------------ #

def test_aliases():
    lam = 0.8
    node = _make_node(port=7205, lambda_ema=lam, initial_score=0.5)
    peer = _make_peer(8205)
    node.add_neighbour(peer, edge_prob=0.5)

    em = EdgeManager(node=node, lambda_ema=lam, initial_score=0.5)
    em.add_edge(peer.node_id, initial_prob=0.5)

    em.reinforce(peer.node_id)
    entry = node.get_neighbour(peer.node_id)
    expected = lam * 0.5 + (1 - lam) * 1.0  # 0.6
    assert abs(entry.reliability_score - expected) < 1e-9

    em.penalise(peer.node_id)
    expected2 = lam * expected + (1 - lam) * 0.0  # 0.48
    assert abs(entry.reliability_score - expected2) < 1e-9

    print("[PASS] test_aliases")


# ------------------------------------------------------------------ #
# Runner                                                              #
# ------------------------------------------------------------------ #

def run_all():
    print("\n====== EMA Edge Reliability Tests ======")
    test_ema_success()
    test_ema_failure()
    test_edge_death()
    test_edge_revival()
    test_sync()
    test_aliases()
    print("\n====== All 6 tests passed ======\n")


if __name__ == "__main__":
    run_all()
