"""
tests/test_dissemination.py -- Unit tests for C3a: Dissemination Engine
=========================================================================
Run with:
    cd code/
    python tests/test_dissemination.py
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

from config.base_config import BaseConfig, GossipConfig, HeartbeatConfig, DiscoveryConfig
from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType
from components.dissemination import DisseminationEngine
from components.edge_manager import EdgeManager


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def _make_config(fmin=1, fmax=6, receipt_threshold=3):
    return BaseConfig(
        heartbeat=HeartbeatConfig(delta_t=0.5),
        gossip=GossipConfig(fmin=fmin, fmax=fmax, receipt_threshold=receipt_threshold),
        discovery=DiscoveryConfig(peer_cache_file="test_peers.json"),
    )


def _make_node(port=7000, **kw):
    return LocalNode(host="127.0.0.1", port=port, config=_make_config(**kw))


def _make_peer(port):
    return NodeInfo.from_address("127.0.0.1", port)


# ------------------------------------------------------------------ #
# Test 1: Publish delivers locally and forwards                       #
# ------------------------------------------------------------------ #

def test_publish_local_delivery():
    node = _make_node(port=7100)
    node.subscribe("sensors")

    sent_to = []
    def send_fn(peer, msg):
        sent_to.append(peer.node_id)
        return True

    engine = DisseminationEngine(node, send_fn)

    delivered = []
    engine.on_message("sensors", lambda msg: delivered.append(msg.content))

    # Add a neighbour so forwarding has a target
    peer = _make_peer(8100)
    node.add_neighbour(peer)

    msg = engine.publish("sensors", {"temp": 22.5})

    assert len(delivered) == 1, f"Expected 1 local delivery, got {len(delivered)}"
    assert delivered[0] == {"temp": 22.5}
    assert peer.node_id in sent_to, "Message should be forwarded to neighbour"

    print("[PASS] test_publish_local_delivery")


# ------------------------------------------------------------------ #
# Test 2: Deduplication -- same message not delivered twice            #
# ------------------------------------------------------------------ #

def test_deduplication():
    node = _make_node(port=7101)
    node.subscribe("alerts")

    def send_fn(peer, msg):
        return True

    engine = DisseminationEngine(node, send_fn)

    delivered = []
    engine.on_message("alerts", lambda msg: delivered.append(msg.msg_id))

    # Publish a message (self-delivers once)
    msg = engine.publish("alerts", "fire!")

    # Simulate receiving the same message back (echo)
    engine.receive(msg, sender_info=_make_peer(9999))

    assert len(delivered) == 1, f"Expected 1 delivery (dedup), got {len(delivered)}"

    print("[PASS] test_deduplication")


# ------------------------------------------------------------------ #
# Test 3: Receipt threshold stops re-forwarding                      #
# ------------------------------------------------------------------ #

def test_receipt_threshold():
    node = _make_node(port=7102, receipt_threshold=2)

    forward_count = [0]
    def send_fn(peer, msg):
        forward_count[0] += 1
        return True

    engine = DisseminationEngine(node, send_fn)

    # Add 3 neighbours
    for i in range(3):
        node.add_neighbour(_make_peer(8200 + i))

    # Publish (counts as forward #1)
    msg = engine.publish("data", "payload")
    first_forwards = forward_count[0]

    # Receive the same message again from external (forward #2)
    engine.receive(msg, sender_info=_make_peer(9000))
    second_forwards = forward_count[0]

    # Receive again (forward #3 — should be blocked by threshold=2)
    engine.receive(msg, sender_info=_make_peer(9001))
    third_forwards = forward_count[0]

    assert third_forwards == second_forwards, \
        f"Should not forward beyond threshold; forwards={third_forwards}"

    print("[PASS] test_receipt_threshold")


# ------------------------------------------------------------------ #
# Test 4: Edge probability gating                                     #
# ------------------------------------------------------------------ #

def test_edge_probability_gating():
    node = _make_node(port=7103)

    sent_to = []
    def send_fn(peer, msg):
        sent_to.append(peer.node_id)
        return True

    engine = DisseminationEngine(node, send_fn)

    # Add a neighbour with probability 0.0 (should NEVER forward)
    peer_blocked = _make_peer(8300)
    node.add_neighbour(peer_blocked, edge_prob=0.0)

    # Add a neighbour with probability 1.0 (should ALWAYS forward)
    peer_allowed = _make_peer(8301)
    node.add_neighbour(peer_allowed, edge_prob=1.0)

    engine.publish("test", "hello")

    assert peer_allowed.node_id in sent_to, "Peer with prob=1.0 should receive"
    assert peer_blocked.node_id not in sent_to, "Peer with prob=0.0 should NOT receive"

    print("[PASS] test_edge_probability_gating")


# ------------------------------------------------------------------ #
# Test 5: Edge reinforcement on successful forward                    #
# ------------------------------------------------------------------ #

def test_edge_reinforcement():
    node = _make_node(port=7104)

    send_log = []
    def send_fn(peer, msg):
        send_log.append(peer.node_id)
        return True

    em = EdgeManager(node=node, lambda_ema=0.8)
    engine = DisseminationEngine(node, send_fn, edge_manager=em)

    peer = _make_peer(8400)
    # Set edge_prob=1.0 so forwarding always happens (no random gating)
    node.add_neighbour(peer, edge_prob=1.0)
    em.add_edge(peer.node_id, initial_prob=1.0)

    engine.publish("data", "test-reinforcement")

    # Verify the message was actually sent
    assert peer.node_id in send_log, "Message should have been sent to peer"

    # Test EMA reinforcement directly: start at 0.3, reinforce (success=True)
    # EMA: 0.8 * 0.3 + 0.2 * 1.0 = 0.44
    em2 = EdgeManager(node=node, lambda_ema=0.8)
    peer2 = _make_peer(8401)
    node.add_neighbour(peer2, edge_prob=0.3)
    em2.add_edge(peer2.node_id, initial_prob=0.3)
    em2.reinforce(peer2.node_id)
    new_prob = em2.get_probability(peer2.node_id)
    expected = 0.8 * 0.3 + 0.2 * 1.0  # 0.44
    assert new_prob is not None and abs(new_prob - expected) < 0.001, \
        f"Edge should be reinforced to {expected}, got {new_prob}"

    print("[PASS] test_edge_reinforcement")


# ------------------------------------------------------------------ #
# Test 6: Unsubscribed channel -- no local delivery                   #
# ------------------------------------------------------------------ #

def test_unsubscribed_no_delivery():
    node = _make_node(port=7105)
    # NOT subscribed to "private"

    def send_fn(peer, msg):
        return True

    engine = DisseminationEngine(node, send_fn)

    delivered = []
    engine.on_message("private", lambda msg: delivered.append(msg))

    # Simulate receiving a gossip message on "private"
    peer = _make_peer(8500)
    msg = node.factory.gossip("private", "secret-data")
    engine.receive(msg, sender_info=peer)

    assert len(delivered) == 0, "Should NOT deliver to unsubscribed channel"

    print("[PASS] test_unsubscribed_no_delivery")


# ------------------------------------------------------------------ #
# Runner                                                              #
# ------------------------------------------------------------------ #

def run_all():
    print("\n====== C3a: Dissemination Engine Tests ======")
    test_publish_local_delivery()
    test_deduplication()
    test_receipt_threshold()
    test_edge_probability_gating()
    test_edge_reinforcement()
    test_unsubscribed_no_delivery()
    print("\n====== All 6 tests passed ======\n")


if __name__ == "__main__":
    run_all()
