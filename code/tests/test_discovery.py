"""
tests/test_discovery.py — Unit tests for C1: Peer Discovery
=============================================================
Run with:
    cd code/
    python tests/test_discovery.py
"""

import json
import logging
import os
import sys
import tempfile
import time

# Ensure the code/ package root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from config.base_config import BaseConfig, DiscoveryConfig, GossipConfig, HeartbeatConfig
from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType
from components.discovery import PeerCache, PeerDiscovery


# ────────────────────────────────────────────────────────── #
# Helpers                                                     #
# ────────────────────────────────────────────────────────── #

def _make_config(cache_path: str, seeds: list = None, fmin=2, fmax=6) -> BaseConfig:
    """Build a BaseConfig with a custom cache path and seed list."""
    return BaseConfig(
        heartbeat=HeartbeatConfig(delta_t=0.5),
        gossip=GossipConfig(fmin=fmin, fmax=fmax),
        discovery=DiscoveryConfig(
            peer_cache_file=cache_path,
            peer_cache_max=20,
            seed_nodes=seeds or ["127.0.0.1:5000"],
            discovery_timeout=1.0,
            peer_exchange_interval=60.0,
        ),
    )


def _make_node(host="127.0.0.1", port=7000, cache_path="peers.json", **kw) -> LocalNode:
    cfg = _make_config(cache_path, **kw)
    return LocalNode(host=host, port=port, config=cfg)


def _make_peer(port: int) -> NodeInfo:
    return NodeInfo.from_address("127.0.0.1", port)


# ────────────────────────────────────────────────────────── #
# Test 1: Cache save / load round-trip                        #
# ────────────────────────────────────────────────────────── #

def test_cache_save_load():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "peers.json")
        cache = PeerCache(path, max_peers=20)

        peers = [_make_peer(5000 + i) for i in range(15)]
        cache.save(peers)

        loaded = cache.load()
        assert len(loaded) == 15, f"Expected 15, got {len(loaded)}"

        for orig, loaded_p in zip(peers, loaded):
            assert orig.node_id == loaded_p.node_id
            assert orig.host == loaded_p.host
            assert orig.port == loaded_p.port

    print("[PASS] test_cache_save_load")


# ────────────────────────────────────────────────────────── #
# Test 2: Cache respects max limit                            #
# ────────────────────────────────────────────────────────── #

def test_cache_max_limit():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "peers.json")
        cache = PeerCache(path, max_peers=20)

        peers = [_make_peer(5000 + i) for i in range(30)]
        cache.save(peers)

        loaded = cache.load()
        assert len(loaded) == 20, f"Expected 20, got {len(loaded)}"

        # Should keep the LAST 20 (most recent)
        expected_ids = [p.node_id for p in peers[-20:]]
        loaded_ids = [p.node_id for p in loaded]
        assert loaded_ids == expected_ids

    print("[PASS] test_cache_max_limit")


# ────────────────────────────────────────────────────────── #
# Test 3: Join from cache (reachable peer)                    #
# ────────────────────────────────────────────────────────── #

def test_join_from_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "peers.json")

        # Pre-populate cache with a "reachable" peer
        reachable = _make_peer(8001)
        cache = PeerCache(cache_path, max_peers=20)
        cache.save([reachable])

        node = _make_node(port=7001, cache_path=cache_path, fmin=1, fmax=6)

        # Mock send_fn: peer 8001 is always alive
        sent_messages = []

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            sent_messages.append((peer, msg.msg_type))
            return True

        discovery = PeerDiscovery(node, send_fn)
        success = discovery.join_network()

        assert success, "join_network() should succeed when cache has a reachable peer"
        assert node.get_neighbour(reachable.node_id) is not None, \
            "Reachable peer should be in neighbour table"

        # Should have sent a HEARTBEAT (ping) and a PEER_LIST_REQUEST
        msg_types = [t for _, t in sent_messages]
        assert MessageType.HEARTBEAT in msg_types, "Should have pinged the peer"
        assert MessageType.PEER_LIST_REQUEST in msg_types, \
            "Should have requested peer list after connecting"

    print("[PASS] test_join_from_cache")


# ────────────────────────────────────────────────────────── #
# Test 4: Join falls back to seeds when cache empty           #
# ────────────────────────────────────────────────────────── #

def test_join_fallback_to_seeds():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "peers.json")
        # NO cache file — first boot

        node = _make_node(
            port=7002,
            cache_path=cache_path,
            seeds=["127.0.0.1:5000"],
            fmin=1,
            fmax=6,
        )

        contacted = []

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            contacted.append(peer.address)
            return True  # seed is alive

        discovery = PeerDiscovery(node, send_fn)
        success = discovery.join_network()

        assert success, "Should succeed via seed node"
        assert any("5000" in addr for addr in contacted), \
            "Should have contacted the seed node at port 5000"

    print("[PASS] test_join_fallback_to_seeds")


# ────────────────────────────────────────────────────────── #
# Test 5: Peer exchange flow                                  #
# ────────────────────────────────────────────────────────── #

def test_peer_exchange_flow():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "peers.json")
        node = _make_node(port=7003, cache_path=cache_path, fmin=1, fmax=10)

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            return True

        discovery = PeerDiscovery(node, send_fn)

        # Add one known neighbour first
        peer_a = _make_peer(8010)
        node.add_neighbour(peer_a)

        # Simulate receiving a PEER_LIST_RESPONSE with 3 new peers
        new_peers_data = [
            {"node_id": _make_peer(8011 + i).node_id,
             "host": "127.0.0.1",
             "port": 8011 + i}
            for i in range(3)
        ]
        response_msg = node.factory.peer_list_response(new_peers_data)
        discovery.handle_peer_list_response(response_msg)

        # Verify new peers were added
        for p_data in new_peers_data:
            entry = node.get_neighbour(p_data["node_id"])
            assert entry is not None, \
                f"Peer {p_data['node_id'][:8]} should be in neighbour table"

        # Verify cache was updated
        cached = PeerCache(cache_path).load()
        assert len(cached) == 3, f"Cache should have 3 peers, got {len(cached)}"

    print("[PASS] test_peer_exchange_flow")


# ────────────────────────────────────────────────────────── #
# Test 6: Re-discovery on dead node                           #
# ────────────────────────────────────────────────────────── #

def test_rediscovery_on_dead_node():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "peers.json")
        node = _make_node(port=7004, cache_path=cache_path, fmin=2, fmax=6)

        peer_list_requested = []

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            if msg.msg_type == MessageType.PEER_LIST_REQUEST:
                peer_list_requested.append(peer.node_id)
            return True

        discovery = PeerDiscovery(node, send_fn)

        # Start with 2 neighbours (at fmin)
        peer_a = _make_peer(8020)
        peer_b = _make_peer(8021)
        node.add_neighbour(peer_a)
        node.add_neighbour(peer_b)

        assert node.neighbour_count == 2

        # Kill one — now below fmin
        dead_peer = peer_a
        node.remove_neighbour(dead_peer.node_id)
        assert node.neighbour_count == 1  # below fmin=2

        # Trigger re-discovery
        discovery.trigger_rediscovery(dead_peer)

        # Should have asked the remaining alive neighbour for its peer list
        assert len(peer_list_requested) > 0, \
            "Should have requested peer list from remaining neighbour"

    print("[PASS] test_rediscovery_on_dead_node")


# ────────────────────────────────────────────────────────── #
# Test 7: fmax is respected during peer exchange              #
# ────────────────────────────────────────────────────────── #

def test_fmax_respected():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = os.path.join(tmpdir, "peers.json")
        node = _make_node(port=7005, cache_path=cache_path, fmin=1, fmax=3)

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            return True

        discovery = PeerDiscovery(node, send_fn)

        # Fill neighbour table to fmax
        for i in range(3):
            node.add_neighbour(_make_peer(9000 + i))

        assert node.neighbour_count == 3  # at fmax

        # Simulate peer exchange with 5 more peers
        new_peers_data = [
            {"node_id": _make_peer(9100 + i).node_id,
             "host": "127.0.0.1",
             "port": 9100 + i}
            for i in range(5)
        ]
        response_msg = node.factory.peer_list_response(new_peers_data)
        discovery.handle_peer_list_response(response_msg)

        # Neighbour count should NOT exceed fmax
        assert node.neighbour_count <= 3, \
            f"Neighbour count {node.neighbour_count} exceeds fmax=3"

    print("[PASS] test_fmax_respected")


# ────────────────────────────────────────────────────────── #
# Runner                                                      #
# ────────────────────────────────────────────────────────── #

def run_all():
    print("\n====== C1: Peer Discovery Tests ======")
    test_cache_save_load()
    test_cache_max_limit()
    test_join_from_cache()
    test_join_fallback_to_seeds()
    test_peer_exchange_flow()
    test_rediscovery_on_dead_node()
    test_fmax_respected()
    print("\n====== All 7 tests passed ======\n")


if __name__ == "__main__":
    run_all()
