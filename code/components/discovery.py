"""
components/discovery.py  —  C1: Peer Discovery
=================================================
Oussama Laaroussi — Gossip Protocol Project

Implements a priority-based peer discovery strategy:

  1. Local Peer Cache (peers.json)
     The node first tries the last N peers it was talking to before it went
     offline.  If the network is stable, at least one will likely still be alive.

  2. Hardcoded Seeds
     If (and only if) the local cache fails or is empty (first boot), the node
     reaches out to hardcoded seed addresses.

  3. Peer Exchange
     Once connected to *any* node, the node immediately requests a peer list.
     It then overwrites its local cache with this fresh data.

Integration:
  - HeartbeatManager.on_node_dead → trigger_rediscovery()
  - EdgeManager.on_edge_died     → trigger_rediscovery()
  - Transport layer injects send_fn for actual message delivery.
"""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from core.node import LocalNode, NeighbourEntry, NodeInfo
from core.message import Message, MessageType
from config.base_config import BaseConfig

logger = logging.getLogger(__name__)

# Type alias: callable that sends a message to a peer and returns True on success
SendFn = Callable[[NodeInfo, Message], bool]


# ---------------------------------------------------------------------------
# Peer Cache — persistent storage of known peers
# ---------------------------------------------------------------------------

class PeerCache:
    """
    Read/write a JSON file containing the last known peers.

    File format (peers.json):
    [
        {"node_id": "abc...", "host": "10.0.0.2", "port": 7001},
        ...
    ]
    """

    def __init__(self, path: str, max_peers: int = 20) -> None:
        self.path = Path(path)
        self.max_peers = max_peers

    def load(self) -> List[NodeInfo]:
        """Load cached peers from disk. Returns [] if file missing or corrupt."""
        if not self.path.exists():
            logger.debug("[PeerCache] No cache file at %s", self.path)
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            peers = [
                NodeInfo(node_id=p["node_id"], host=p["host"], port=p["port"])
                for p in data
                if "node_id" in p and "host" in p and "port" in p
            ]
            logger.info("[PeerCache] Loaded %d peers from %s", len(peers), self.path)
            return peers
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            logger.warning("[PeerCache] Corrupt cache file: %s", exc)
            return []

    def save(self, peers: List[NodeInfo]) -> None:
        """Save the most recent peers to disk (keeps at most max_peers)."""
        trimmed = peers[-self.max_peers:]
        data = [
            {"node_id": p.node_id, "host": p.host, "port": p.port}
            for p in trimmed
        ]
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info("[PeerCache] Saved %d peers to %s", len(data), self.path)
        except OSError as exc:
            logger.error("[PeerCache] Failed to write cache: %s", exc)

    def clear(self) -> None:
        """Remove the cache file."""
        if self.path.exists():
            self.path.unlink()


# ---------------------------------------------------------------------------
# PeerDiscovery
# ---------------------------------------------------------------------------

class PeerDiscovery:
    """
    Manages node discovery for the gossip overlay.

    Lifecycle:
        discovery = PeerDiscovery(local_node, send_fn)
        success = discovery.join_network()       # called once at startup
        discovery.start()                        # optional background refresh
        ...
        discovery.shutdown()                     # saves cache to disk

    The HeartbeatManager wires `on_node_dead` to `trigger_rediscovery()` so
    that when neighbours fall below fmin the node automatically seeks new peers.
    """

    def __init__(
        self,
        node: LocalNode,
        send_fn: SendFn,
        on_peers_updated: Optional[Callable[[], None]] = None,
    ) -> None:
        self.node = node
        self.send_fn = send_fn
        self.on_peers_updated = on_peers_updated or (lambda: None)

        cfg = node.config.discovery
        self.cache = PeerCache(cfg.peer_cache_file, cfg.peer_cache_max)
        self.seed_nodes: List[str] = list(cfg.seed_nodes)
        self.timeout = cfg.discovery_timeout
        self.exchange_interval = cfg.peer_exchange_interval

        # Pending peer-list responses (thread-safe)
        self._pending_response: Optional[List[Dict[str, Any]]] = None
        self._response_event = threading.Event()

        # Background refresh thread
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        logger.info(
            "[Discovery] Initialised: cache=%s, seeds=%s",
            cfg.peer_cache_file, self.seed_nodes,
        )

    # ------------------------------------------------------------------
    # Join network (called at startup)
    # ------------------------------------------------------------------

    def join_network(self) -> bool:
        """
        Attempt to join the overlay.  Unlike the previous implementation that
        stopped after a single successful contact, this version tries ALL
        reachable peers until fmin <= neighbour_count <= fmax.

        Priority:
          1. Cached peers (from peers.json)
          2. Hardcoded seed nodes
          3. Peer exchange with any connected neighbour

        Returns True if at least one peer was successfully contacted.
        """
        logger.info("[Discovery] === Joining network ===")

        # Step 1: Try all cached peers — add as many as possible
        cached = self.cache.load()
        if cached:
            logger.info("[Discovery] Step 1: Trying %d cached peers", len(cached))
            added = self._try_connect_all(cached)
            logger.info("[Discovery] Added %d peers from cache", added)

        # Step 2: If still below fmin, try seed nodes
        if self.node.needs_more_neighbours():
            seed_peers = self._seeds_to_node_infos()
            if seed_peers:
                logger.info("[Discovery] Step 2: Trying %d seed nodes", len(seed_peers))
                added = self._try_connect_all(seed_peers)
                logger.info("[Discovery] Added %d peers from seeds", added)

        # Step 3: If connected to anyone but still below fmin, do peer exchange
        if self.node.neighbour_count > 0 and self.node.needs_more_neighbours():
            logger.info("[Discovery] Step 3: Peer exchange to reach fmin")
            self._peer_exchange_and_connect()

        if self.node.neighbour_count > 0:
            logger.info(
                "[Discovery] Joined network with %d neighbours (fmin=%d)",
                self.node.neighbour_count, self.node.config.gossip.fmin,
            )
            return True

        logger.error("[Discovery] Failed to join — no reachable peers or seeds")
        return False

    def _try_connect_all(self, candidates: List[NodeInfo]) -> int:
        """
        Try to connect to ALL candidates.  Adds every reachable peer as a
        neighbour (up to fmax).  Returns the number of newly added peers.
        """
        added = 0
        is_udp = self.node.config.transport == "udp"
        for peer in candidates:
            if peer.node_id == self.node.node_id:
                continue  # don't connect to ourselves

            if self.node.neighbour_count >= self.node.config.gossip.fmax:
                logger.debug("[Discovery] fmax reached, stopping connection attempts")
                break

            if self._has_neighbour(peer):
                continue  # already a neighbour

            logger.debug("[Discovery] Trying peer %s (%s)", peer.node_id[:8], peer.address)

            # Send a PING to check liveness. For UDP, send success alone does
            # not prove the peer exists; require a heartbeat/ACK handshake.
            ping = self.node.factory.create(MessageType.HEARTBEAT)
            try:
                alive = self.send_fn(peer, ping)
            except Exception as exc:
                logger.debug("[Discovery] Ping to %s failed: %s", peer.address, exc)
                continue

            if not alive:
                logger.debug("[Discovery] Peer %s did not respond", peer.address)
                continue

            if is_udp:
                if self._await_handshake(peer):
                    added += 1
                    logger.info(
                        "[Discovery] Connected to %s (%s) [%d/%d]",
                        peer.node_id[:8], peer.address,
                        self.node.neighbour_count, self.node.config.gossip.fmax,
                    )
                else:
                    logger.debug(
                        "[Discovery] Handshake timeout for %s (%s)",
                        peer.node_id[:8], peer.address,
                    )
            else:
                # gRPC send failures are explicit, so optimistic add is safe.
                if self.node.add_neighbour(peer):
                    added += 1
                    logger.info(
                        "[Discovery] Connected to %s (%s) [%d/%d]",
                        peer.node_id[:8], peer.address,
                        self.node.neighbour_count, self.node.config.gossip.fmax,
                    )

        # Request peer exchange from all newly connected peers
        if added > 0:
            for entry in self.node.alive_neighbours():
                self._request_peer_list(entry.info)

        return added

    def _peer_exchange_and_connect(self) -> None:
        """
        Request peer lists from all current neighbours and attempt to connect
        to the results.  Used as a third-tier strategy when cache and seeds
        are insufficient to reach fmin.
        """
        for entry in self.node.alive_neighbours():
            self._request_peer_list(entry.info)

    def _seeds_to_node_infos(self) -> List[NodeInfo]:
        """Convert seed address strings ("host:port") to NodeInfo objects."""
        results = []
        for addr in self.seed_nodes:
            try:
                host, port_str = addr.rsplit(":", 1)
                port = int(port_str)
                results.append(NodeInfo.from_address(host, port))
            except (ValueError, AttributeError) as exc:
                logger.warning("[Discovery] Invalid seed address '%s': %s", addr, exc)
        return results

    def _has_neighbour(self, peer: NodeInfo) -> bool:
        if self.node.get_neighbour(peer.node_id) is not None:
            return True
        for entry in self.node.all_neighbours():
            if entry.info.host == peer.host and entry.info.port == peer.port:
                return True
        return False

    def _await_handshake(self, peer: NodeInfo) -> bool:
        timeout = max(0.3, min(self.timeout, 3.0))
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._has_neighbour(peer):
                return True
            time.sleep(0.02)
        return self._has_neighbour(peer)

    def _connect_candidates_async(self, candidates: List[NodeInfo]) -> None:
        """Connect to peers in a background thread to avoid blocking recv loop."""
        if not candidates:
            return

        def _worker() -> None:
            try:
                added = self._try_connect_all(candidates)
                if added > 0:
                    self.on_peers_updated()
            except Exception as exc:
                logger.warning("[Discovery] Async connect failed: %s", exc)

        threading.Thread(
            target=_worker,
            name=f"PeerConnect-{self.node.node_id[:8]}",
            daemon=True,
        ).start()

    # ------------------------------------------------------------------
    # Peer Exchange
    # ------------------------------------------------------------------

    def _request_peer_list(self, peer: NodeInfo) -> None:
        """Send a PEER_LIST_REQUEST to a connected peer."""
        msg = self.node.factory.peer_list_request()
        try:
            self.send_fn(peer, msg)
            logger.info("[Discovery] Sent PEER_LIST_REQUEST to %s", peer.node_id[:8])
        except Exception as exc:
            logger.warning("[Discovery] Failed to request peer list: %s", exc)

    def handle_peer_list_request(
        self, msg: Message, sender_info: NodeInfo
    ) -> None:
        """
        Handle an incoming PEER_LIST_REQUEST.
        Reply with our current neighbour list.
        """
        self.node.clock.receive(msg.lamport_ts)

        peers_data = [
            {"node_id": e.info.node_id, "host": e.info.host, "port": e.info.port}
            for e in self.node.alive_neighbours()
        ]
        # Include ourselves so the requester knows about us
        peers_data.append({
            "node_id": self.node.node_id,
            "host": self.node.info.host,
            "port": self.node.info.port,
        })

        response = self.node.factory.peer_list_response(peers_data)
        try:
            self.send_fn(sender_info, response)
            logger.info(
                "[Discovery] Sent PEER_LIST_RESPONSE (%d peers) to %s",
                len(peers_data), sender_info.node_id[:8],
            )
        except Exception as exc:
            logger.warning("[Discovery] Failed to send peer list response: %s", exc)

    def handle_peer_list_response(self, msg: Message) -> None:
        """
        Handle an incoming PEER_LIST_RESPONSE.
        Add new peers to our neighbour table (respecting fmax) and refresh cache.
        """
        self.node.clock.receive(msg.lamport_ts)

        if not isinstance(msg.content, list):
            logger.warning("[Discovery] Invalid PEER_LIST_RESPONSE content")
            return

        new_count = 0
        queued_count = 0
        all_peers: List[NodeInfo] = []
        udp_candidates: List[NodeInfo] = []
        is_udp = self.node.config.transport == "udp"

        for p in msg.content:
            try:
                peer = NodeInfo(
                    node_id=p["node_id"], host=p["host"], port=int(p["port"])
                )
            except (KeyError, ValueError, TypeError):
                continue

            all_peers.append(peer)

            if peer.node_id == self.node.node_id:
                continue  # skip self

            if self._has_neighbour(peer):
                continue  # already known

            if is_udp:
                udp_candidates.append(peer)
                queued_count += 1
            elif self.node.add_neighbour(peer):
                new_count += 1
                logger.info("[Discovery] Added new peer %s via exchange", peer.node_id[:8])

        # Refresh the local cache with fresh data
        self.cache.save(all_peers)

        if is_udp and udp_candidates:
            # In UDP, validate via heartbeat handshake before adding.
            self._connect_candidates_async(udp_candidates)

        logger.info(
            "[Discovery] Peer exchange complete: %d new peers added, %d queued for validation, "
            "cache updated with %d entries",
            new_count, queued_count, len(all_peers),
        )
        self.on_peers_updated()

    # ------------------------------------------------------------------
    # Re-discovery (called by HeartbeatManager / EdgeManager)
    # ------------------------------------------------------------------

    def trigger_rediscovery(self, dead_peer: Optional[NodeInfo] = None) -> None:
        """
        Called when a neighbour is declared dead or an edge dies.
        If neighbour count < fmin, attempt to find new peers.

        Strategy:
          1. Ask ALL alive neighbours for their peer lists.
          2. If no alive neighbours remain, fall back to a full join_network().
        """
        if dead_peer:
            logger.info(
                "[Discovery] Re-discovery triggered (dead peer: %s)",
                dead_peer.node_id[:8],
            )

        if not self.node.needs_more_neighbours():
            logger.debug(
                "[Discovery] Neighbour count %d >= fmin=%d, no action needed",
                self.node.neighbour_count, self.node.config.gossip.fmin,
            )
            return

        logger.warning(
            "[Discovery] Neighbour count %d < fmin=%d — seeking new peers",
            self.node.neighbour_count, self.node.config.gossip.fmin,
        )

        # Strategy 1: Ask ALL alive neighbours for their peer lists
        alive = self.node.alive_neighbours()
        if alive:
            for entry in alive:
                self._request_peer_list(entry.info)
            return

        # Strategy 2: No alive neighbours — fall back to full join
        self.join_network()

    # ------------------------------------------------------------------
    # Background refresh (optional)
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background peer-exchange refresh loop."""
        if self._refresh_thread is not None:
            return
        self._stop_event.clear()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name=f"PeerRefresh-{self.node.node_id[:8]}",
            daemon=True,
        )
        self._refresh_thread.start()
        logger.info(
            "[Discovery] Background refresh started (interval=%.0fs)",
            self.exchange_interval,
        )

    def _refresh_loop(self) -> None:
        """Periodically ask a random neighbour for their peer list."""
        while not self._stop_event.wait(self.exchange_interval):
            alive = self.node.alive_neighbours()
            if alive:
                target = random.choice(alive)
                self._request_peer_list(target.info)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Stop background threads and save the current peer cache."""
        self._stop_event.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=2.0)
            self._refresh_thread = None

        # Persist current neighbours to cache for next startup
        current_peers = [e.info for e in self.node.all_neighbours()]
        if current_peers:
            self.cache.save(current_peers)
            logger.info("[Discovery] Saved %d peers to cache on shutdown", len(current_peers))

        logger.info("[Discovery] Shut down")
