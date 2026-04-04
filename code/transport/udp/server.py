"""
transport/udp/server.py  --  UDP Node Server
===============================================
Wires together all components into a single runnable gossip node
using UDP as the transport backend.

Now supports dynamic sequencer election and core node management:
  - On startup, each channel bootstraps by discovering or self-electing
    a sequencer (no more hardcoded sequencer_address in config).
  - Core nodes are tracked dynamically via CoreNodeManager.
  - ElectionManager handles sequencer failure detection and election.

Usage:
    from config.udp_config import get_udp_config
    from transport.udp.server import UDPServer

    server = UDPServer(host="0.0.0.0", port=6001, config=get_udp_config())
    server.start()
    server.engine.publish("sensors", {"temp": 22.5})
    ...
    server.stop()
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Optional

from config.udp_config import UDPConfig, get_udp_config
from config.base_config import ChannelConfig
from core.node import LocalNode, NodeInfo
from core.message import Message, MessageType
from core.heartbeat import HeartbeatManager
from components.discovery import PeerDiscovery
from components.dissemination import DisseminationEngine
from components.edge_manager import EdgeManager
from components.storage import MessageStore
from components.sequencer import ChannelSequencer
from components.core_manager import CoreNodeManager
from components.election import ElectionManager
from transport.udp.transport import UDPTransport

logger = logging.getLogger(__name__)


class UDPServer:
    """
    Top-level node server using UDP transport.

    Creates and wires:
      - LocalNode           (identity + neighbour table)
      - UDPTransport        (send/recv over UDP)
      - HeartbeatManager    (failure detection)
      - EdgeManager         (forwarding probabilities)
      - PeerDiscovery       (join + re-discovery)
      - DisseminationEngine (gossip forwarding + total order)
      - MessageStore        (disk persistence, optional)
      - CoreNodeManager     (dynamic core group per channel)
      - ElectionManager     (sequencer election per channel)
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 6001,
        config: Optional[UDPConfig] = None,
    ) -> None:
        self.config = config or get_udp_config()

        # Core node
        self.node = LocalNode(host=host, port=port, config=self.config)

        # Transport
        self.transport = UDPTransport(self.config, host=host, port=port)

        # send_fn: the universal callable used by all components
        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            return self.transport.send(peer, msg)

        self._send_fn = send_fn

        # Storage (disk persistence)
        self.store: Optional[MessageStore] = None
        if self.config.storage.enabled:
            self.store = MessageStore(self.config.storage.db_path)

        # Heartbeat manager (created first — provides congestion data)
        self.heartbeat = HeartbeatManager(
            node=self.node,
            send_fn=send_fn,
            on_node_dead=lambda peer: self.discovery.trigger_rediscovery(peer),
        )

        # Edge manager (wired to heartbeat congestion + discovery callback)
        self.edge_manager = EdgeManager(
            node=self.node,
            on_edge_died=lambda nid: self.discovery.trigger_rediscovery(),
            get_congestion_fn=self.heartbeat.get_congestion,
        )

        # Now wire edge_manager back into heartbeat
        self.heartbeat.edge_manager = self.edge_manager

        # Discovery
        self.discovery = PeerDiscovery(
            node=self.node,
            send_fn=send_fn,
        )

        # Dissemination engine
        self.engine = DisseminationEngine(
            node=self.node,
            send_fn=send_fn,
            edge_manager=self.edge_manager,
            store=self.store,
        )

        # Wire sequencer-unreachable callback
        self.engine.set_on_sequencer_unreachable(self._on_sequencer_unreachable)

        # Status reporter (created lazily if monitor enabled)
        self.status_reporter = None

        # Per-channel state
        self._channel_configs: Dict[str, ChannelConfig] = {}
        self._core_managers: Dict[str, CoreNodeManager] = {}
        self._election_managers: Dict[str, ElectionManager] = {}

        # Pending FIND_SEQUENCER_RESPONSE (used during bootstrap)
        self._find_seq_responses: Dict[str, list] = {}
        self._find_seq_events: Dict[str, threading.Event] = {}

        # Prepare channels
        self._setup_channels(send_fn)

    # ------------------------------------------------------------------
    # Channel setup (prepare for bootstrap)
    # ------------------------------------------------------------------

    def _setup_channels(self, send_fn) -> None:
        """Record channel configs. Bootstrap happens in start()."""
        for ch_cfg in self.config.channels:
            if not ch_cfg.channel_name:
                continue
            self._channel_configs[ch_cfg.channel_name] = ch_cfg

    # ------------------------------------------------------------------
    # Bootstrap: discover or self-elect sequencer per channel
    # ------------------------------------------------------------------

    def _bootstrap_channels(self) -> None:
        """
        For each configured channel, discover the sequencer or self-elect.
        Called after the transport is up and network is joined.
        """
        for channel, ch_cfg in self._channel_configs.items():
            self._bootstrap_channel(channel, ch_cfg)

    def _bootstrap_channel(self, channel: str, ch_cfg: ChannelConfig) -> None:
        """Bootstrap a single channel."""
        # Create CoreNodeManager
        core_mgr = CoreNodeManager(
            channel=channel,
            node=self.node,
            send_fn=self._send_fn,
            store=self.store,
            core_join_min_seq=ch_cfg.core_join_min_seq,
        )
        core_mgr.load_state()
        self._core_managers[channel] = core_mgr

        # Create ElectionManager
        election_mgr = ElectionManager(
            channel=channel,
            node=self.node,
            send_fn=self._send_fn,
            core_manager=core_mgr,
            store=self.store,
            on_new_sequencer=self._on_new_sequencer,
            sequencer_heartbeat_interval=ch_cfg.sequencer_heartbeat_interval,
            sequencer_heartbeat_timeout=ch_cfg.sequencer_heartbeat_timeout,
            election_timeout=ch_cfg.election_timeout,
        )
        election_mgr.load_state()
        self._election_managers[channel] = election_mgr

        # Register core manager with engine
        self.engine.register_core_manager(channel, core_mgr)

        # Try to discover the sequencer
        seq_addr = election_mgr.sequencer_address

        if seq_addr:
            # We have a persisted sequencer — use it
            logger.info(
                "[UDPServer] Channel '%s': using persisted sequencer %s",
                channel, seq_addr,
            )
            self._activate_sequencer(channel, seq_addr, election_mgr.last_known_seq + 1)
            return

        # No persisted state — broadcast FIND_SEQUENCER_REQUEST
        logger.info(
            "[UDPServer] Channel '%s': broadcasting FIND_SEQUENCER_REQUEST",
            channel,
        )
        self._find_seq_responses[channel] = []
        self._find_seq_events[channel] = threading.Event()

        # Build target list: alive neighbours + seed nodes (for redundancy)
        seed_infos = []
        for seed_addr in self.config.discovery.seed_nodes:
            try:
                h, p = seed_addr.rsplit(":", 1)
                si = NodeInfo.from_address(h, int(p))
                if si.node_id != self.node.node_id:
                    seed_infos.append(si)
            except (ValueError, AttributeError):
                pass

        bootstrap_cfg = self.config.bootstrap
        for attempt in range(bootstrap_cfg.broadcast_attempts):
            req = self.node.factory.find_sequencer_request(channel)

            # Send to all alive neighbours
            targets_sent = set()
            for entry in self.node.alive_neighbours():
                try:
                    self._send_fn(entry.info, req)
                    targets_sent.add(entry.info.node_id)
                except Exception:
                    pass

            # Also send directly to seed nodes (they may know the sequencer
            # even if they aren't in our neighbour table yet)
            for si in seed_infos:
                if si.node_id not in targets_sent:
                    try:
                        self._send_fn(si, req)
                    except Exception:
                        pass

            # Wait for a response
            if self._find_seq_events[channel].wait(bootstrap_cfg.broadcast_interval):
                break

        # Check if we got a response
        responses = self._find_seq_responses.pop(channel, [])
        self._find_seq_events.pop(channel, None)

        if responses:
            # Use the first valid response
            resp = responses[0]
            seq_addr = resp.get("sequencer_address", "")
            core_list = resp.get("core_list", [])
            if seq_addr:
                logger.info(
                    "[UDPServer] Channel '%s': discovered sequencer %s",
                    channel, seq_addr,
                )
                election_mgr.set_sequencer(seq_addr)
                if core_list:
                    core_mgr.set_initial_core(core_list)
                    self.engine.update_core_nodes_from_manager(channel)
                self._activate_sequencer(channel, seq_addr, election_mgr.last_known_seq + 1)
                return

        # No response — self-elect as sequencer (first node in network)
        logger.info(
            "[UDPServer] Channel '%s': no sequencer found — self-electing",
            channel,
        )
        election_mgr.set_sequencer(self.node.address, next_seq=1)
        core_mgr.set_initial_core([self.node.address])
        self.engine.update_core_nodes_from_manager(channel)
        self._activate_sequencer(channel, self.node.address, 1)

    def _activate_sequencer(
        self, channel: str, seq_addr: str, next_seq: int
    ) -> None:
        """Wire up the sequencer (local or remote) for a channel."""
        try:
            host, port_str = seq_addr.rsplit(":", 1)
            seq_info = NodeInfo.from_address(host, int(port_str))
        except (ValueError, AttributeError):
            logger.error(
                "[UDPServer] Invalid sequencer address: %s", seq_addr
            )
            return

        election_mgr = self._election_managers.get(channel)
        core_mgr = self._core_managers.get(channel)

        if seq_info.node_id == self.node.node_id:
            # WE are the sequencer
            sequencer = ChannelSequencer(
                channel, self.node, self._send_fn, store=self.store,
            )
            sequencer.load_state()
            # Ensure seq counter is at least next_seq - 1
            if sequencer.current_seq < next_seq - 1:
                sequencer._seq_counter = next_seq - 1
            self.engine.register_sequencer(channel, sequencer)
            # Remove remote sequencer if set
            self.engine._sequencer_nodes.pop(channel, None)
            # Start sequencer heartbeat
            if election_mgr:
                election_mgr.start_sequencer_heartbeat(sequencer)
            logger.info(
                "[UDPServer] Channel '%s': WE are the sequencer (seq=%d)",
                channel, sequencer.current_seq,
            )
        else:
            # Remote sequencer
            self.engine.register_sequencer_node(channel, seq_info)
            # Remove local sequencer if set
            self.engine._sequencers.pop(channel, None)
            # If we're a core node, start monitoring the sequencer
            if election_mgr and core_mgr and core_mgr.is_core:
                election_mgr.start_sequencer_checker()
            logger.info(
                "[UDPServer] Channel '%s': remote sequencer at %s",
                channel, seq_addr,
            )

    # ------------------------------------------------------------------
    # Callback: new sequencer elected or discovered
    # ------------------------------------------------------------------

    def _on_new_sequencer(
        self, channel: str, address: str, next_seq: int
    ) -> None:
        """Callback from ElectionManager when a new sequencer is elected."""
        logger.info(
            "[UDPServer] New sequencer for '%s': %s (next_seq=%d)",
            channel, address, next_seq,
        )

        # Stop any running sequencer heartbeat/checker
        em = self._election_managers.get(channel)
        if em:
            em.stop_sequencer_heartbeat()
            em.stop_sequencer_checker()

        # Activate the new sequencer
        self._activate_sequencer(channel, address, next_seq)

        # Update core nodes in engine
        self.engine.update_core_nodes_from_manager(channel)

    # ------------------------------------------------------------------
    # Callback: sequencer unreachable (from DisseminationEngine)
    # ------------------------------------------------------------------

    def _on_sequencer_unreachable(self, channel: str) -> None:
        """Non-core node couldn't reach sequencer — try to find the new one."""
        logger.warning(
            "[UDPServer] Sequencer unreachable for '%s' — discovering...",
            channel,
        )
        req = self.node.factory.find_sequencer_request(channel)
        for entry in self.node.alive_neighbours():
            try:
                self._send_fn(entry.info, req)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    def _on_message(self, msg: Message, sender_info: Optional[NodeInfo]) -> None:
        """Central message dispatcher."""
        # --- Heartbeat ---
        if msg.msg_type in (MessageType.HEARTBEAT, MessageType.HEARTBEAT_ACK):
            self.heartbeat.handle_incoming(msg, sender_info)

        # --- Discovery ---
        elif msg.msg_type == MessageType.PEER_LIST_REQUEST:
            if sender_info:
                self.discovery.handle_peer_list_request(msg, sender_info)
        elif msg.msg_type == MessageType.PEER_LIST_RESPONSE:
            self.discovery.handle_peer_list_response(msg)

        # --- Gossip / total order ---
        elif msg.msg_type in (MessageType.GOSSIP, MessageType.SEQUENCE_ASSIGN):
            self.engine.receive(msg, sender_info)

        elif msg.msg_type == MessageType.HISTORY_REQUEST:
            self.engine.handle_history_request(msg, sender_info)
        elif msg.msg_type == MessageType.HISTORY_RESPONSE:
            self.engine.handle_history_response(msg)

        # --- Dynamic core nodes ---
        elif msg.msg_type == MessageType.CORE_JOIN_REQUEST:
            channel = msg.channel
            mgr = self._core_managers.get(channel)
            if mgr and channel in self.engine._sequencers and sender_info:
                mgr.handle_join_request(msg, sender_info)

        elif msg.msg_type == MessageType.CORE_JOIN_APPROVED:
            channel = msg.channel
            mgr = self._core_managers.get(channel)
            if mgr:
                mgr.handle_join_approved(msg)
                self.engine.update_core_nodes_from_manager(channel)
                # If we just became core, start sequencer checker
                em = self._election_managers.get(channel)
                if em and mgr.is_core and not em.is_sequencer:
                    em.start_sequencer_checker()
            # Forward via gossip
            self._forward_control_msg(msg, sender_info)

        elif msg.msg_type == MessageType.CORE_LEAVE:
            channel = msg.channel
            mgr = self._core_managers.get(channel)
            if mgr:
                mgr.handle_core_leave(msg)
                self.engine.update_core_nodes_from_manager(channel)
            self._forward_control_msg(msg, sender_info)

        # --- Sequencer election ---
        elif msg.msg_type == MessageType.SEQUENCER_HEARTBEAT:
            channel = msg.channel
            em = self._election_managers.get(channel)
            if em:
                em.handle_sequencer_heartbeat(msg)

        elif msg.msg_type == MessageType.ELECTION_REQUEST:
            channel = msg.channel
            em = self._election_managers.get(channel)
            if em and sender_info:
                em.handle_election_request(msg, sender_info)

        elif msg.msg_type == MessageType.NEW_SEQUENCER:
            channel = msg.channel
            em = self._election_managers.get(channel)
            if em:
                em.handle_new_sequencer(msg)
            self._forward_control_msg(msg, sender_info)

        # --- Sequencer discovery ---
        elif msg.msg_type == MessageType.FIND_SEQUENCER_REQUEST:
            channel = msg.channel
            em = self._election_managers.get(channel)
            if em and sender_info:
                em.handle_find_sequencer_request(msg, sender_info)

        elif msg.msg_type == MessageType.FIND_SEQUENCER_RESPONSE:
            self._handle_find_sequencer_response(msg)

        # --- Monitoring ---
        elif msg.msg_type == MessageType.STATUS_REQUEST:
            if self.status_reporter:
                self.status_reporter._report()

        elif msg.msg_type in (MessageType.JOIN, MessageType.LEAVE):
            logger.debug(
                "[UDPServer] Received %s from %s", msg.msg_type.name,
                sender_info.node_id[:8] if sender_info else "unknown",
            )

        else:
            logger.debug("[UDPServer] Unknown message type: %s", msg.msg_type)

    def _forward_control_msg(
        self, msg: Message, sender_info: Optional[NodeInfo]
    ) -> None:
        """Forward a control message (CORE_JOIN_APPROVED, CORE_LEAVE, NEW_SEQUENCER) via gossip."""
        if not self.node.should_forward(msg.msg_id):
            return
        self.node.mark_seen(msg.msg_id)
        self.node.increment_forward_count(msg.msg_id)
        sender_id = sender_info.node_id if sender_info else msg.sender_id
        for entry in self.node.alive_neighbours():
            if entry.info.node_id == sender_id:
                continue
            try:
                self._send_fn(entry.info, msg)
            except Exception:
                pass

    def _handle_find_sequencer_response(self, msg: Message) -> None:
        """Handle FIND_SEQUENCER_RESPONSE during bootstrap or recovery."""
        channel = msg.channel
        content = msg.content if isinstance(msg.content, dict) else {}

        # During bootstrap: store response and signal event
        if channel in self._find_seq_events:
            self._find_seq_responses.setdefault(channel, []).append(content)
            self._find_seq_events[channel].set()
            return

        # During runtime recovery: update sequencer via ElectionManager
        em = self._election_managers.get(channel)
        if em:
            em.handle_find_sequencer_response(msg)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all components and join the network."""
        logger.info("[UDPServer] Starting node %s", self.node.info)

        # Load persisted neighbours (to be revalidated after transport starts).
        persisted_peers: list[NodeInfo] = []
        if self.store:
            saved_peers = self.store.load_neighbours()
            for p in saved_peers:
                try:
                    peer = NodeInfo(
                        node_id=p["node_id"], host=p["host"], port=int(p["port"])
                    )
                    if peer.node_id != self.node.node_id:
                        persisted_peers.append(peer)
                except (KeyError, ValueError):
                    pass

        # Start transport listener
        self.transport.start(on_message=self._on_message)

        # Re-validate persisted peers instead of trusting stale entries.
        if persisted_peers:
            logger.info(
                "[UDPServer] Re-validating %d persisted peers",
                len(persisted_peers),
            )
            self.discovery._try_connect_all(persisted_peers)

        # Join the network
        self.discovery.join_network()

        # Start background services
        self.heartbeat.start()
        self.edge_manager.start()
        self.discovery.start()
        self.engine.start_gap_checker()

        # Wait for at least one neighbour before bootstrap
        # (discovery.join_network() contacts seeds but responses are async)
        bootstrap_cfg = self.config.bootstrap
        deadline = time.time() + bootstrap_cfg.bootstrap_wait
        while self.node.neighbour_count == 0 and time.time() < deadline:
            time.sleep(0.3)

        # Bootstrap channels (discover/elect sequencers)
        self._bootstrap_channels()

        # Start status reporter if monitoring is enabled
        if self.config.monitor.enabled:
            try:
                from components.status_reporter import StatusReporter
                self.status_reporter = StatusReporter(
                    node=self.node,
                    send_fn=self._send_fn,
                    monitor_address=self.config.monitor.monitor_address,
                    interval=self.config.monitor.report_interval,
                )
                self.status_reporter.set_engine(self.engine)
                self.status_reporter.set_core_managers(self._core_managers)
                self.status_reporter.set_election_managers(self._election_managers)
                self.status_reporter.start()
            except ImportError:
                logger.warning("[UDPServer] StatusReporter not available")

        logger.info("[UDPServer] Node %s is running", self.node.address)

    def stop(self) -> None:
        """Gracefully shut down all components. Safe to call multiple times."""
        if getattr(self, '_stopped', False):
            return
        self._stopped = True
        logger.info("[UDPServer] Stopping node %s", self.node.address)

        if self.status_reporter:
            self.status_reporter.stop()

        # Stop election managers
        for em in self._election_managers.values():
            em.stop()

        self.engine.stop_gap_checker()
        self.discovery.shutdown()
        self.edge_manager.stop()
        self.heartbeat.stop()
        self.transport.stop()

        # Persist state to disk
        if self.store:
            self.store.save_neighbours([
                {"node_id": e.info.node_id, "host": e.info.host, "port": e.info.port}
                for e in self.node.all_neighbours()
            ])
            for ch, buf in self.engine.delivery_buffers.items():
                self.store.save_channel_state(
                    ch, buf.last_delivered_seq, self.node.is_subscribed(ch)
                )
            # Save core/election state
            for mgr in self._core_managers.values():
                mgr.save_state()
            for em in self._election_managers.values():
                em.save_state()
            self.store.close()

        logger.info("[UDPServer] Node %s stopped", self.node.address)

    def __repr__(self) -> str:
        return (
            f"UDPServer(addr={self.node.address}, "
            f"neighbours={self.node.neighbour_count})"
        )
