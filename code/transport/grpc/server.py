"""
transport/grpc/server.py  --  gRPC Node Server
=================================================
Wires together all components into a single runnable gossip node
using gRPC as the transport backend.

Supports dynamic sequencer election and core node management
(same logic as UDPServer).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, Optional

from config.grpc_config import GRPCConfig, get_grpc_config
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
from transport.grpc.transport import GRPCTransport

logger = logging.getLogger(__name__)


class GRPCServer:
    """
    Top-level node server using gRPC transport.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 50051,
        config: Optional[GRPCConfig] = None,
    ) -> None:
        self.config = config or get_grpc_config()

        # Core node
        self.node = LocalNode(host=host, port=port, config=self.config)

        # Transport
        self.transport = GRPCTransport(
            self.config,
            host=host,
            port=port,
            local_node_id=self.node.node_id,
        )

        def send_fn(peer: NodeInfo, msg: Message) -> bool:
            return self.transport.send(peer, msg)

        self._send_fn = send_fn

        # Storage
        self.store: Optional[MessageStore] = None
        if self.config.storage.enabled:
            self.store = MessageStore(self.config.storage.db_path)

        # Heartbeat
        self.heartbeat = HeartbeatManager(
            node=self.node,
            send_fn=send_fn,
            on_node_dead=lambda peer: self.discovery.trigger_rediscovery(peer),
        )

        # Edge manager
        self.edge_manager = EdgeManager(
            node=self.node,
            on_edge_died=lambda nid: self.discovery.trigger_rediscovery(),
            get_congestion_fn=self.heartbeat.get_congestion,
        )
        self.heartbeat.edge_manager = self.edge_manager

        # Discovery
        self.discovery = PeerDiscovery(node=self.node, send_fn=send_fn)

        # Dissemination
        self.engine = DisseminationEngine(
            node=self.node,
            send_fn=send_fn,
            edge_manager=self.edge_manager,
            store=self.store,
        )
        self.engine.set_on_sequencer_unreachable(self._on_sequencer_unreachable)

        self.status_reporter = None

        # Per-channel state
        self._channel_configs: Dict[str, ChannelConfig] = {}
        self._core_managers: Dict[str, CoreNodeManager] = {}
        self._election_managers: Dict[str, ElectionManager] = {}
        self._find_seq_responses: Dict[str, list] = {}
        self._find_seq_events: Dict[str, threading.Event] = {}

        self._setup_channels(send_fn)

    # ------------------------------------------------------------------
    # Channel setup
    # ------------------------------------------------------------------

    def _setup_channels(self, send_fn) -> None:
        for ch_cfg in self.config.channels:
            if not ch_cfg.channel_name:
                continue
            self._channel_configs[ch_cfg.channel_name] = ch_cfg

    def _bootstrap_channels(self) -> None:
        for channel, ch_cfg in self._channel_configs.items():
            self._bootstrap_channel(channel, ch_cfg)

    def _bootstrap_channel(self, channel: str, ch_cfg: ChannelConfig) -> None:
        core_mgr = CoreNodeManager(
            channel=channel,
            node=self.node,
            send_fn=self._send_fn,
            store=self.store,
            core_join_min_seq=ch_cfg.core_join_min_seq,
        )
        core_mgr.load_state()
        self._core_managers[channel] = core_mgr

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
        core_mgr.set_view_number_provider(lambda em=election_mgr: em.current_view)
        self.engine.register_core_manager(channel, core_mgr)

        persisted_seq_addr = election_mgr.sequencer_address

        # Broadcast FIND_SEQUENCER_REQUEST
        self._find_seq_responses[channel] = []
        self._find_seq_events[channel] = threading.Event()

        # Build seed node targets for fallback
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
        for _ in range(bootstrap_cfg.broadcast_attempts):
            req = self.node.factory.find_sequencer_request(channel)
            targets_sent = set()
            for entry in self.node.alive_neighbours():
                try:
                    self._send_fn(entry.info, req)
                    targets_sent.add(entry.info.node_id)
                except Exception:
                    pass
            for si in seed_infos:
                if si.node_id not in targets_sent:
                    try:
                        self._send_fn(si, req)
                    except Exception:
                        pass
            if self._find_seq_events[channel].wait(bootstrap_cfg.broadcast_interval):
                break

        responses = self._find_seq_responses.pop(channel, [])
        self._find_seq_events.pop(channel, None)

        if responses:
            resp = responses[0]
            seq_addr = resp.get("sequencer_address", "")
            core_list = resp.get("core_list", [])
            if seq_addr:
                election_mgr.set_sequencer(seq_addr)
                if core_list:
                    core_mgr.set_initial_core(core_list)
                    self.engine.update_core_nodes_from_manager(channel)
                self._activate_sequencer(
                    channel, seq_addr, election_mgr.last_known_seq + 1
                )
                return

        # Discovery failed: fall back to persisted sequencer if available.
        if persisted_seq_addr:
            self._activate_sequencer(
                channel,
                persisted_seq_addr,
                election_mgr.last_known_seq + 1,
            )
            return

        # Self-elect
        election_mgr.set_sequencer(self.node.address, next_seq=1)
        core_mgr.set_initial_core([self.node.address])
        self.engine.update_core_nodes_from_manager(channel)
        self._activate_sequencer(channel, self.node.address, 1)

    def _activate_sequencer(self, channel: str, seq_addr: str, next_seq: int) -> None:
        try:
            host, port_str = seq_addr.rsplit(":", 1)
            seq_info = NodeInfo.from_address(host, int(port_str))
        except (ValueError, AttributeError):
            return

        election_mgr = self._election_managers.get(channel)
        core_mgr = self._core_managers.get(channel)

        if seq_info.node_id == self.node.node_id:
            sequencer = ChannelSequencer(
                channel,
                self.node,
                self._send_fn,
                store=self.store,
            )
            sequencer.load_state()
            if sequencer.current_seq < next_seq - 1:
                sequencer._seq_counter = next_seq - 1
            self.engine.register_sequencer(channel, sequencer)
            self.engine._sequencer_nodes.pop(channel, None)
            if election_mgr:
                election_mgr.start_sequencer_heartbeat(sequencer)
        else:
            self.engine.register_sequencer_node(channel, seq_info)
            self.engine._sequencers.pop(channel, None)
            if election_mgr and core_mgr and core_mgr.is_core:
                election_mgr.start_sequencer_checker()

    def _on_new_sequencer(self, channel: str, address: str, next_seq: int) -> None:
        em = self._election_managers.get(channel)
        if em:
            em.stop_sequencer_heartbeat()
            em.stop_sequencer_checker()
        self._activate_sequencer(channel, address, next_seq)
        self.engine.update_core_nodes_from_manager(channel)

    def _on_sequencer_unreachable(self, channel: str) -> None:
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
        if msg.msg_type in (MessageType.HEARTBEAT, MessageType.HEARTBEAT_ACK):
            self.heartbeat.handle_incoming(msg, sender_info)

        elif msg.msg_type == MessageType.PEER_LIST_REQUEST:
            if sender_info:
                self.discovery.handle_peer_list_request(msg, sender_info)
        elif msg.msg_type == MessageType.PEER_LIST_RESPONSE:
            self.discovery.handle_peer_list_response(msg)

        elif msg.msg_type in (MessageType.GOSSIP, MessageType.SEQUENCE_ASSIGN):
            self.engine.receive(msg, sender_info)
        elif msg.msg_type == MessageType.HISTORY_REQUEST:
            self.engine.handle_history_request(msg, sender_info)
        elif msg.msg_type == MessageType.HISTORY_RESPONSE:
            self.engine.handle_history_response(msg)

        elif msg.msg_type == MessageType.CORE_JOIN_REQUEST:
            ch = msg.channel
            mgr = self._core_managers.get(ch)
            if mgr and ch in self.engine._sequencers and sender_info:
                mgr.handle_join_request(msg, sender_info)
        elif msg.msg_type == MessageType.CORE_JOIN_APPROVED:
            ch = msg.channel
            mgr = self._core_managers.get(ch)
            em = self._election_managers.get(ch)
            if em and not em.is_message_view_acceptable(msg):
                return
            if mgr:
                mgr.handle_join_approved(msg)
                self.engine.update_core_nodes_from_manager(ch)
                if em and mgr.is_core and not em.is_sequencer:
                    em.start_sequencer_checker()
            self._forward_control_msg(msg, sender_info)
        elif msg.msg_type == MessageType.CORE_LEAVE:
            ch = msg.channel
            mgr = self._core_managers.get(ch)
            em = self._election_managers.get(ch)
            if em and not em.is_message_view_acceptable(msg):
                return
            if mgr:
                mgr.handle_core_leave(msg)
                self.engine.update_core_nodes_from_manager(ch)
            self._forward_control_msg(msg, sender_info)

        elif msg.msg_type == MessageType.SEQUENCER_HEARTBEAT:
            em = self._election_managers.get(msg.channel)
            if em:
                em.handle_sequencer_heartbeat(msg)
        elif msg.msg_type == MessageType.ELECTION_REQUEST:
            em = self._election_managers.get(msg.channel)
            if em and sender_info:
                em.handle_election_request(msg, sender_info)
        elif msg.msg_type == MessageType.NEW_SEQUENCER:
            em = self._election_managers.get(msg.channel)
            accepted = False
            if em:
                accepted = em.handle_new_sequencer(msg)
            if accepted:
                self._forward_control_msg(msg, sender_info)

        elif msg.msg_type == MessageType.FIND_SEQUENCER_REQUEST:
            em = self._election_managers.get(msg.channel)
            if em and sender_info:
                em.handle_find_sequencer_request(msg, sender_info)
        elif msg.msg_type == MessageType.FIND_SEQUENCER_RESPONSE:
            self._handle_find_sequencer_response(msg)

        elif msg.msg_type == MessageType.STATUS_REQUEST:
            if self.status_reporter:
                self.status_reporter._report()

    def _forward_control_msg(
        self, msg: Message, sender_info: Optional[NodeInfo]
    ) -> None:
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
        channel = msg.channel
        content = msg.content if isinstance(msg.content, dict) else {}
        if channel in self._find_seq_events:
            self._find_seq_responses.setdefault(channel, []).append(content)
            self._find_seq_events[channel].set()
            return
        em = self._election_managers.get(channel)
        if em:
            em.handle_find_sequencer_response(msg)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        logger.info("[GRPCServer] Starting node %s", self.node.info)

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

        self.transport.start(on_message=self._on_message)
        if persisted_peers:
            logger.info(
                "[GRPCServer] Re-validating %d persisted peers",
                len(persisted_peers),
            )
            self.discovery._try_connect_all(persisted_peers)
        self.discovery.join_network()
        self.heartbeat.start()
        self.edge_manager.start()
        self.discovery.start()
        self.engine.start_gap_checker()

        # Wait for at least one neighbour before bootstrap
        bootstrap_cfg = self.config.bootstrap
        deadline = time.time() + bootstrap_cfg.bootstrap_wait
        while self.node.neighbour_count == 0 and time.time() < deadline:
            time.sleep(0.3)

        self._bootstrap_channels()

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
                pass

        logger.info("[GRPCServer] Node %s is running", self.node.address)

    def stop(self) -> None:
        logger.info("[GRPCServer] Stopping node %s", self.node.address)

        if self.status_reporter:
            self.status_reporter.stop()

        for em in self._election_managers.values():
            em.stop()

        self.engine.stop_gap_checker()
        self.discovery.shutdown()
        self.edge_manager.stop()
        self.heartbeat.stop()
        self.transport.stop()

        if self.store:
            self.store.save_neighbours(
                [
                    {
                        "node_id": e.info.node_id,
                        "host": e.info.host,
                        "port": e.info.port,
                    }
                    for e in self.node.all_neighbours()
                ]
            )
            for ch, buf in self.engine.delivery_buffers.items():
                self.store.save_channel_state(
                    ch, buf.last_delivered_seq, self.node.is_subscribed(ch)
                )
            for mgr in self._core_managers.values():
                mgr.save_state()
            for em in self._election_managers.values():
                em.save_state()
            self.store.close()

        logger.info("[GRPCServer] Node %s stopped", self.node.address)
