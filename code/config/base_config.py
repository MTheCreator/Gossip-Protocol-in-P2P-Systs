"""
Base configuration shared across all transport backends.
Tune these values per environment (simulation vs real deployment).
"""
from dataclasses import dataclass, field
from typing import List, Literal, Optional


@dataclass
class HeartbeatConfig:
    """
    Heartbeat timing parameters as defined in the spec.
      Tsuspect = 3 * delta_t
      Tdead    = 6 * delta_t
    """
    delta_t: float = 1.0           # Heartbeat interval in seconds
    tsuspect_multiplier: int = 3   # Missed beats before SUSPECT
    tdead_multiplier: int = 6      # Missed beats before DEAD

    @property
    def tsuspect(self) -> float:
        return self.delta_t * self.tsuspect_multiplier

    @property
    def tdead(self) -> float:
        return self.delta_t * self.tdead_multiplier


@dataclass
class GossipConfig:
    """Core gossip overlay parameters."""
    fmin: int = 2                  # Minimum active neighbors
    fmax: int = 6                  # Maximum active neighbors
    receipt_threshold: int = 3     # Max forwards before ignoring a message
    drop_probability: float = 0.0  # Simulated message drop rate [0.0 – 1.0]
    backoff_initial: float = 1.0   # Initial back-off on edge failure (seconds)
    backoff_max: float = 60.0      # Maximum back-off cap (seconds)
    backoff_multiplier: float = 2.0


@dataclass
class DiscoveryConfig:
    """
    Peer-discovery parameters (C1).

    Priority: peers.json cache → hardcoded seeds → peer exchange.
    """
    peer_cache_file: str = "peers.json"          # Path to local peer cache
    peer_cache_max: int = 20                      # Max peers to remember
    seed_nodes: List[str] = field(default_factory=lambda: ["127.0.0.1:5000"])
    discovery_timeout: float = 3.0                # Timeout per connection attempt (s)
    peer_exchange_interval: float = 30.0          # Background refresh interval (s)
    seed_fmax_override: int = 50                  # Seed nodes accept more connections


@dataclass
class ChannelConfig:
    """Per-channel configuration. Sequencer and core nodes are discovered dynamically."""
    channel_name: str = ""
    history_batch_size: int = 100                      # Max messages per HISTORY_RESPONSE
    sequencer_heartbeat_interval: float = 0.3          # Fast heartbeat from sequencer to core nodes
    sequencer_heartbeat_timeout: float = 1.5           # Core nodes suspect sequencer after this
    election_timeout: float = 3.0                      # Max time to collect ELECTION_REQUEST votes
    core_join_min_seq: int = 5                         # Min delivered seq before eligible to join core


@dataclass
class StorageConfig:
    """Disk persistence settings."""
    enabled: bool = True
    db_path: str = "gossip_store.db"              # SQLite database file path
    checkpoint_interval: float = 30.0             # Seconds between state snapshots


@dataclass
class MonitorConfig:
    """Monitoring server settings."""
    enabled: bool = False
    monitor_address: str = "127.0.0.1:9090"       # Address of the monitoring server
    report_interval: float = 5.0                  # Seconds between status reports



@dataclass
class BootstrapConfig:
    """Bootstrap configuration for first-node-becomes-sequencer logic."""
    bootstrap_wait: float = 3.0          # Seconds to wait for FIND_SEQUENCER_RESPONSE
    broadcast_attempts: int = 3          # Number of FIND_SEQUENCER_REQUEST broadcasts
    broadcast_interval: float = 1.0      # Seconds between broadcast attempts


@dataclass
class EdgeReliabilityConfig:
    """
    EMA-based edge reliability scoring parameters.

    reliability_score = λ * old_score + (1 - λ) * outcome
    where outcome ∈ {0.0 (failure), 1.0 (success)}
    """
    lambda_ema: float = 0.8          # EMA smoothing factor (0 < λ < 1)
    dead_threshold: float = 0.1      # below this → edge is dead
    revival_threshold: float = 0.2   # above this after dead → edge revived
    initial_score: float = 1.0       # starting reliability for new edges


@dataclass
class BaseConfig:
    transport: Literal["grpc", "udp"] = "grpc"
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    edge_reliability: EdgeReliabilityConfig = field(default_factory=EdgeReliabilityConfig)
    channels: List[ChannelConfig] = field(default_factory=list)
    storage: StorageConfig = field(default_factory=StorageConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    bootstrap: BootstrapConfig = field(default_factory=BootstrapConfig)
    bootstrap_host: str = "127.0.0.1"
    bootstrap_port: int = 5000
    node_id_bits: int = 160        # SHA-1 sized node IDs (Kademlia compatible)
    k_bucket_size: int = 20        # Kademlia k parameter