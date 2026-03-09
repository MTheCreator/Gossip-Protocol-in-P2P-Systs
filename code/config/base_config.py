"""
Base configuration shared across all transport backends.
Tune these values per environment (simulation vs real deployment).
"""
from dataclasses import dataclass, field
from typing import List, Literal


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


@dataclass
class BaseConfig:
    transport: Literal["grpc", "udp"] = "grpc"
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    bootstrap_host: str = "127.0.0.1"
    bootstrap_port: int = 5000
    node_id_bits: int = 160        # SHA-1 sized node IDs (Kademlia compatible)
    k_bucket_size: int = 20        # Kademlia k parameter