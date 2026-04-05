"""
gRPC transport configuration.
"""
from dataclasses import dataclass, field
from .base_config import BaseConfig, HeartbeatConfig, GossipConfig


@dataclass
class GRPCConfig(BaseConfig):
    transport: str = "grpc"
    default_port: int = 50051
    max_workers: int = 10
    max_message_size_bytes: int = 4 * 1024 * 1024   # 4 MB
    keepalive_time_ms: int = 10_000
    keepalive_timeout_ms: int = 5_000
    # Slightly longer heartbeat interval — gRPC adds connection overhead
    heartbeat: HeartbeatConfig = field(
        default_factory=lambda: HeartbeatConfig(delta_t=1.0)
    )
    gossip: GossipConfig = field(default_factory=GossipConfig)


def get_grpc_config() -> GRPCConfig:
    return GRPCConfig()