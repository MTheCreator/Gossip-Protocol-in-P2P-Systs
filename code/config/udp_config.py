"""
UDP transport configuration.
UDP is connectionless — heartbeats are more critical here because
there is no built-in connection state to detect failures.
"""
from dataclasses import dataclass, field
from .base_config import BaseConfig, HeartbeatConfig, GossipConfig


@dataclass
class UDPConfig(BaseConfig):
    transport: str = "udp"
    default_port: int = 6001
    recv_buffer_size: int = 65_535     # Max UDP datagram
    send_buffer_size: int = 65_535
    socket_timeout: float = 0.5        # Non-blocking recv timeout (seconds)
    max_datagram_size: int = 1400      # Stay under typical MTU to avoid fragmentation
    # Shorter delta_t for UDP — cheap to send, important to detect failures fast
    heartbeat: HeartbeatConfig = field(
        default_factory=lambda: HeartbeatConfig(delta_t=0.5)
    )
    gossip: GossipConfig = field(default_factory=GossipConfig)


def get_udp_config() -> UDPConfig:
    return UDPConfig()