"""
components/edge_manager.py  —  C4: Edge Probability Update Strategies
======================================================================
Elghali Benjelloun — Gossip Protocol Project

Integrated from src/edge_manager.py into the shared code/ framework.

Manages per-edge forwarding probabilities stored on NeighbourEntry.edge_prob
inside a LocalNode's neighbour table.

Strategies (all configurable via BaseConfig or constructor overrides):
  1. Usage-based reinforcement — edge_prob += reinforce_step on success
  2. Inactivity decay          — edge_prob *= decay_factor each tick
  3. Optional random noise     — ± noise_amplitude each tick (disabled by default)

Integration points:
  - Call `em.reinforce(node_id)` from the dissemination / forwarding engine
    whenever a message is successfully forwarded to that neighbour.
  - Register `on_edge_died`   → triggers the C1 peer re-discovery callback.
  - Register `on_edge_revived`→ optional book-keeping / logging.
  - Wire into HeartbeatManager via `hb.on_edge_failure(node_id)` on death,
    and `hb.on_edge_recovery(node_id)` on revival.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

from core.node import LocalNode, NeighbourEntry, NodeInfo
from config.base_config import BaseConfig

logger = logging.getLogger(__name__)

Edge = Tuple[str, str]   # canonical (min_id, max_id)


def _canonical(a: str, b: str) -> Edge:
    """Return edge tuple in consistent lexicographic order."""
    return (a, b) if a < b else (b, a)


# ---------------------------------------------------------------------------
# EdgeManager
# ---------------------------------------------------------------------------

class EdgeManager:
    """
    Manages forwarding-probability lifecycle for all edges of a LocalNode.

    It runs a background decay loop (one thread, daemon) at the same interval
    as the heartbeat (delta_t).  The loop:
      1. Multiplies each neighbour's edge_prob by decay_factor.
      2. Applies optional random noise.
      3. Detects edges that fall below dead_threshold and fires on_edge_died.

    Public API
    ----------
    em.add_edge(node_id, initial_prob)   — start tracking a new edge
    em.remove_edge(node_id)              — stop tracking (also removes neighbour)
    em.reinforce(node_id)                — called on successful message forward
    em.get_probability(node_id)          — query current probability
    em.all_edges()                       — snapshot {node_id: prob}
    em.start() / em.stop()               — lifecycle
    """

    def __init__(
        self,
        node: LocalNode,
        reinforce_step:  float = 0.05,
        decay_factor:    float = 0.99,
        dead_threshold:  float = 0.01,
        enable_noise:    bool  = False,
        noise_amplitude: float = 0.02,
        on_edge_died:    Optional[Callable[[str], None]] = None,
        on_edge_revived: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.node            = node
        self.reinforce_step  = reinforce_step
        self.decay_factor    = decay_factor
        self.dead_threshold  = dead_threshold
        self.enable_noise    = enable_noise
        self.noise_amplitude = noise_amplitude

        # delta_t comes from the shared config so all components stay in sync
        self._delta_t = node.config.heartbeat.delta_t

        self._on_edge_died    = on_edge_died    or (lambda nid: None)
        self._on_edge_revived = on_edge_revived or (lambda nid: None)

        # Track which edges we consider "dead" to fire the callback only once
        self._dead_edges: set = set()
        self._lock = threading.Lock()

        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Edge management
    # ------------------------------------------------------------------

    def add_edge(self, node_id: str, initial_prob: float = 0.5) -> None:
        """
        Ensure node_id is in the local neighbour table with the given
        starting probability.  Does nothing if the edge is already tracked.
        """
        entry = self.node.get_neighbour(node_id)
        if entry is None:
            logger.warning(
                "[EdgeMgr] add_edge(%s) — node not in neighbour table; "
                "call node.add_neighbour() first.", node_id[:8]
            )
            return
        with self._lock:
            entry.edge_prob = max(0.0, min(1.0, initial_prob))
            self._dead_edges.discard(node_id)
        logger.info(
            "[EdgeMgr] Tracking edge → %s (p=%.2f)", node_id[:8], initial_prob
        )

    def remove_edge(self, node_id: str) -> None:
        """Stop tracking an edge (called after re-discovery is triggered)."""
        with self._lock:
            self._dead_edges.discard(node_id)
        logger.info("[EdgeMgr] Removed edge → %s", node_id[:8])

    def get_probability(self, node_id: str) -> Optional[float]:
        """Return current forwarding probability for node_id, or None."""
        entry = self.node.get_neighbour(node_id)
        return entry.edge_prob if entry else None

    def all_edges(self) -> Dict[str, float]:
        """Snapshot of {node_id: probability} for all tracked neighbours."""
        return {
            e.info.node_id: e.edge_prob
            for e in self.node.all_neighbours()
        }

    # ------------------------------------------------------------------
    # Feedback from forwarding engine
    # ------------------------------------------------------------------

    def reinforce(self, node_id: str) -> None:
        """
        Call when a message is successfully forwarded to node_id.
        Increases edge probability and may revive a previously dead edge.
        """
        entry = self.node.get_neighbour(node_id)
        if entry is None:
            return

        with self._lock:
            old_p = entry.edge_prob
            entry.edge_prob = min(1.0, entry.edge_prob + self.reinforce_step)

            was_dead = node_id in self._dead_edges
            if was_dead and entry.edge_prob > self.dead_threshold:
                self._dead_edges.discard(node_id)
                logger.info(
                    "[EdgeMgr] Edge → %s REVIVED (p: %.3f → %.3f)",
                    node_id[:8], old_p, entry.edge_prob,
                )
                self._on_edge_revived(node_id)
            else:
                logger.debug(
                    "[EdgeMgr] Edge → %s reinforced (p: %.3f → %.3f)",
                    node_id[:8], old_p, entry.edge_prob,
                )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background decay loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._decay_loop,
            name=f"EdgeMgr-{self.node.node_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        logger.info("[EdgeMgr] started (∆t=%.2fs, decay=%.3f)", self._delta_t, self.decay_factor)

    def stop(self) -> None:
        """Stop the decay loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._delta_t * 2)
        logger.info("[EdgeMgr] stopped")

    # ------------------------------------------------------------------
    # Core decay loop (runs in background thread)
    # ------------------------------------------------------------------

    def _decay_loop(self) -> None:
        """
        Every delta_t seconds:
          1. Decay all edge probabilities.
          2. Apply optional random noise.
          3. Detect edges that fell below dead_threshold → fire on_edge_died.
        """
        while self._running:
            time.sleep(self._delta_t)
            if not self._running:
                break

            for entry in self.node.all_neighbours():
                nid = entry.info.node_id
                with self._lock:
                    old_p = entry.edge_prob

                    # Strategy 1: usage-based decay
                    entry.edge_prob *= self.decay_factor

                    # Strategy 2 (optional): random noise
                    if self.enable_noise:
                        noise = random.uniform(-self.noise_amplitude, self.noise_amplitude)
                        entry.edge_prob = max(0.0, min(1.0, entry.edge_prob + noise))

                    entry.edge_prob = max(0.0, min(1.0, entry.edge_prob))

                    # Strategy 3: detect death
                    if nid not in self._dead_edges and entry.edge_prob < self.dead_threshold:
                        self._dead_edges.add(nid)
                        logger.warning(
                            "[EdgeMgr] Edge → %s DIED (p: %.3f → %.3f < threshold=%.3f)",
                            nid[:8], old_p, entry.edge_prob, self.dead_threshold,
                        )
                        self._on_edge_died(nid)
