"""
components/edge_manager.py  —  C4: Edge Probability Update (EMA-based)
======================================================================
Elghali Benjelloun — Gossip Protocol Project

Manages per-edge forwarding probabilities using an **Exponential Moving
Average (EMA)** of message delivery outcomes.

Formula (on every send attempt):
    reliability_score = λ * old_score + (1 - λ) * outcome
    where outcome ∈ {1.0 (success), 0.0 (failure)}

The reliability_score is kept in sync with NeighbourEntry.edge_prob so
the probabilistic forwarding in DisseminationEngine automatically
favors reliable connections.

Integration points:
  - Call `em.record_outcome(node_id, success)` from the dissemination
    engine after every send attempt.
  - Convenience: `em.reinforce(node_id)` = record_outcome(…, True)
  - Convenience: `em.penalise(node_id)`  = record_outcome(…, False)
  - Register `on_edge_died`   → triggers C1 peer re-discovery callback.
  - Register `on_edge_revived`→ optional book-keeping / logging.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable, Dict, Optional, Tuple

from core.node import LocalNode, NeighbourEntry, NodeInfo
from config.base_config import BaseConfig, EdgeReliabilityConfig

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
    Manages forwarding-probability lifecycle for all edges of a LocalNode
    using EMA-based reliability scoring.

    Instead of a background decay loop, the reliability score is updated
    **reactively** on each message send attempt via `record_outcome()`.

    Public API
    ----------
    em.add_edge(node_id, initial_prob)       — start tracking a new edge
    em.remove_edge(node_id)                  — stop tracking
    em.record_outcome(node_id, success)      — EMA update (core method)
    em.reinforce(node_id)                    — alias for record_outcome(…, True)
    em.penalise(node_id)                     — alias for record_outcome(…, False)
    em.get_probability(node_id)              — query current probability
    em.all_edges()                           — snapshot {node_id: prob}
    """

    def __init__(
        self,
        node: LocalNode,
        lambda_ema: Optional[float] = None,
        dead_threshold: Optional[float] = None,
        revival_threshold: Optional[float] = None,
        initial_score: Optional[float] = None,
        on_edge_died: Optional[Callable[[str], None]] = None,
        on_edge_revived: Optional[Callable[[str], None]] = None,
        # Legacy compat — accepted but ignored
        reinforce_step: Optional[float] = None,
        decay_factor: Optional[float] = None,
        enable_noise: bool = False,
        noise_amplitude: float = 0.0,
    ) -> None:
        self.node = node

        # Read from config, allow constructor overrides
        rel_cfg: EdgeReliabilityConfig = node.config.edge_reliability
        self._lambda = lambda_ema if lambda_ema is not None else rel_cfg.lambda_ema
        self._dead_threshold = dead_threshold if dead_threshold is not None else rel_cfg.dead_threshold
        self._revival_threshold = revival_threshold if revival_threshold is not None else rel_cfg.revival_threshold
        self._initial_score = initial_score if initial_score is not None else rel_cfg.initial_score

        self._on_edge_died = on_edge_died or (lambda nid: None)
        self._on_edge_revived = on_edge_revived or (lambda nid: None)

        # Track which edges we consider "dead" to fire the callback only once
        self._dead_edges: set = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Edge management
    # ------------------------------------------------------------------

    def add_edge(self, node_id: str, initial_prob: Optional[float] = None) -> None:
        """
        Ensure node_id is in the local neighbour table with the given
        starting probability / reliability score.
        """
        entry = self.node.get_neighbour(node_id)
        if entry is None:
            logger.warning(
                "[EdgeMgr] add_edge(%s) — node not in neighbour table; "
                "call node.add_neighbour() first.", node_id[:8]
            )
            return
        prob = initial_prob if initial_prob is not None else self._initial_score
        with self._lock:
            entry.edge_prob = max(0.0, min(1.0, prob))
            entry.reliability_score = entry.edge_prob
            self._dead_edges.discard(node_id)
        logger.info(
            "[EdgeMgr] Tracking edge → %s (p=%.2f, λ=%.2f)",
            node_id[:8], entry.edge_prob, self._lambda,
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
    # Core: EMA-based reliability update
    # ------------------------------------------------------------------

    def record_outcome(self, node_id: str, success: bool) -> None:
        """
        Record the outcome of a message send attempt.

        Updates the reliability score using the EMA formula:
            score = λ * old_score + (1 - λ) * outcome

        Also:
          - Keeps edge_prob in sync with reliability_score
          - Detects edge death (score < dead_threshold)
          - Detects edge revival (score > revival_threshold after death)
        """
        entry = self.node.get_neighbour(node_id)
        if entry is None:
            return

        outcome = 1.0 if success else 0.0

        with self._lock:
            old_score = entry.reliability_score
            new_score = self._lambda * old_score + (1.0 - self._lambda) * outcome

            # Clamp to [0.0, 1.0]
            new_score = max(0.0, min(1.0, new_score))

            entry.reliability_score = new_score
            entry.edge_prob = new_score  # keep forwarding probability in sync

            logger.debug(
                "[EdgeMgr] %s outcome=%s (score: %.3f → %.3f)",
                node_id[:8], "OK" if success else "FAIL",
                old_score, new_score,
            )

            # Death detection: score fell below threshold
            if node_id not in self._dead_edges and new_score < self._dead_threshold:
                self._dead_edges.add(node_id)
                logger.warning(
                    "[EdgeMgr] Edge → %s DIED (score: %.3f → %.3f < threshold=%.3f)",
                    node_id[:8], old_score, new_score, self._dead_threshold,
                )
                self._on_edge_died(node_id)

            # Revival detection: score rose above revival threshold after being dead
            if node_id in self._dead_edges and new_score > self._revival_threshold:
                self._dead_edges.discard(node_id)
                logger.info(
                    "[EdgeMgr] Edge → %s REVIVED (score: %.3f → %.3f > threshold=%.3f)",
                    node_id[:8], old_score, new_score, self._revival_threshold,
                )
                self._on_edge_revived(node_id)

    # ------------------------------------------------------------------
    # Convenience aliases
    # ------------------------------------------------------------------

    def reinforce(self, node_id: str) -> None:
        """Alias: record a successful message delivery."""
        self.record_outcome(node_id, success=True)

    def penalise(self, node_id: str) -> None:
        """Alias: record a failed message delivery."""
        self.record_outcome(node_id, success=False)

    # ------------------------------------------------------------------
    # Lifecycle (no-ops — no background thread needed with EMA)
    # ------------------------------------------------------------------

    def start(self) -> None:
        """No-op. EMA updates are reactive, no background loop needed."""
        logger.info(
            "[EdgeMgr] started (λ=%.2f, dead_thresh=%.2f, revival_thresh=%.2f)",
            self._lambda, self._dead_threshold, self._revival_threshold,
        )

    def stop(self) -> None:
        """No-op."""
        logger.info("[EdgeMgr] stopped")
