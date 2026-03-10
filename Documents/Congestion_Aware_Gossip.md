# Congestion-Aware Edge Probability in Gossip Protocols
**Component C4: Edge Updates**

## Overview
This document justifies the algorithmic choices for the Edge Manager's probability updates (C4). The edge weight $p_{ij}$ evolves based on **two continuous signals**: message delivery reliability and network congestion — unified into a single EMA formula.

## Theoretical Background

1. **Adaptive Gossip Protocols**
   Standard gossip protocols use fixed probabilities, risking broadcast storms in dense networks. Adaptive variants scale probabilities based on local conditions.
   *Ref: Literature on adaptive gossip and probabilistic delay-based retransmission.*

2. **Delay-Based Congestion Control (TCP Vegas)**
   Instead of waiting for packet loss, measure RTT. Rising RTT → intermediate queues are filling up → congestion detected *before* failure.
   *Ref: "TCP Vegas" (Brakmo & Peterson, 1994).*

## The 3-Variable Formula

### Architecture: Who Does What

| Component | Responsibility | What it updates |
|---|---|---|
| **HeartbeatManager** | Measures RTT, maintains `congestion_score` ∈ [0,1] | Only its own metric |
| **DisseminationEngine** | Sends messages, reports success/failure | Calls `em.record_outcome()` |
| **EdgeManager** | Combines both → computes `p_ij` | Writes `edge_prob` |

### The Formula

When `record_outcome(peer_id, success)` is called:

$$\text{combined} = w_{rel} \times \text{outcome} + w_{cong} \times \text{congestion}$$
$$p_{new} = \lambda \times p_{old} + (1 - \lambda) \times \text{combined}$$

Where:
- $\text{outcome} \in \{0, 1\}$ — did the message arrive?
- $\text{congestion} \in [0, 1]$ — how clear is the link? (from HeartbeatManager)
- $w_{rel} = 0.7$, $w_{cong} = 0.3$ — reliability matters more, but congestion still counts
- $\lambda = 0.8$ — EMA smoothing factor

### Congestion Score (in HeartbeatManager)

On each `HEARTBEAT_ACK`:
$$\text{congestion\_score} = \text{clamp}\left(\frac{RTT_{ema}}{RTT_{current}}, 0, 1\right)$$

- $RTT \approx RTT_{ema}$ → score ≈ 1.0 (clear)
- $RTT = 2 \times RTT_{ema}$ → score = 0.5 (moderate congestion)
- $RTT = 10 \times RTT_{ema}$ → score = 0.1 (severe)

### Example Walkthrough ($\lambda=0.8,\ w_{rel}=0.7,\ w_{cong}=0.3$)

| Step | Delivery | Congestion | Combined | $p_{old}$ | $p_{new}$ |
|---|---|---|---|---|---|
| Start | — | 1.0 | — | 1.000 | — |
| 1 | ✅ (1.0) | 1.0 (clear) | 1.00 | 1.000 | **1.000** |
| 2 | ✅ (1.0) | 0.2 (heavy) | 0.76 | 1.000 | **0.952** |
| 3 | ❌ (0.0) | 0.1 (severe) | 0.03 | 0.952 | **0.768** |
| 4 | ✅ (1.0) | 1.0 (clear) | 1.00 | 0.768 | **0.814** |

> **Key insight:** Step 2 shows that even a *successful* delivery on a congested link drops the probability. This is exactly the behavior we want — the gossip protocol proactively routes away from slow links before they fail.

### Death & Revival
- **Dead**: $r_{ij} < 0.1$ → `on_edge_died()` → triggers C1 re-discovery
- **Revived**: $r_{ij} > 0.2$ after being dead → `on_edge_revived()`

## Architecture Diagram

```
HeartbeatManager                         EdgeManager
    |                                        |
    |-- HEARTBEAT → peer → ACK back          |
    |-- compute RTT, update congestion_score  |
    |                                        |
    |  get_congestion(peer_id) ------------->| (reads score)
    |                                        |
DisseminationEngine                          |
    |-- send message to peer                 |
    |-- em.record_outcome(peer, success) --->|
    |                                        | combined = w_rel*outcome + w_cong*congestion
    |                                        | p_new = λ*p_old + (1-λ)*combined
```

## Verification: Test Scenarios (`tests/test_congestion.py`)

| # | Scenario | Delivery | Congestion | Combined | $p_{new}$ | Why It Matters |
|---|---|---|---|---|---|---|
| 1 | Success + clear link | ✅ 1.0 | 1.0 (clear) | 1.00 | **1.000** | Baseline: no degradation when everything is fine |
| 2 | Success + heavy congestion | ✅ 1.0 | 0.2 (heavy) | 0.76 | **0.952** | Prob drops *even though delivery succeeded* |
| 3 | Failure + severe congestion | ❌ 0.0 | 0.1 (severe) | 0.03 | **0.806** | Both signals bad → fastest drop |
| 4 | End-to-end RTT integration | ✅ 1.0 | 0.068 (RTT spike) | — | **0.944** | Full pipeline: RTT → congestion_score → edge_prob |

> Test 4 uses real `HeartbeatManager` RTT measurement: baseline 20ms ACK, then a 300ms spike drives congestion to 0.068, and a subsequent fast ACK recovers it to 1.0.

## Conclusion
By making congestion a **continuous variable** blended directly into the EMA formula alongside delivery outcomes, the system naturally adapts. No separate penalty callback or decay loop is needed — the math handles everything in a single, unified update rule. The weights ($w_{rel}$, $w_{cong}$) provide a tunable knob to balance how much each signal matters.
