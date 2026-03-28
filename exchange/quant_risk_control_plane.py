from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from enum import Enum
from statistics import mean
from typing import Deque, Dict, Iterable, List, Optional, Tuple
import math
import random
import time


class Action(str, Enum):
    OK = "OK"
    ALERT = "ALERT"
    SAFE_MODE = "SAFE_MODE"
    HALT = "HALT"


@dataclass
class Thresholds:
    stale_market_seconds: float = 1.5
    hard_stale_market_seconds: float = 5.0
    max_latency_ms: float = 50.0    # normal ~8 ms, stress ~58 ms → alert on stress
    hard_latency_ms: float = 100.0  # only halt on extreme spikes
    max_reject_rate: float = 0.08
    hard_reject_rate: float = 0.15
    max_drawdown: float = 25_000.0
    hard_drawdown: float = 50_000.0
    psi_alert: float = 0.25         # raised: brief distributional shifts → ALERT
    psi_halt: float = 0.60          # raised: only sustained heavy drift → SAFE_MODE
    min_reference_points: int = 300
    min_live_points: int = 120
    zscore_alert: float = 5.0
    zscore_halt: float = 8.0
    max_spread_bps: float = 60.0    # raised: stress MM quotes ~40 bps, normal ~10 bps


@dataclass
class MarketEvent:
    ts: float
    bid: float
    ask: float
    features: Dict[str, float]
    venue_latency_ms: float


@dataclass
class OrderEvent:
    ts: float
    accepted: bool


@dataclass
class PnLEvent:
    ts: float
    realized_pnl: float


@dataclass
class Decision:
    action: Action
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)


class RollingWindow:
    def __init__(self, size: int):
        self.size = size
        self.values: Deque[float] = deque(maxlen=size)

    def add(self, value: float) -> None:
        self.values.append(float(value))

    def __len__(self) -> int:
        return len(self.values)

    def data(self) -> List[float]:
        return list(self.values)

    def mean(self) -> Optional[float]:
        if not self.values:
            return None
        return sum(self.values) / len(self.values)


class EWMAZScore:
    """Streaming z-score approximation using EWMA mean and variance."""

    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha
        self.mu: Optional[float] = None
        self.var: float = 0.0

    def update(self, x: float) -> float:
        if self.mu is None:
            self.mu = x
            self.var = 1e-12
            return 0.0

        prev_mu = self.mu
        self.mu = self.alpha * x + (1.0 - self.alpha) * self.mu
        innovation = x - prev_mu
        self.var = self.alpha * (innovation ** 2) + (1.0 - self.alpha) * self.var
        std = math.sqrt(max(self.var, 1e-12))
        return (x - self.mu) / std


class PSIDetector:
    """
    Population Stability Index detector.

    - fit(reference): builds bin edges from reference quantiles
    - score(live): compares live distribution to reference distribution
    """

    def __init__(self, bins: int = 10, eps: float = 1e-6):
        self.bins = bins
        self.eps = eps
        self.edges: Optional[List[float]] = None
        self.reference_hist: Optional[List[float]] = None

    def fit(self, reference: Iterable[float]) -> None:
        ref = sorted(float(x) for x in reference)
        if len(ref) < self.bins:
            raise ValueError("Need at least as many reference points as bins")

        # Quantile-based edges; ensure monotonicity with tiny nudges.
        edges: List[float] = []
        n = len(ref)
        for i in range(self.bins + 1):
            q = i / self.bins
            idx = min(int(q * (n - 1)), n - 1)
            edges.append(ref[idx])
        for i in range(1, len(edges)):
            if edges[i] <= edges[i - 1]:
                edges[i] = edges[i - 1] + 1e-9
        self.edges = edges
        self.reference_hist = self._hist(ref)

    def score(self, live: Iterable[float]) -> float:
        if self.edges is None or self.reference_hist is None:
            raise RuntimeError("PSIDetector must be fit before scoring")
        live_vals = [float(x) for x in live]
        if not live_vals:
            return 0.0
        live_hist = self._hist(live_vals)
        psi = 0.0
        for p, q in zip(self.reference_hist, live_hist):
            p = max(p, self.eps)
            q = max(q, self.eps)
            psi += (q - p) * math.log(q / p)
        return psi

    def _hist(self, values: Iterable[float]) -> List[float]:
        if self.edges is None:
            raise RuntimeError("No edges")
        counts = [0] * self.bins
        total = 0
        for x in values:
            total += 1
            placed = False
            for i in range(self.bins):
                lo, hi = self.edges[i], self.edges[i + 1]
                if i == self.bins - 1:
                    if lo <= x <= hi:
                        counts[i] += 1
                        placed = True
                        break
                if lo <= x < hi:
                    counts[i] += 1
                    placed = True
                    break
            if not placed:
                if x < self.edges[0]:
                    counts[0] += 1
                else:
                    counts[-1] += 1
        return [c / max(total, 1) for c in counts]


class FeatureHealth:
    def __init__(self, name: str, reference_window: int = 2000, live_window: int = 300):
        self.name = name
        self.reference = RollingWindow(reference_window)
        self.live = RollingWindow(live_window)
        self.psi = PSIDetector(bins=10)
        self.zscore = EWMAZScore(alpha=0.03)
        self.ready = False
        self.last_z = 0.0
        self.last_psi = 0.0

    def update(self, x: float, thresholds: Thresholds) -> Dict[str, float]:
        self.last_z = self.zscore.update(x)
        if len(self.reference) < self.reference.size:
            self.reference.add(x)
        else:
            if not self.ready and len(self.reference) >= thresholds.min_reference_points:
                self.psi.fit(self.reference.data())
                self.ready = True
            self.live.add(x)
            if self.ready and len(self.live) >= thresholds.min_live_points:
                self.last_psi = self.psi.score(self.live.data())
        return {"zscore": self.last_z, "psi": self.last_psi}


class StrategyHealthMonitor:
    def __init__(self, thresholds: Thresholds, feature_names: Iterable[str]):
        self.thresholds = thresholds
        self.features = {name: FeatureHealth(name) for name in feature_names}
        self.last_market_ts: Optional[float] = None
        self.last_latency_ms: float = 0.0
        self.spread_bps: float = 0.0
        self.order_accepts = 0
        self.order_rejects = 0
        self.pnl = 0.0
        self.peak_pnl = 0.0
        self.last_decision = Decision(action=Action.OK)

    def on_market(self, event: MarketEvent) -> None:
        if event.ask <= event.bid:
            self.last_decision = Decision(
                action=Action.HALT,
                reasons=["crossed_or_invalid_market"],
                metrics={"bid": event.bid, "ask": event.ask},
            )
            return

        mid = (event.bid + event.ask) / 2.0
        self.spread_bps = ((event.ask - event.bid) / mid) * 10_000.0
        self.last_market_ts = event.ts
        self.last_latency_ms = event.venue_latency_ms

        metrics: Dict[str, float] = {
            "spread_bps": self.spread_bps,
            "venue_latency_ms": self.last_latency_ms,
        }
        reasons: List[str] = []
        action = Action.OK

        for name, value in event.features.items():
            if name not in self.features:
                self.features[name] = FeatureHealth(name)
            scores = self.features[name].update(value, self.thresholds)
            metrics[f"{name}_z"] = scores["zscore"]
            metrics[f"{name}_psi"] = scores["psi"]

            if abs(scores["zscore"]) >= self.thresholds.zscore_halt:
                action = self._max_action(action, Action.HALT)
                reasons.append(f"feature_outlier:{name}:z={scores['zscore']:.2f}")
            elif abs(scores["zscore"]) >= self.thresholds.zscore_alert:
                action = self._max_action(action, Action.ALERT)
                reasons.append(f"feature_outlier:{name}:z={scores['zscore']:.2f}")

            if scores["psi"] >= self.thresholds.psi_halt:
                action = self._max_action(action, Action.SAFE_MODE)
                reasons.append(f"feature_drift:{name}:psi={scores['psi']:.3f}")
            elif scores["psi"] >= self.thresholds.psi_alert:
                action = self._max_action(action, Action.ALERT)
                reasons.append(f"feature_drift:{name}:psi={scores['psi']:.3f}")

        if self.spread_bps >= self.thresholds.max_spread_bps:
            action = self._max_action(action, Action.ALERT)
            reasons.append(f"spread_too_wide:{self.spread_bps:.2f}bps")

        if self.last_latency_ms >= self.thresholds.hard_latency_ms:
            action = self._max_action(action, Action.SAFE_MODE)
            reasons.append(f"latency_spike_hard:{self.last_latency_ms:.1f}ms")
        elif self.last_latency_ms >= self.thresholds.max_latency_ms:
            action = self._max_action(action, Action.ALERT)
            reasons.append(f"latency_spike:{self.last_latency_ms:.1f}ms")

        self.last_decision = Decision(action=action, reasons=reasons, metrics=metrics)

    def on_order(self, event: OrderEvent) -> None:
        if event.accepted:
            self.order_accepts += 1
        else:
            self.order_rejects += 1

    def on_pnl(self, event: PnLEvent) -> None:
        self.pnl = event.realized_pnl
        self.peak_pnl = max(self.peak_pnl, self.pnl)

    def evaluate(self, now_ts: float) -> Decision:
        action = self.last_decision.action
        reasons = list(self.last_decision.reasons)
        metrics = dict(self.last_decision.metrics)

        # Market data staleness.
        if self.last_market_ts is None:
            action = self._max_action(action, Action.SAFE_MODE)
            reasons.append("no_market_data")
        else:
            age = now_ts - self.last_market_ts
            metrics["market_data_age_s"] = age
            if age >= self.thresholds.hard_stale_market_seconds:
                action = self._max_action(action, Action.HALT)
                reasons.append(f"market_data_stale_hard:{age:.2f}s")
            elif age >= self.thresholds.stale_market_seconds:
                action = self._max_action(action, Action.SAFE_MODE)
                reasons.append(f"market_data_stale:{age:.2f}s")

        # Reject rate.
        total_orders = self.order_accepts + self.order_rejects
        reject_rate = (self.order_rejects / total_orders) if total_orders else 0.0
        metrics["reject_rate"] = reject_rate
        metrics["orders_total"] = float(total_orders)
        if reject_rate >= self.thresholds.hard_reject_rate and total_orders >= 20:
            action = self._max_action(action, Action.SAFE_MODE)
            reasons.append(f"reject_rate_hard:{reject_rate:.2%}")
        elif reject_rate >= self.thresholds.max_reject_rate and total_orders >= 20:
            action = self._max_action(action, Action.ALERT)
            reasons.append(f"reject_rate:{reject_rate:.2%}")

        # Drawdown from peak.
        drawdown = self.peak_pnl - self.pnl
        metrics["realized_pnl"] = self.pnl
        metrics["peak_pnl"] = self.peak_pnl
        metrics["drawdown"] = drawdown
        if drawdown >= self.thresholds.hard_drawdown:
            action = self._max_action(action, Action.HALT)
            reasons.append(f"drawdown_hard:{drawdown:.2f}")
        elif drawdown >= self.thresholds.max_drawdown:
            action = self._max_action(action, Action.SAFE_MODE)
            reasons.append(f"drawdown:{drawdown:.2f}")

        # Deduplicate while preserving order.
        deduped_reasons = list(dict.fromkeys(reasons))
        decision = Decision(action=action, reasons=deduped_reasons, metrics=metrics)
        self.last_decision = decision
        return decision

    @staticmethod
    def _max_action(a: Action, b: Action) -> Action:
        ordering = {
            Action.OK: 0,
            Action.ALERT: 1,
            Action.SAFE_MODE: 2,
            Action.HALT: 3,
        }
        return a if ordering[a] >= ordering[b] else b


class IncidentAgent:
    """
    Optional LLM/agent hook.

    Important: keep this advisory only. Deterministic controls above should remain
    the actual gating logic for regulatory and operational reasons.
    """

    def summarize(self, decision: Decision) -> str:
        core = ", ".join(decision.reasons[:5]) or "no clear issue"
        return (
            f"Action={decision.action}. "
            f"Primary signals: {core}. "
            f"Suggested next steps: verify market-data feed, venue latency, recent deployments, "
            f"and hedger/order gateway health."
        )


def demo_run(seed: int = 7) -> List[Decision]:
    random.seed(seed)
    thresholds = Thresholds()
    monitor = StrategyHealthMonitor(
        thresholds=thresholds,
        feature_names=["ret_1s", "spread_bps", "imbalance", "vol_10s"],
    )

    now = time.time()
    decisions: List[Decision] = []
    mid = 100.0
    pnl = 0.0

    # Warm-up reference regime.
    for i in range(2200):
        mid += random.gauss(0.0, 0.02)
        spread = max(0.002, abs(random.gauss(0.01, 0.002)))
        bid = mid - spread / 2
        ask = mid + spread / 2
        feat = {
            "ret_1s": random.gauss(0.0, 0.0008),
            "spread_bps": (spread / mid) * 10_000.0,
            "imbalance": random.gauss(0.0, 0.2),
            "vol_10s": max(0.0001, random.gauss(0.01, 0.002)),
        }
        latency_ms = max(1.0, random.gauss(8.0, 2.0))
        monitor.on_market(MarketEvent(ts=now + i * 0.1, bid=bid, ask=ask, features=feat, venue_latency_ms=latency_ms))
        monitor.on_order(OrderEvent(ts=now + i * 0.1, accepted=(random.random() > 0.02)))
        pnl += random.gauss(6.0, 12.0)
        monitor.on_pnl(PnLEvent(ts=now + i * 0.1, realized_pnl=pnl))

    # Stable evaluation.
    decisions.append(monitor.evaluate(now + 220.5))

    # Regime shift + latency + rejects + pnl drop.
    for i in range(180):
        mid += random.gauss(-0.08, 0.08)
        spread = max(0.02, abs(random.gauss(0.09, 0.03)))
        bid = mid - spread / 2
        ask = mid + spread / 2
        feat = {
            "ret_1s": random.gauss(-0.004, 0.003),
            "spread_bps": (spread / mid) * 10_000.0,
            "imbalance": random.gauss(-0.7, 0.25),
            "vol_10s": max(0.0001, random.gauss(0.06, 0.012)),
        }
        latency_ms = max(1.0, random.gauss(58.0, 12.0))
        ts = now + 221 + i * 0.2
        monitor.on_market(MarketEvent(ts=ts, bid=bid, ask=ask, features=feat, venue_latency_ms=latency_ms))
        monitor.on_order(OrderEvent(ts=ts, accepted=(random.random() > 0.25)))
        pnl += random.gauss(-320.0, 180.0)
        monitor.on_pnl(PnLEvent(ts=ts, realized_pnl=pnl))

    decisions.append(monitor.evaluate(now + 260.0))

    # Hard staleness.
    decisions.append(monitor.evaluate(now + 266.5))
    return decisions


if __name__ == "__main__":
    decisions = demo_run()
    agent = IncidentAgent()
    for idx, d in enumerate(decisions, start=1):
        print(f"--- decision {idx} ---")
        print(f"action: {d.action}")
        print("reasons:")
        for r in d.reasons[:8]:
            print(f"  - {r}")
        print("metrics:")
        for k in sorted(d.metrics)[:12]:
            print(f"  {k}: {d.metrics[k]}")
        print("agent_summary:")
        print(f"  {agent.summarize(d)}")
