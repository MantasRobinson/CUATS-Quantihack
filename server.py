"""
The Synesthetic Orderbook - WebSocket Server
============================================
Data flow (per PID spec):
  OrderBook (matching engine)
    -> real bid/ask/depth/fills
    -> StrategyHealthMonitor (risk control plane)
    -> PID smoothing
    -> WebSocket broadcast @ 60 Hz
    -> client.html (Tone.js audio + CSS visuals)

Usage
-----
    pip install websockets
    python server.py

Then open client.html in a browser.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import importlib
import json
import os
import queue
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from websockets.asyncio.server import serve as ws_serve
except ImportError:
    print("ERROR: 'websockets' package not found.\n"
          "Install it with:  pip install websockets", file=sys.stderr)
    sys.exit(1)

from agents.manipulator import ManipulatorAgent, ManipulatorConfig
from agents.market_maker import MarketMakerAgent, MarketMakerConfig
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig
from agents.simulation import Simulation, SimulationConfig
from btc_sim_config import BTC_ASSET, BTC_START_PRICE, build_btc_market_maker_config
from exchange.kafka_ingestion import iter_livecoinwatch_market_events
from exchange.quant_risk_control_plane import (
    MarketEvent,
    OrderEvent,
    PnLEvent,
    StrategyHealthMonitor,
    Thresholds,
)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
TICK_RATE    = 60          # Hz - WebSocket broadcast rate
TICK_DT      = 1.0 / TICK_RATE
MARKET_RATE  = 10          # market events per second (every 6th broadcast tick)
MARKET_EVERY = TICK_RATE // MARKET_RATE
HOST         = "localhost"
PORT         = 8765
LIVECOINWATCH_URL = "https://www.livecoinwatch.com/price/Bitcoin-BTC"

STRESS_CHANCE    = 0.005   # probability per tick of triggering a stress event
STRESS_MIN_TICKS = 90      # ~1.5 s at 60 Hz
STRESS_MAX_TICKS = 300     # ~5 s
CALM_MIN_TICKS   = 360     # ~6 s mandatory calm after each stress (longer recovery)


def build_agent_sim_thresholds() -> Thresholds:
    """Thresholds tuned for the built-in agent simulator.

    These are intentionally looser than the generic control-plane defaults:
    the local agent sim uses synthetic market structure and does not provide a
    strategy-level realized PnL series, so we avoid letting proxy PnL and brief
    spread shocks dominate the state machine.
    """
    return Thresholds(
        max_latency_ms=80.0,
        hard_latency_ms=200.0,
        max_drawdown=1e18,
        hard_drawdown=1e18,
        psi_alert=1.5,          # loose — only genuine regime shifts trigger ALERT
        psi_halt=4.0,           # very loose — only extreme distributional breaks
        min_reference_points=200,
        min_live_points=80,
        zscore_alert=8.0,
        zscore_halt=18.0,
        max_spread_bps=150.0,   # stress MM quotes ~40 bps, give headroom
    )


def build_livecoinwatch_thresholds() -> Thresholds:
    """Thresholds tuned for external Live Coin Watch polling.

    The live BTC feed is fetched over HTTP, but control-plane latency is now
    separated from external fetch RTT. That means the decision thresholds can
    stay close to exchange-like real-time market data systems.

    Because this source is polled rather than streamed, market-data staleness
    must remain looser than an exchange feed, but still tight enough to catch a
    broken connection quickly.
    """
    return Thresholds(
        stale_market_seconds=10.0,
        hard_stale_market_seconds=30.0,
        max_latency_ms=80.0,
        hard_latency_ms=200.0,
        max_reject_rate=0.08,
        hard_reject_rate=0.15,
        max_drawdown=1e18,
        hard_drawdown=1e18,
        psi_alert=1.5,
        psi_halt=4.0,
        min_reference_points=200,
        min_live_points=80,
        zscore_alert=8.0,
        zscore_halt=18.0,
        max_spread_bps=150.0,
    )


def build_agent_sim_stress_mm_config() -> MarketMakerConfig:
    return dataclasses.replace(
        build_btc_market_maker_config(),
        half_spread=90.0,
        quote_size=8,
        num_levels=2,
        level_spacing=35.0,
        annual_vol=1.20,
    )


def _kafka_api_version(default: tuple[int, ...] = (0, 10, 0)) -> tuple[int, ...]:
    raw = os.getenv("KAFKA_API_VERSION")
    if not raw:
        return default
    try:
        parts = tuple(int(p) for p in raw.split(".") if p != "")
        return parts if parts else default
    except ValueError:
        return default


def _kafka_preflight_hint(brokers: str, exc: Exception) -> str:
    api_version = ".".join(str(p) for p in _kafka_api_version())
    return (
        f"\n[kafka] Cannot reach broker at {brokers}:\n"
        f"  {exc}\n\n"
        f"Suggested checks:\n"
        f"  - verify KAFKA_BROKERS points at a running broker\n"
        f"  - set KAFKA_API_VERSION={api_version} if your broker is older/newer\n"
        f"  - if you do not need Kafka, run: python server.py --source agents\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# PID Controller
# ─────────────────────────────────────────────────────────────────────────────
class PIDController:
    """
    Velocity-form PID for audio-safe signal smoothing.

        velocity(t) = Kp * e(t) + Ki * integral(e) * dt  -  Kd * de/dt
        output(t+1) = clamp(output(t) + velocity(t))

    The subtracted Kd term reduces velocity when the error changes rapidly,
    preventing audio clipping on sudden orderbook sweeps.
    Anti-windup clamps the integral contribution.

    Tuning at 60 Hz:
      Kp ~ 0.025  ->  ~63% of a step change reached in ~1 s
      Ki ~ 0.003  ->  eliminates sustained drift
      Kd ~ 1.5e-4 ->  noticeably softens sharp spikes
    """

    def __init__(
        self, kp: float, ki: float, kd: float,
        out_min: float, out_max: float, initial: float = 0.0,
    ) -> None:
        self.kp, self.ki, self.kd = kp, ki, kd
        self.out_min, self.out_max = out_min, out_max
        self._output   = float(initial)
        self._integral = 0.0
        self._prev_err = 0.0

    def update(self, setpoint: float, dt: float = TICK_DT) -> float:
        err             = setpoint - self._output
        self._integral += err * dt
        if self.ki > 0:
            lim = (self.out_max - self.out_min) / self.ki
            self._integral = max(-lim, min(lim, self._integral))

        derivative     = (err - self._prev_err) / max(dt, 1e-9)
        self._prev_err = err

        velocity = self.kp * err + self.ki * self._integral * dt - self.kd * derivative
        self._output = max(self.out_min, min(self.out_max, self._output + velocity))
        return self._output


# ─────────────────────────────────────────────────────────────────────────────
# Agent-driven orderbook simulator
# ─────────────────────────────────────────────────────────────────────────────
class AgentOrderbookSimulator:
    """
    Drives StrategyHealthMonitor with data generated by the agent framework:
    one MarketMakerAgent + five RetailTraderAgents sharing an OrderBook.

    In normal regime the agents run with their baseline configs.
    In stress regime their configs are modified on-the-fly to simulate:
      - MM:     wider spread, fewer quoted levels, higher price drift
      - Retail: higher activity rate, stronger trend-chasing (panic/FOMO)
    When stress ends the original configs are restored.
    """

    ASSET = BTC_ASSET

    # Normal MM config — half_spread=0.05 -> ~10 bps spread on $100, well below
    # the 40 bps ALERT threshold so the monitor stays OK in calm conditions.
    _MM_NORMAL = build_btc_market_maker_config()
    # Stressed MM config — half_spread=0.20 -> ~40 bps, triggers ALERT as intended.
    _MM_STRESS = build_agent_sim_stress_mm_config()

    # Normal retail personalities (mirrors run_simulation.py)
    _RETAIL_NORMAL = [
        RetailTraderConfig(activity_rate=0.15, min_size=1, max_size=8,
                           trend_sensitivity=0.2, limit_order_pct=0.6, max_position=50),
        RetailTraderConfig(activity_rate=0.25, min_size=1, max_size=10,
                           trend_sensitivity=0.3, limit_order_pct=0.4, max_position=60),
        RetailTraderConfig(activity_rate=0.35, min_size=1, max_size=12,
                           trend_sensitivity=0.5, limit_order_pct=0.3, max_position=50),
        RetailTraderConfig(activity_rate=0.30, min_size=1, max_size=15,
                           trend_sensitivity=0.6, limit_order_pct=0.2, max_position=40),
        RetailTraderConfig(activity_rate=0.20, min_size=1, max_size=8,
                           trend_sensitivity=0.1, limit_order_pct=0.5, max_position=50),
    ]
    # Stressed retail (panic / FOMO: more active, strongly trend-chasing)
    _RETAIL_STRESS = [
        RetailTraderConfig(activity_rate=0.55, min_size=1, max_size=8,
                           trend_sensitivity=0.85, limit_order_pct=0.1, max_position=50),
        RetailTraderConfig(activity_rate=0.65, min_size=1, max_size=10,
                           trend_sensitivity=0.90, limit_order_pct=0.1, max_position=60),
        RetailTraderConfig(activity_rate=0.75, min_size=1, max_size=12,
                           trend_sensitivity=0.90, limit_order_pct=0.1, max_position=50),
        RetailTraderConfig(activity_rate=0.70, min_size=1, max_size=15,
                           trend_sensitivity=0.95, limit_order_pct=0.0, max_position=40),
        RetailTraderConfig(activity_rate=0.60, min_size=1, max_size=8,
                           trend_sensitivity=0.80, limit_order_pct=0.1, max_position=50),
    ]

    def __init__(
        self, seed: int = 42,
        num_mm: int = 1, num_retail: int = 5, num_manip: int = 1,
    ) -> None:
        self._rng = random.Random(seed)

        # ── agent simulation ─────────────────────────────────────────────
        self._sim = Simulation(SimulationConfig(asset=self.ASSET, seed=seed))

        # Market makers — single MM keeps name "MM" for backward compat
        self._mm_agents: list[MarketMakerAgent] = []
        for i in range(max(num_mm, 0)):
            name = "MM" if num_mm == 1 else f"MM-{i}"
            mm = MarketMakerAgent(
                name, self.ASSET, self._sim.ob, self._sim.id_generator,
                config=dataclasses.replace(self._MM_NORMAL),
            )
            self._sim.add_agent(mm)
            self._mm_agents.append(mm)
        # Convenience reference for PnL / backward-compat fields
        self._mm = self._mm_agents[0] if self._mm_agents else None

        # Retail traders — cycle through personality presets if more than 5 requested
        self._retail: list[RetailTraderAgent] = []
        for i in range(max(num_retail, 0)):
            cfg = self._RETAIL_NORMAL[i % len(self._RETAIL_NORMAL)]
            rt = RetailTraderAgent(
                f"RT-{i}", self.ASSET, self._sim.ob, self._sim.id_generator,
                config=dataclasses.replace(cfg),
                rng_seed=seed + i + 1,
            )
            self._sim.add_agent(rt)
            self._retail.append(rt)

        # Manipulators
        self._manip: list[ManipulatorAgent] = []
        for i in range(max(num_manip, 0)):
            name = "MANIP" if num_manip == 1 else f"MANIP-{i}"
            ma = ManipulatorAgent(
                name, self.ASSET, self._sim.ob, self._sim.id_generator,
                rng_seed=seed + 1000 + i,
            )
            self._sim.add_agent(ma)
            self._manip.append(ma)

        # ── risk monitor ─────────────────────────────────────────────────
        self.thresholds = build_agent_sim_thresholds()
        self.monitor    = StrategyHealthMonitor(
            thresholds=self.thresholds,
            feature_names=["ret_1s", "imbalance"],
        )

        self.tick_count   = 0
        self.step_count   = 0   # simulation step index passed to agents
        self.stress_until = -1
        self.calm_until   = -1
        self.prev_mid     = BTC_START_PRICE
        self._in_stress   = False
        self._halt_active = False   # True while monitor state == HALT
        self._halt_cooldown = 0     # ticks remaining before HALT can re-trigger

        print("Warming up agent-driven orderbook (2200 steps)...", flush=True)
        self._warmup()
        print("Warmup complete. Live simulation started.\n", flush=True)

    # ── internal helpers ──────────────────────────────────────────────────────

    def _run_agents(self) -> None:
        """One full simulation step: all agents act, then reconcile passive fills."""
        agents = list(self._sim.agents)
        self._rng.shuffle(agents)
        for agent in agents:
            agent.step(self.step_count)
        for agent in agents:
            agent.reconcile()
        self.step_count += 1

    def _set_stress(self, in_stress: bool) -> None:
        """Switch all agent configs between normal and stressed presets."""
        if in_stress == self._in_stress:
            return
        self._in_stress = in_stress
        mm_cfg = self._MM_STRESS if in_stress else self._MM_NORMAL
        for mm in self._mm_agents:
            for field in dataclasses.fields(mm_cfg):
                setattr(mm.config, field.name, getattr(mm_cfg, field.name))
        retail_cfgs = self._RETAIL_STRESS if in_stress else self._RETAIL_NORMAL
        for i, rt in enumerate(self._retail):
            cfg_preset = retail_cfgs[i % len(retail_cfgs)]
            for field in dataclasses.fields(cfg_preset):
                setattr(rt.config, field.name, getattr(cfg_preset, field.name))

    def _read_book(self) -> tuple[float, float, dict]:
        ob       = self._sim.ob
        best_bid = ob.get_best_bid(self.ASSET)
        best_ask = ob.get_best_ask(self.ASSET)

        # Fallback proportional to mid so spread_bps stays ~20 bps when book is empty.
        # Always guarantee bid < ask to avoid triggering crossed-market HALT.
        bid = best_bid[0] if best_bid else self.prev_mid * 0.9990
        ask = best_ask[0] if best_ask else self.prev_mid * 1.0010
        if bid >= ask:
            mid_est = (bid + ask) / 2.0
            bid = mid_est * 0.9990
            ask = mid_est * 1.0010
        mid = (bid + ask) / 2.0

        depth   = ob.get_market_depth(self.ASSET, levels=5)
        bid_vol = sum(v for _, v in depth["bids"])
        ask_vol = sum(v for _, v in depth["asks"])
        imbal   = (bid_vol - ask_vol) / max(bid_vol + ask_vol, 1)

        ret_1s    = (mid - self.prev_mid) / max(self.prev_mid, 1e-6)
        self.prev_mid = mid
        return bid, ask, {"ret_1s": ret_1s, "imbalance": imbal}

    def _warmup(self) -> None:
        now = time.time()
        for i in range(2200):
            self._run_agents()
            bid, ask, feat = self._read_book()
            ts         = now + i * 0.1
            latency_ms = max(1.0, self._rng.gauss(8.0, 2.0))
            self.monitor.on_market(MarketEvent(
                ts=ts, bid=bid, ask=ask, features=feat, venue_latency_ms=latency_ms,
            ))
            self.monitor.on_order(OrderEvent(ts=ts, accepted=True))
            mm_pnl = self._mm.state.unrealized_pnl(self.prev_mid) if self._mm else 0.0
            self.monitor.on_pnl(PnLEvent(ts=ts, realized_pnl=mm_pnl))

    # ── public API ────────────────────────────────────────────────────────────

    def debug_info(self) -> dict:
        """Snapshot of raw engine state for the debug visualiser."""
        ob  = self._sim.ob
        bb  = ob.get_best_bid(self.ASSET)
        ba  = ob.get_best_ask(self.ASSET)
        bid = bb[0] if bb else None
        ask = ba[0] if ba else None
        mid = (bid + ask) / 2.0 if (bid and ask) else self.prev_mid

        recent = ob.get_trade_history(self.ASSET, limit=10)
        trades = [{"price": t.price, "qty": t.quantity} for t in recent]

        depth  = ob.get_market_depth(self.ASSET, levels=5)

        all_agents = (
            [(mm.name, mm.state) for mm in self._mm_agents] +
            [(rt.name, rt.state) for rt in self._retail] +
            [(ma.name, ma.state) for ma in self._manip]
        )
        return {
            "bid":              round(bid, 3) if bid else None,
            "ask":              round(ask, 3) if ask else None,
            "mid":              round(mid, 3),
            "total_trades":     len(ob._trades),
            "mm_position":      self._mm.state.position if self._mm else 0,
            "mm_pnl":           round(self._mm.state.unrealized_pnl(mid), 2) if self._mm else 0.0,
            "retail_positions": [rt.state.position for rt in self._retail],
            "manip_positions":  [ma.state.position for ma in self._manip],
            "agent_pnls":       [{"name": n, "pnl": round(s.unrealized_pnl(mid), 2)}
                                 for n, s in all_agents],
            "recent_trades":    trades,
            "depth_bids":       [[p, v] for p, v in depth["bids"]],
            "depth_asks":       [[p, v] for p, v in depth["asks"]],
        }

    def tick(self) -> dict:
        """Advance one market-data tick; return state/reasons/raw metrics."""
        self.tick_count += 1
        now = time.time()

        # Stress scenario management
        if (self.tick_count > self.calm_until
                and self.tick_count > self.stress_until
                and self._rng.random() < STRESS_CHANCE):
            dur               = self._rng.randint(STRESS_MIN_TICKS, STRESS_MAX_TICKS)
            self.stress_until = self.tick_count + dur
            self.calm_until   = self.stress_until + CALM_MIN_TICKS
            print(f"  [sim] Stress event: {dur} ticks ({dur / TICK_RATE:.1f}s)", flush=True)

        in_stress = self.tick_count <= self.stress_until
        self._set_stress(in_stress)

        latency_ms = (max(1.0, self._rng.gauss(58.0, 12.0)) if in_stress
                      else max(1.0, self._rng.gauss(8.0,  2.0)))

        if self._halt_active:
            # Trading is halted — allow passive fills to settle but block new orders
            for agent in self._sim.agents:
                agent.reconcile()
            self.step_count += 1
        else:
            self._run_agents()

        bid, ask, feat = self._read_book()

        self.monitor.on_market(MarketEvent(
            ts=now, bid=bid, ask=ask, features=feat, venue_latency_ms=latency_ms,
        ))
        self.monitor.on_order(OrderEvent(ts=now, accepted=True))
        mm_pnl = self._mm.state.unrealized_pnl(self.prev_mid) if self._mm else 0.0
        self.monitor.on_pnl(PnLEvent(ts=now, realized_pnl=mm_pnl))

        decision  = self.monitor.evaluate(now)
        new_halt  = decision.action.value == "HALT"

        # Suppress re-HALT during cooldown so agents can re-establish quotes
        if self._halt_cooldown > 0:
            self._halt_cooldown -= 1
            new_halt = False

        if new_halt and not self._halt_active:
            # Transition INTO halt — cancel every resting order immediately
            for agent in self._sim.agents:
                agent.cancel_all_orders()
            print("  [halt] HALT triggered — all resting orders cancelled, trading suspended.",
                  flush=True)
        elif not new_halt and self._halt_active:
            self._halt_cooldown = 120  # ~12 s at 10 Hz — give agents time to rebuild quotes
            print("  [halt] HALT cleared — trading resumed (60-step cooldown).", flush=True)

        self._halt_active = new_halt

        m       = decision.metrics
        drift_z = abs(m.get("ret_1s_z", m.get("imbalance_z", 0.0)))

        return {
            "state":   decision.action.value,
            "reasons": decision.reasons[:5],
            "raw": {
                "spread_bps":   m.get("spread_bps",       10.0),
                "latency_ms":   m.get("venue_latency_ms",  8.0),
                "drift_zscore": drift_z,
            },
            "debug": self.debug_info(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Kafka data source
# ─────────────────────────────────────────────────────────────────────────────
class KafkaSource:
    """
    Drives StrategyHealthMonitor with live events from a Kafka/Redpanda topic.

    A background thread consumes the Kafka stream and pushes parsed events into
    a queue; tick() drains the queue, feeds the monitor, and returns the same
    dict shape as AgentOrderbookSimulator.tick() so the broadcast loop is
    source-agnostic.

    Configure via environment variables (same as kafka_ingestion.py):
        KAFKA_BROKERS   default localhost:9092
        KAFKA_TOPIC     default market-events
        KAFKA_GROUP_ID  default qrm-server
    """

    def __init__(self) -> None:
        from exchange.kafka_ingestion import create_consumer, iter_events

        self.thresholds = Thresholds()
        self.monitor    = StrategyHealthMonitor(
            thresholds=self.thresholds,
            feature_names=["ret_1s", "imbalance"],
        )

        self._queue: queue.Queue = queue.Queue()
        self._last_bid   = 100.0
        self._last_ask   = 100.02
        self._last_lat   = 8.0
        self._total_trades = 0

        brokers  = os.getenv("KAFKA_BROKERS",  "localhost:9092")
        topic    = os.getenv("KAFKA_TOPIC",    "market-events")
        group_id = os.getenv("KAFKA_GROUP_ID", "qrm-server")

        # Preflight: check the broker is reachable before starting the server.
        print(f"[kafka] Checking broker at {brokers} …", flush=True)
        try:
            kafka_mod = importlib.import_module("kafka")
            probe = kafka_mod.KafkaAdminClient(
                bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
                request_timeout_ms=4000,
                api_version=_kafka_api_version(),
            )
            probe.close()
            print(f"[kafka] Broker reachable.", flush=True)
        except Exception as exc:
            print(_kafka_preflight_hint(brokers, exc), file=sys.stderr)
            sys.exit(1)

        def _consume():
            retry_delay = 2.0
            while True:
                try:
                    consumer = create_consumer(brokers, topic, group_id,
                                               poll_timeout_ms=200)
                    print(f"[kafka] Consuming from {topic}", flush=True)
                    retry_delay = 2.0
                    for event_type, event in iter_events(consumer):
                        self._queue.put((event_type, event))
                except Exception as exc:
                    print(f"[kafka] Lost connection ({exc}). Retrying in {retry_delay:.0f}s…",
                          flush=True)
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30.0)

        t = threading.Thread(target=_consume, daemon=True, name="kafka-consumer")
        t.start()

    def tick(self) -> dict:
        now = time.time()

        # Drain all events that arrived since the last tick
        while True:
            try:
                event_type, event = self._queue.get_nowait()
            except queue.Empty:
                break

            if event_type == "market":
                self._last_bid = event.bid
                self._last_ask = event.ask
                self._last_lat = event.venue_latency_ms
                self.monitor.on_market(event)
            elif event_type == "order":
                self.monitor.on_order(event)
            elif event_type == "pnl":
                self.monitor.on_pnl(event)
                self._total_trades += 1

        decision = self.monitor.evaluate(now)
        m        = decision.metrics
        drift_z  = abs(m.get("ret_1s_z", m.get("imbalance_z", 0.0)))
        mid      = (self._last_bid + self._last_ask) / 2.0

        return {
            "state":   decision.action.value,
            "reasons": decision.reasons[:5],
            "state_driver": decision.reasons[0] if decision.reasons else "ok",
            "decision_metrics": {
                "spread_bps":       m.get("spread_bps"),
                "venue_latency_ms": m.get("venue_latency_ms"),
                "market_data_age_s": m.get("market_data_age_s"),
                "reject_rate":      m.get("reject_rate"),
                "drawdown":         m.get("drawdown"),
                "ret_1s_z":         m.get("ret_1s_z"),
                "ret_1s_psi":       m.get("ret_1s_psi"),
                "imbalance_z":      m.get("imbalance_z"),
                "imbalance_psi":    m.get("imbalance_psi"),
            },
            "raw": {
                "spread_bps":   m.get("spread_bps",       10.0),
                "latency_ms":   m.get("venue_latency_ms",  self._last_lat),
                "drift_zscore": drift_z,
            },
            "debug": {
                "bid":              round(self._last_bid, 3),
                "ask":              round(self._last_ask, 3),
                "mid":              round(mid, 3),
                "total_trades":     self._total_trades,
                "mm_position":      None,
                "mm_pnl":           None,
                "retail_positions": [],
                "recent_trades":    [],
                "depth_bids":       [],
                "depth_asks":       [],
            },
        }


class LiveCoinWatchSource:
    """Poll Live Coin Watch for BTC/USD and expose it as market data."""

    def __init__(self) -> None:
        self.thresholds = build_livecoinwatch_thresholds()
        self.monitor    = StrategyHealthMonitor(
            thresholds=self.thresholds,
            feature_names=["ret_1s", "imbalance"],
        )

        self._url = os.getenv("LIVECOINWATCH_URL", LIVECOINWATCH_URL)
        self._poll_interval_s = float(os.getenv("LIVECOINWATCH_POLL_INTERVAL_S", "5.0"))
        self._synthetic_spread_bps = float(os.getenv("LIVECOINWATCH_SPREAD_BPS", "10.0"))

        self._last_price: float | None = None
        self._last_bid = 0.0
        self._last_ask = 0.0
        self._last_latency = 100.0
        self._last_latency_ema = 100.0
        self._control_latency_ms = 8.0
        self._last_fetch = 0.0
        self._total_samples = 0
        self._queue: queue.Queue = queue.Queue()

        print(f"[livecoinwatch] Polling {self._url}", flush=True)

        def _consume():
            retry_delay = 2.0
            while True:
                try:
                    for payload in iter_livecoinwatch_market_events(
                        self._url,
                        synthetic_spread_bps=self._synthetic_spread_bps,
                        poll_interval_s=self._poll_interval_s,
                    ):
                        self._queue.put(payload)
                    retry_delay = 2.0
                except Exception as exc:
                    print(f"[livecoinwatch] Stream error ({exc}). Retrying in {retry_delay:.0f}s…",
                          flush=True)
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30.0)

        t = threading.Thread(target=_consume, daemon=True, name="livecoinwatch-consumer")
        t.start()

    def tick(self) -> dict:
        now = time.time()

        while True:
            try:
                event = self._queue.get_nowait()
            except queue.Empty:
                break

            self._last_bid = event["bid"]
            self._last_ask = event["ask"]
            self._last_latency = event["venue_latency_ms"]
            self._last_latency_ema = (
                self._last_latency
                if self._total_samples == 0
                else (0.8 * self._last_latency_ema + 0.2 * self._last_latency)
            )
            self._last_fetch = event["ts"]
            self._total_samples += 1
            self._last_price = event.get("price", self._last_price)
            self.monitor.on_market(MarketEvent(
                ts=event["ts"],
                bid=event["bid"],
                ask=event["ask"],
                features=event.get("features", {}),
                venue_latency_ms=self._control_latency_ms,
            ))

        decision = self.monitor.evaluate(now)
        m        = decision.metrics
        drift_z  = abs(m.get("ret_1s_z", m.get("imbalance_z", 0.0)))
        mid      = (self._last_bid + self._last_ask) / 2.0 if self._last_price is not None else 0.0

        return {
            "state":   decision.action.value,
            "reasons": decision.reasons[:5],
            "state_driver": decision.reasons[0] if decision.reasons else "ok",
            "decision_metrics": {
                "spread_bps":       m.get("spread_bps"),
                "venue_latency_ms": m.get("venue_latency_ms"),
                "market_data_age_s": m.get("market_data_age_s"),
                "reject_rate":      m.get("reject_rate"),
                "drawdown":         m.get("drawdown"),
                "ret_1s_z":         m.get("ret_1s_z"),
                "ret_1s_psi":       m.get("ret_1s_psi"),
                "imbalance_z":      m.get("imbalance_z"),
                "imbalance_psi":    m.get("imbalance_psi"),
            },
            "raw": {
                "spread_bps":   m.get("spread_bps",       self._synthetic_spread_bps),
                "latency_ms":   m.get("venue_latency_ms",  self._control_latency_ms),
                "drift_zscore": drift_z,
            },
            "debug": {
                "bid":              round(self._last_bid, 3) if self._last_price is not None else None,
                "ask":              round(self._last_ask, 3) if self._last_price is not None else None,
                "mid":              round(mid, 3) if self._last_price is not None else None,
                "total_trades":     self._total_samples,
                "latency_raw_ms":   round(self._last_latency, 3),
                "latency_ema_ms":   round(self._last_latency_ema, 3),
                "latency_control_ms": round(self._control_latency_ms, 3),
                "mm_position":      None,
                "mm_pnl":           None,
                "retail_positions": [],
                "recent_trades":    [],
                "depth_bids":       [],
                "depth_asks":       [],
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket server
# ─────────────────────────────────────────────────────────────────────────────
_clients: set = set()


async def _broadcast_loop(source, source_label: str) -> None:
    """60 Hz loop: step the matching engine, PID-smooth, broadcast."""

    pid: dict[str, PIDController] = {
        # Kp=0.025 -> ~63% of a step reached in ~1 s at 60 Hz
        # Kd is subtracted (velocity form) -> dampens sharp spikes
        "spread_bps":   PIDController(0.025, 0.003, 0.00015, 0.0, 100.0, initial=10.0),
        "latency_ms":   PIDController(0.025, 0.003, 0.00015, 0.0, 250.0, initial=8.0),
        "drift_zscore": PIDController(0.030, 0.002, 0.00010, 0.0,  20.0, initial=0.0),
    }

    tick    = 0
    raw     = {"spread_bps": 10.0, "latency_ms": 8.0, "drift_zscore": 0.0}
    state   = "OK"
    reasons: list[str] = []

    while True:
        t0 = time.monotonic()

        # Step the matching engine every MARKET_EVERY-th broadcast tick
        debug: dict = {}
        if tick % MARKET_EVERY == 0:
            result  = source.tick()
            state   = result["state"]
            reasons = result["reasons"]
            raw     = result["raw"]
            debug   = result["debug"]

        # PID-smooth every broadcast tick (60 Hz)
        smoothed = {k: pid[k].update(raw[k]) for k in raw}

        payload = json.dumps({
            "timestamp": time.time(),
            "source":    source_label,
            "state":     state,
            "reasons":   reasons,
            "smoothed_metrics": {
                "spread_bps":   round(smoothed["spread_bps"],   3),
                "latency_ms":   round(smoothed["latency_ms"],   3),
                "drift_zscore": round(smoothed["drift_zscore"], 4),
            },
            "debug": debug,
        })

        if _clients:
            await asyncio.gather(
                *[ws.send(payload) for ws in list(_clients)],
                return_exceptions=True,
            )

        tick += 1
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0.0, TICK_DT - elapsed))


async def _handler(websocket) -> None:
    _clients.add(websocket)
    print(f"[ws] Client connected  - {len(_clients)} total", flush=True)
    try:
        await websocket.wait_closed()
    finally:
        _clients.discard(websocket)
        print(f"[ws] Client disconnected - {len(_clients)} remaining", flush=True)


async def _main(source, src_label: str) -> None:
    loop_task = asyncio.create_task(_broadcast_loop(source, src_label))
    async with ws_serve(_handler, HOST, PORT):
        print(f"+----------------------------------------------+")
        print(f"|    The Synesthetic Orderbook - Server        |")
        print(f"+----------------------------------------------+")
        print(f"|  WebSocket : ws://{HOST}:{PORT}              |")
        print(f"|  Tick rate : {TICK_RATE} Hz  | Market: {MARKET_RATE} Hz           |")
        print(f"|  Source    : {src_label:<29}|")
        print(f"|  Open client.html in your browser            |")
        print(f"+----------------------------------------------+\n")
        await loop_task


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="The Synesthetic Orderbook server")
    parser.add_argument(
           "--source", choices=["agents", "kafka", "livecoinwatch"], default="agents",
           help="Data source (default: agents). 'kafka' reads from a Kafka/Redpanda "
               "topic; 'livecoinwatch' polls the BTC price page.",
    )
    parser.add_argument(
        "--mm", type=int, default=1, metavar="N",
        help="Number of market-maker agents (default: 1, agents source only)",
    )
    parser.add_argument(
        "--retail", type=int, default=5, metavar="N",
        help="Number of retail-trader agents (default: 5, agents source only)",
    )
    parser.add_argument(
        "--manip", type=int, default=1, metavar="N",
        help="Number of market-manipulator agents (default: 1, agents source only)",
    )
    args = parser.parse_args()

    if args.source == "kafka":
        source = KafkaSource()
        src_label = "Kafka"
    elif args.source == "livecoinwatch":
        source = LiveCoinWatchSource()
        src_label = "LiveCoinWatch BTC"
    else:
        source = AgentOrderbookSimulator(
            num_mm=args.mm, num_retail=args.retail, num_manip=args.manip,
        )
        src_label = (f"{args.mm} MM  {args.retail} Retail  {args.manip} Manip")

    try:
        asyncio.run(_main(source, src_label))
    except KeyboardInterrupt:
        print("\nServer stopped.")
