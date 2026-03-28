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

import asyncio
import itertools
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from websockets.asyncio.server import serve as ws_serve
except ImportError:
    print("ERROR: 'websockets' package not found.\n"
          "Install it with:  pip install websockets", file=sys.stderr)
    sys.exit(1)

from exchange.orderbook import OrderBook
from exchange.quant_risk_control_plane import (
    Action,
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

STRESS_CHANCE    = 0.008   # probability per tick of triggering a stress event
STRESS_MIN_TICKS = 120     # ~2 s at 60 Hz
STRESS_MAX_TICKS = 420     # ~7 s
CALM_MIN_TICKS   = 240     # ~4 s mandatory calm after each stress


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
# Orderbook-driven simulator
# ─────────────────────────────────────────────────────────────────────────────
class OrderbookSimulator:
    """
    Drives the StrategyHealthMonitor with data produced by the real matching
    engine (OrderBook).

    Two synthetic participants maintain the book:
      - Market Maker ("mm"): places limit orders on both sides, cancels and
        reprices each tick.  In stress mode it widens the spread and thins
        depth, exactly as a real MM would in a volatile market.
      - Taker ("taker"): submits IOC orders that cross the spread, producing
        real trades and PnL.  In stress mode it becomes more aggressive and
        directionally biased (one-sided selling pressure).

    All MarketEvent, OrderEvent, and PnLEvent objects fed to the monitor are
    derived from actual OrderBook state, not from random numbers directly.
    """

    ASSET = "SYN"

    def __init__(self, seed: int = 42) -> None:
        self._rng  = random.Random(seed)
        self._oids = itertools.count(1)

        # ── matching engine ─────────────────────────────────────────────
        self.book     = OrderBook()
        self.mid      = 100.0
        self.prev_mid = 100.0
        self.pnl      = 0.0

        # Active maker order ids (so we can cancel/reprice them)
        self._mm_bids: list[int] = []
        self._mm_asks: list[int] = []

        # ── risk monitor ────────────────────────────────────────────────
        # Use only ret_1s and imbalance as PSI/z-score features.
        # The monitor already tracks spread_bps natively via on_market(), so
        # including it as a feature too creates a double-counted signal whose
        # bimodal distribution (seed spread vs refreshed spread) generates
        # spurious PSI alerts in normal operation.
        self.thresholds = Thresholds()
        self.monitor    = StrategyHealthMonitor(
            thresholds=self.thresholds,
            feature_names=["ret_1s", "imbalance"],
        )

        self.tick_count   = 0
        self.stress_until = -1
        self.calm_until   = -1

        print("Seeding orderbook and warming up reference window...", flush=True)
        self._seed_book()
        self._warmup()
        print("Warmup complete. Live simulation started.\n", flush=True)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _oid(self) -> int:
        return next(self._oids)

    def _seed_book(self) -> None:
        """Place an initial layer of resting limit orders to give the book depth."""
        r = self._rng
        for level in range(10):
            # Bid: 0.05 ticks below mid, then 0.05 wider each level
            pb = round(self.mid - 0.05 * (level + 1), 2)
            qty = max(1, int(r.gauss(20, 4)))
            oid = self._oid()
            self.book.submit_order(oid, "mm", "limit", "buy",  self.ASSET, qty, pb)
            self._mm_bids.append(oid)

            pa = round(self.mid + 0.05 * (level + 1), 2)
            qty = max(1, int(r.gauss(20, 4)))
            oid = self._oid()
            self.book.submit_order(oid, "mm", "limit", "sell", self.ASSET, qty, pa)
            self._mm_asks.append(oid)

    def _refresh_mm(self, in_stress: bool) -> None:
        """Cancel a fraction of maker orders and reprice the remainder."""
        r = self._rng
        cancel_rate = 0.55 if in_stress else 0.30

        keep_b, keep_a = [], []
        for oid in self._mm_bids:
            if r.random() < cancel_rate:
                self.book.cancel_order(oid)
            else:
                keep_b.append(oid)
        for oid in self._mm_asks:
            if r.random() < cancel_rate:
                self.book.cancel_order(oid)
            else:
                keep_a.append(oid)
        self._mm_bids, self._mm_asks = keep_b, keep_a

        # Place fresh orders
        if in_stress:
            half_spread = r.uniform(0.10, 0.30)
            n_levels    = r.randint(2, 4)
            qty_mu      = max(3, int(r.gauss(6, 2)))
        else:
            half_spread = r.uniform(0.03, 0.07)
            n_levels    = r.randint(4, 8)
            qty_mu      = max(5, int(r.gauss(18, 4)))

        for lvl in range(n_levels):
            pb  = round(self.mid - half_spread * (1 + lvl * 0.5), 2)
            qty = max(1, int(r.gauss(qty_mu, qty_mu * 0.3)))
            oid = self._oid()
            res = self.book.submit_order(oid, "mm", "limit", "buy",  self.ASSET, qty, pb)
            if res.status != "cancelled":
                self._mm_bids.append(oid)

            pa  = round(self.mid + half_spread * (1 + lvl * 0.5), 2)
            qty = max(1, int(r.gauss(qty_mu, qty_mu * 0.3)))
            oid = self._oid()
            res = self.book.submit_order(oid, "mm", "limit", "sell", self.ASSET, qty, pa)
            if res.status != "cancelled":
                self._mm_asks.append(oid)

    def _taker_hit(self, in_stress: bool) -> tuple[float, bool]:
        """Submit a taker IOC that crosses the spread. Returns (pnl_delta, accepted)."""
        r = self._rng

        if in_stress:
            if r.random() > 0.65:      # 65 % taker activity during stress
                return 0.0, True
            side = "sell" if r.random() < 0.70 else "buy"   # directional selling pressure
            qty  = max(1, int(r.gauss(14, 5)))
        else:
            if r.random() > 0.30:      # 30 % taker activity during normal regime
                return 0.0, True
            side = r.choice(["buy", "sell"])
            qty  = max(1, int(r.gauss(8, 3)))

        oid    = self._oid()
        result = self.book.submit_order(oid, "taker", "ioc", side, self.ASSET, qty)

        accepted  = result.status in ("filled", "partial")
        pnl_delta = sum(f.quantity * f.price * 0.0001 for f in result.fills)
        if side == "sell":
            pnl_delta = -pnl_delta

        return pnl_delta, accepted

    def _read_book(self) -> tuple[float, float, dict]:
        """
        Read live bid/ask/depth from the OrderBook and compute market features.
        If the book has been drained, re-seed it.
        """
        best_bid = self.book.get_best_bid(self.ASSET)
        best_ask = self.book.get_best_ask(self.ASSET)

        if best_bid is None or best_ask is None:
            self._seed_book()
            best_bid = self.book.get_best_bid(self.ASSET)
            best_ask = self.book.get_best_ask(self.ASSET)

        bid = best_bid[0] if best_bid else self.mid - 0.05
        ask = best_ask[0] if best_ask else self.mid + 0.05
        mid = (bid + ask) / 2.0

        depth   = self.book.get_market_depth(self.ASSET, levels=5)
        bid_vol = sum(v for _, v in depth["bids"])
        ask_vol = sum(v for _, v in depth["asks"])
        total   = bid_vol + ask_vol
        imbal   = (bid_vol - ask_vol) / max(total, 1)

        ret_1s = (mid - self.prev_mid) / max(self.prev_mid, 1e-6)

        self.prev_mid = mid
        self.mid      = mid

        return bid, ask, {
            "ret_1s":    ret_1s,
            "imbalance": imbal,
        }

    def _step(self, in_stress: bool, latency_ms: float, ts: float) -> None:
        """Run one full matching-engine tick and feed results to the monitor."""
        r = self._rng

        # Market maker reprices ~40% of ticks (100% during stress for faster spread widening)
        if in_stress or r.random() < 0.40:
            self._refresh_mm(in_stress)

        # Taker crosses the spread (produces real fills)
        pnl_delta, accepted = self._taker_hit(in_stress)
        self.pnl += pnl_delta

        # Read real orderbook state
        bid, ask, feat = self._read_book()

        # Feed into risk monitor
        self.monitor.on_market(MarketEvent(
            ts=ts, bid=bid, ask=ask, features=feat, venue_latency_ms=latency_ms,
        ))
        self.monitor.on_order(OrderEvent(ts=ts, accepted=accepted))
        self.monitor.on_pnl(PnLEvent(ts=ts, realized_pnl=self.pnl))

    def _warmup(self) -> None:
        """Synchronous warmup: 2200 ticks to fill the monitor's reference window."""
        now = time.time()
        r   = self._rng
        for i in range(2200):
            ts         = now + i * 0.1
            latency_ms = max(1.0, r.gauss(8.0, 2.0))
            self._step(in_stress=False, latency_ms=latency_ms, ts=ts)

    # ── public API ────────────────────────────────────────────────────────────

    def tick(self) -> dict:
        """Advance by one market-data tick; return state/reasons/raw metrics."""
        self.tick_count += 1
        now = time.time()
        r   = self._rng

        # Stress scenario management
        if (self.tick_count > self.calm_until
                and self.tick_count > self.stress_until
                and r.random() < STRESS_CHANCE):
            dur               = r.randint(STRESS_MIN_TICKS, STRESS_MAX_TICKS)
            self.stress_until = self.tick_count + dur
            self.calm_until   = self.stress_until + CALM_MIN_TICKS
            print(f"  [sim] Stress event: {dur} ticks ({dur / TICK_RATE:.1f}s)", flush=True)

        in_stress  = self.tick_count <= self.stress_until
        latency_ms = (max(1.0, r.gauss(58.0, 12.0)) if in_stress
                      else max(1.0, r.gauss(8.0,  2.0)))

        self._step(in_stress, latency_ms, now)

        decision = self.monitor.evaluate(now)
        m        = decision.metrics
        drift_z  = abs(m.get("spread_bps_z", m.get("ret_1s_z", 0.0)))

        return {
            "state":   decision.action.value,
            "reasons": decision.reasons[:5],
            "raw": {
                "spread_bps":   m.get("spread_bps",       10.0),
                "latency_ms":   m.get("venue_latency_ms",  8.0),
                "drift_zscore": drift_z,
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket server
# ─────────────────────────────────────────────────────────────────────────────
_clients: set = set()


async def _broadcast_loop() -> None:
    """60 Hz loop: step the matching engine, PID-smooth, broadcast."""

    sim = OrderbookSimulator()

    pid: dict[str, PIDController] = {
        # Kp=0.025 -> ~63% of a step reached in ~1 s at 60 Hz
        # Kd is subtracted (velocity form) -> dampens sharp spikes
        "spread_bps":   PIDController(0.025, 0.003, 0.00015, 0.0, 400.0, initial=10.0),
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
        if tick % MARKET_EVERY == 0:
            result  = sim.tick()
            state   = result["state"]
            reasons = result["reasons"]
            raw     = result["raw"]

        # PID-smooth every broadcast tick (60 Hz)
        smoothed = {k: pid[k].update(raw[k]) for k in raw}

        payload = json.dumps({
            "timestamp": time.time(),
            "state":     state,
            "reasons":   reasons,
            "smoothed_metrics": {
                "spread_bps":   round(smoothed["spread_bps"],   3),
                "latency_ms":   round(smoothed["latency_ms"],   3),
                "drift_zscore": round(smoothed["drift_zscore"], 4),
            },
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


async def _main() -> None:
    loop_task = asyncio.create_task(_broadcast_loop())
    async with ws_serve(_handler, HOST, PORT):
        print(f"+----------------------------------------------+")
        print(f"|    The Synesthetic Orderbook - Server        |")
        print(f"+----------------------------------------------+")
        print(f"|  WebSocket : ws://{HOST}:{PORT}              |")
        print(f"|  Tick rate : {TICK_RATE} Hz  | Market: {MARKET_RATE} Hz           |")
        print(f"|  Engine    : OrderBook + StrategyMonitor     |")
        print(f"|  Open client.html in your browser            |")
        print(f"+----------------------------------------------+\n")
        await loop_task


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
