"""Live dashboard — anchors the simulation to a live BTC feed when available.

Usage:
    python live_dashboard.py                         # 1 MM, 5 retail, 1 manip
    python live_dashboard.py --mm 1 --retail 3 --manip 1
    python live_dashboard.py --market-source simulated

Press Q or close the window to stop.

By default the dashboard polls Live Coin Watch and uses the live BTC price
as the anchor for the market maker, so the other agents trade around the real
price path rather than a purely synthetic one.
"""

from __future__ import annotations

import argparse
import threading
import sys
import traceback

# Fix DPI scaling on Windows HiDPI displays
import ctypes
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(0)
except Exception:
    pass

import matplotlib
matplotlib.use("TkAgg")
matplotlib.rcParams["figure.dpi"] = 80

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.gridspec as gridspec
import numpy as np
from collections import deque

from agents.simulation import Simulation, SimulationConfig
from agents.market_maker import MarketMakerAgent
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig
from agents.manipulator import ManipulatorAgent, ManipulatorConfig
from exchange.kafka_ingestion import iter_livecoinwatch_market_events
from btc_sim_config import BTC_ASSET, build_btc_market_maker_config


# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────

WINDOW          = 600    # steps of history visible on screen
STEPS_PER_FRAME = 3      # sim steps computed per animation frame
FRAME_MS        = 80     # milliseconds between frames (~12 fps)
LIVECOINWATCH_POLL_INTERVAL_S = 5.0
LIVECOINWATCH_SYNTHETIC_SPREAD_BPS = 10.0

AGENT_COLORS = {
    "MM":    "#E91E63",
    "RT-0":  "#9C27B0",
    "RT-1":  "#3F51B5",
    "RT-2":  "#009688",
    "RT-3":  "#FF9800",
    "RT-4":  "#795548",
    "MANIP": "#FFD600",
}

_RETAIL_CFGS = [
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


class LiveCoinWatchFeed:
    """Background poller for Live Coin Watch BTC snapshots."""

    def __init__(self, poll_interval_s: float = LIVECOINWATCH_POLL_INTERVAL_S):
        self.poll_interval_s = poll_interval_s
        self._lock = threading.Lock()
        self._snapshot: dict[str, object] | None = None
        self._error: str | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            for snapshot in iter_livecoinwatch_market_events(
                poll_interval_s=self.poll_interval_s,
                synthetic_spread_bps=LIVECOINWATCH_SYNTHETIC_SPREAD_BPS,
            ):
                with self._lock:
                    self._snapshot = snapshot
                    self._error = None
        except Exception as exc:  # pragma: no cover - background thread guard
            with self._lock:
                self._error = str(exc)

    def latest(self) -> dict[str, object] | None:
        with self._lock:
            if self._snapshot is None:
                return None
            return dict(self._snapshot)

    def error(self) -> str | None:
        with self._lock:
            return self._error


# ─────────────────────────────────────────────────────────────────────
# Simulation setup
# ─────────────────────────────────────────────────────────────────────

def build_simulation(num_mm: int = 1, num_retail: int = 5, num_manip: int = 1):
    sim_cfg = SimulationConfig(num_steps=999_999, asset=BTC_ASSET, seed=42)
    sim = Simulation(sim_cfg)

    mm_agents = []
    for i in range(max(num_mm, 0)):
        name = "MM" if num_mm == 1 else f"MM-{i}"
        mm = MarketMakerAgent(
            name=name, asset=sim.asset, orderbook=sim.ob,
            id_generator=sim.id_generator,
            config=build_btc_market_maker_config(),
        )
        sim.add_agent(mm)
        mm_agents.append(mm)

    for i in range(max(num_retail, 0)):
        cfg = _RETAIL_CFGS[i % len(_RETAIL_CFGS)]
        sim.add_agent(RetailTraderAgent(
            name=f"RT-{i}", asset=sim.asset, orderbook=sim.ob,
            id_generator=sim.id_generator, config=cfg,
            rng_seed=sim_cfg.seed + i + 1,
        ))

    for i in range(max(num_manip, 0)):
        name = "MANIP" if num_manip == 1 else f"MANIP-{i}"
        sim.add_agent(ManipulatorAgent(
            name=name, asset=sim.asset, orderbook=sim.ob,
            id_generator=sim.id_generator,
            rng_seed=sim_cfg.seed + 1000 + i,
        ))

    primary_mm = mm_agents[0] if mm_agents else None
    return sim, primary_mm


# ─────────────────────────────────────────────────────────────────────
# Dashboard class
# ─────────────────────────────────────────────────────────────────────

class LiveDashboard:

    def __init__(self, sim: Simulation, mm: MarketMakerAgent | None, live_feed: LiveCoinWatchFeed | None = None):
        self.sim = sim
        self.mm  = mm
        self.live_feed = live_feed
        self.step = 0
        self.prev_trade_count = 0

        # Rolling buffers (one per agent for positions)
        self.steps      = deque(maxlen=WINDOW)
        self.mids       = deque(maxlen=WINDOW)
        self.bids       = deque(maxlen=WINDOW)
        self.asks       = deque(maxlen=WINDOW)
        self.spreads    = deque(maxlen=WINDOW)
        self.live_mids  = deque(maxlen=WINDOW)
        self.live_bids  = deque(maxlen=WINDOW)
        self.live_asks  = deque(maxlen=WINDOW)
        self.live_latency_ms = deque(maxlen=WINDOW)
        self.trade_rate = deque(maxlen=WINDOW)
        self.positions  = {a.name: deque(maxlen=WINDOW) for a in sim.agents}
        self._depth_volume_floor = self._estimate_depth_volume_floor()

        self._build_figure()

    def _estimate_depth_volume_floor(self) -> int:
        """Use the sim's configured order sizes to keep depth scaling readable."""
        sizes = []

        mm_cfg = getattr(self.mm, "config", None)
        if mm_cfg is not None:
            sizes.append(getattr(mm_cfg, "quote_size", 0))

        manip = next((a for a in self.sim.agents if type(a).__name__ == 'ManipulatorAgent'), None)
        manip_cfg = getattr(manip, "config", None)
        if manip_cfg is not None:
            sizes.extend([
                getattr(manip_cfg, "spoof_size", 0),
                getattr(manip_cfg, "pump_size", 0),
                getattr(manip_cfg, "dump_size", 0),
                getattr(manip_cfg, "wash_size", 0),
            ])

        for agent in self.sim.agents:
            cfg = getattr(agent, "config", None)
            if cfg is None:
                continue
            sizes.append(getattr(cfg, "quote_size", 0))
            sizes.append(getattr(cfg, "max_size", 0))

        return max(max(sizes, default=1), 1)

    @staticmethod
    def _depth_bar_width(prices: list[float]) -> float:
        """Scale bar width from the visible price ladder instead of a fixed toy value."""
        if len(prices) >= 2:
            gaps = [
                abs(b - a)
                for a, b in zip(sorted(prices), sorted(prices)[1:])
                if abs(b - a) > 0
            ]
            if gaps:
                return max(min(gaps) * 0.75, 0.5)
        return 10.0

    # ── Figure construction ──────────────────────────────────────────

    def _build_figure(self):
        plt.style.use("dark_background")

        # Small figsize — let the window manager scale it up
        self.fig = plt.figure(figsize=(14, 8))
        self.fig.patch.set_facecolor("#0D1117")

        # Maximize the window immediately so it fills the screen
        try:
            mgr = plt.get_current_fig_manager()
            mgr.window.state("zoomed")
        except Exception:
            try:
                plt.get_current_fig_manager().full_screen_toggle()
            except Exception:
                pass

        agent_summary = ", ".join(a.name for a in self.sim.agents)
        self.fig.suptitle(
            f"CUATS Live Market Dashboard  [{agent_summary}]",
            fontsize=14, fontweight="bold", color="#00E5FF", y=0.97,
        )

        gs = gridspec.GridSpec(3, 2, hspace=0.50, wspace=0.30,
                               left=0.07, right=0.96, top=0.91, bottom=0.06)

        # — Price (full width) —
        self.ax_price = self.fig.add_subplot(gs[0, :])
        self.ax_price.set_title("Price", fontsize=12, color="#90CAF9", pad=8)
        self.ax_price.set_ylabel("Price", fontsize=9)
        self.ax_price.tick_params(labelsize=8)
        self.ln_mid, = self.ax_price.plot([], [], color="#2196F3", lw=1.4, label="Mid")
        self.ln_bid, = self.ax_price.plot([], [], color="#4CAF50", lw=0.6, alpha=0.4, label="Bid")
        self.ln_ask, = self.ax_price.plot([], [], color="#F44336", lw=0.6, alpha=0.4, label="Ask")
        self.ax_price.legend(fontsize=8, loc="upper left",
                             facecolor="#1a1a2e", edgecolor="#333")
        self.ax_price.grid(True, alpha=0.12)

        # — Spread —
        self.ax_spread = self.fig.add_subplot(gs[1, 0])
        self.ax_spread.set_title("Bid-Ask Spread", fontsize=12, color="#90CAF9", pad=8)
        self.ax_spread.set_ylabel("Spread", fontsize=9)
        self.ax_spread.tick_params(labelsize=8)
        self.ln_spread, = self.ax_spread.plot([], [], color="#4CAF50", lw=0.9)
        self.ax_spread.grid(True, alpha=0.12)

        # ── Panel 3: Trade rate ──
        self.ax_rate = self.fig.add_subplot(gs[1, 1])
        self.ax_rate.set_title("Trade Rate", fontsize=12, color="#90CAF9", pad=8)
        self.ax_rate.set_ylabel("Trades / step", fontsize=9)
        self.ax_rate.tick_params(labelsize=8)
        self.ln_rate, = self.ax_rate.plot([], [], color="#FF5722", lw=0.9)
        self.ax_rate.grid(True, alpha=0.12)

        # ── Panel 4: Positions ──
        self.ax_pos = self.fig.add_subplot(gs[2, 0])
        self.ax_pos.set_title("Agent Inventory", fontsize=12, color="#90CAF9", pad=8)
        self.ax_pos.set_ylabel("Position", fontsize=9)
        self.ax_pos.tick_params(labelsize=8)
        self.pos_lines = {}
        for a in self.sim.agents:
            is_mm    = a.name.startswith("MM")
            is_manip = a.name.startswith("MANIP")
            lw = 2.0 if (is_mm or is_manip) else 0.8
            al = 1.0 if (is_mm or is_manip) else 0.65
            color = AGENT_COLORS.get(a.name, "#FFD600" if is_manip else "#aaaaaa")
            ln, = self.ax_pos.plot([], [], color=color, lw=lw, alpha=al, label=a.name)
            self.pos_lines[a.name] = ln
        self.ax_pos.legend(fontsize=7, ncol=3, loc="upper left",
                           facecolor="#1a1a2e", edgecolor="#333")
        self.ax_pos.axhline(0, color="white", lw=0.3, ls="--")
        self.ax_pos.grid(True, alpha=0.12)

        # ── Panel 5: Depth ──
        self.ax_depth = self.fig.add_subplot(gs[2, 1])
        self.ax_depth.set_title("Order Book Depth", fontsize=12, color="#90CAF9", pad=8)
        self.ax_depth.set_ylabel("Volume", fontsize=9)
        self.ax_depth.set_xlabel("Price", fontsize=9)
        self.ax_depth.tick_params(labelsize=8)
        self.ax_depth.grid(True, alpha=0.12)
        self._depth_bars_bid = None
        self._depth_bars_ask = None
        self._depth_legend_added = False
        self._depth_frame_skip = 0  # only redraw depth every N frames

        # — Stats bar —
        self.stats_txt = self.fig.text(
            0.50, 0.995, "", fontsize=10, ha="center", va="top",
            color="#B0BEC5", family="monospace",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1a2e",
                      edgecolor="#333", alpha=0.9),
        )

    # ── Simulation tick ──────────────────────────────────────────────

    def _tick(self):
        """Run one simulation step and record data."""
        live_snapshot = self.live_feed.latest() if self.live_feed is not None else None
        live_mid = None
        live_bid = None
        live_ask = None
        live_latency_ms = None
        if live_snapshot:
            live_mid = float(live_snapshot.get("mid") or live_snapshot.get("price") or 0.0)
            live_bid = float(live_snapshot.get("bid") or live_mid or 0.0)
            live_ask = float(live_snapshot.get("ask") or live_mid or 0.0)
            live_latency_ms = float(live_snapshot.get("venue_latency_ms") or 0.0)
            for agent in self.sim.agents:
                if isinstance(agent, MarketMakerAgent):
                    agent.sync_reference_price(live_mid)

        self.sim._rng.shuffle(self.sim.agents)
        for agent in self.sim.agents:
            agent.step(self.step)
        for agent in self.sim.agents:
            agent.reconcile()

        bb = self.sim.ob.get_best_bid(self.sim.asset)
        ba = self.sim.ob.get_best_ask(self.sim.asset)
        bid_p = bb[0] if bb else None
        ask_p = ba[0] if ba else None
        mid   = (bid_p + ask_p) / 2.0 if (bid_p and ask_p) else (bid_p or ask_p)
        spread = (ask_p - bid_p) if (bid_p and ask_p) else 0.0

        tc  = len(self.sim.ob._trades)
        new = tc - self.prev_trade_count
        self.prev_trade_count = tc

        self.steps.append(self.step)
        display_mid = live_mid if live_mid is not None else mid
        display_bid = live_bid if live_bid is not None else bid_p
        display_ask = live_ask if live_ask is not None else ask_p
        display_spread = (display_ask - display_bid) if (display_bid and display_ask) else spread

        self.mids.append(display_mid)
        self.bids.append(display_bid)
        self.asks.append(display_ask)
        self.spreads.append(display_spread)
        self.live_mids.append(live_mid)
        self.live_bids.append(live_bid)
        self.live_asks.append(live_ask)
        self.live_latency_ms.append(live_latency_ms)
        self.trade_rate.append(new)
        for a in self.sim.agents:
            self.positions[a.name].append(a.state.position)

        self.step += 1
        return display_mid, display_spread, tc

    # ── Animation callback ───────────────────────────────────────────

    def update(self, _):
        try:
            mid = spread = tc = 0
            for _ in range(STEPS_PER_FRAME):
                mid, spread, tc = self._tick()

            x = list(self.steps)
            if len(x) < 2:
                return

            # — Price —
            vm = [(s, m) for s, m in zip(x, self.mids) if m is not None]
            vb = [(s, b) for s, b in zip(x, self.bids) if b is not None]
            va = [(s, a) for s, a in zip(x, self.asks) if a is not None]

            if vm:
                xs, ys = zip(*vm)
                self.ln_mid.set_data(xs, ys)
                ymin, ymax = min(ys), max(ys)
                pad = max(ymax - ymin, 0.05) * 0.12
                self.ax_price.set_xlim(xs[0], xs[-1])
                self.ax_price.set_ylim(ymin - pad, ymax + pad)
            if vb:
                self.ln_bid.set_data(*zip(*vb))
            if va:
                self.ln_ask.set_data(*zip(*va))

            # — Spread —
            sp = list(self.spreads)
            self.ln_spread.set_data(x, sp)
            self.ax_spread.set_xlim(x[0], x[-1])
            self.ax_spread.set_ylim(0, max(max(sp) * 1.3, 0.1) if sp else 1)

            # — Trade rate (smoothed) —
            rates = list(self.trade_rate)
            if len(rates) >= 15:
                k = np.ones(15) / 15
                smoothed = np.convolve(rates, k, mode="same")
                self.ln_rate.set_data(x, smoothed)
            else:
                self.ln_rate.set_data(x, rates)
            self.ax_rate.set_xlim(x[0], x[-1])
            self.ax_rate.set_ylim(0, max(max(rates) * 1.4, 1) if rates else 1)

            # — Positions —
            all_p = []
            for name, ln in self.pos_lines.items():
                p = list(self.positions[name])
                ln.set_data(x, p)
                all_p.extend(p)
            self.ax_pos.set_xlim(x[0], x[-1])
            if all_p:
                lo, hi = min(all_p), max(all_p)
                pad = max(abs(hi - lo), 10) * 0.15
                self.ax_pos.set_ylim(lo - pad, hi + pad)

            # — Depth (full redraw) —
            self.ax_depth.clear()
            self.ax_depth.set_title("Order Book Depth", fontsize=12,
                                    color="#90CAF9", pad=8)
            self.ax_depth.set_ylabel("Volume", fontsize=9)
            self.ax_depth.set_xlabel("Price", fontsize=9)
            self.ax_depth.tick_params(labelsize=8)
            self.ax_depth.grid(True, alpha=0.12)
            depth = self.sim.ob.get_market_depth(self.sim.asset, levels=10)
            bp = [p for p, v in depth["bids"]]
            bv = [v for p, v in depth["bids"]]
            ap = [p for p, v in depth["asks"]]
            av = [v for p, v in depth["asks"]]
            bar_w = 0.03
            if bp:
                self.ax_depth.bar(bp, bv, width=bar_w, color="#4CAF50",
                                  alpha=0.85, label="Bids")
            if ap:
                self.ax_depth.bar(ap, av, width=bar_w, color="#F44336",
                                  alpha=0.85, label="Asks")
            if bp or ap:
                allp = bp + ap
                self.ax_depth.set_xlim(min(allp) - 0.5, max(allp) + 0.5)
                self.ax_depth.legend(fontsize=7, facecolor="#1a1a2e",
                                     edgecolor="#333")

            # — Stats —
            mid_s  = f"{mid:.3f}" if mid is not None else "---"
            source_s = "Live Coin Watch" if any(v is not None for v in self.live_mids) else "Simulation fallback"
            latency_s = f"{self.live_latency_ms[-1]:.0f}ms" if self.live_latency_ms and self.live_latency_ms[-1] is not None else "n/a"
            mm_s   = (f"MM pos {self.mm.state.position:+d}  fills {self.mm.state.total_fills:,}"
                      if self.mm else "no MM")
            self.stats_txt.set_text(
                f"  Step {self.step:,}  |  Mid {mid_s}  |  "
                f"Spread {spread:.4f}  |  Trades {tc:,}  |  {mm_s}  |  {source_s} ({latency_s})  "
            )

            # Flush events so Windows doesn't show "not responding" cursor
            self.fig.canvas.flush_events()

        except Exception:
            traceback.print_exc()

    # ── Launch ───────────────────────────────────────────────────────

    def run(self):
        self._ani = animation.FuncAnimation(
            self.fig, self.update,
            interval=FRAME_MS,
            blit=False,
            cache_frame_data=False,
        )
        plt.show()


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CUATS live simulation dashboard")
    parser.add_argument("--mm",     type=int, default=1, metavar="N",
                        help="Number of market-maker agents (default: 1)")
    parser.add_argument("--retail", type=int, default=5, metavar="N",
                        help="Number of retail-trader agents (default: 5)")
    parser.add_argument("--manip",  type=int, default=1, metavar="N",
                        help="Number of market-manipulator agents (default: 1)")
    parser.add_argument(
        "--market-source",
        choices=("livecoinwatch", "simulated"),
        default="livecoinwatch",
        help="Choose a live BTC feed or keep the historical simulation price path.",
    )
    args = parser.parse_args()

    print(f"Building simulation: {args.mm} MM  {args.retail} Retail  {args.manip} Manip")
    sim, mm = build_simulation(
        num_mm=args.mm, num_retail=args.retail, num_manip=args.manip,
    )

    live_feed = None
    if args.market_source == "livecoinwatch":
        print("Starting Live Coin Watch feed")
        live_feed = LiveCoinWatchFeed()

    dash = LiveDashboard(sim, mm, live_feed=live_feed)
    dash.run()
