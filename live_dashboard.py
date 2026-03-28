"""Live simulation dashboard — watch agents trade in real-time.

Usage:  python live_dashboard.py
        Press Q or close the window to stop.
"""

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
from agents.market_maker import MarketMakerAgent, MarketMakerConfig
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig
from agents.manipulator import ManipulatorAgent, ManipulatorConfig


# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────

WINDOW         = 600    # how many steps of history visible on screen
STEPS_PER_FRAME = 3     # sim steps computed per animation frame
FRAME_MS       = 80     # milliseconds between frames (~12 fps)

AGENT_COLORS = {
    "MM":    "#E91E63",
    "RT-0":  "#9C27B0",
    "RT-1":  "#3F51B5",
    "RT-2":  "#009688",
    "RT-3":  "#FF9800",
    "RT-4":  "#795548",
    "MANIP": "#FFD600",
}


# ─────────────────────────────────────────────────────────────────────
# Simulation setup
# ─────────────────────────────────────────────────────────────────────

def build_simulation():
    sim_cfg = SimulationConfig(num_steps=999_999, asset="BTC", seed=42)
    sim = Simulation(sim_cfg)

    mm = MarketMakerAgent(
        name="MM", asset=sim.asset, orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=MarketMakerConfig(
            fair_value=100.0, half_spread=0.40, quote_size=15,
            max_inventory=200, skew_factor=0.02, num_levels=5,
            level_spacing=0.15, fair_value_ema=0.05,
            annual_drift=0.05, annual_vol=0.60,
        ),
    )
    sim.add_agent(mm)

    retail_cfgs = [
        RetailTraderConfig(activity_rate=0.15, min_size=1, max_size=8,
                           trend_sensitivity=0.0, limit_order_pct=0.6, max_position=50),
        RetailTraderConfig(activity_rate=0.25, min_size=1, max_size=10,
                           trend_sensitivity=0.0, limit_order_pct=0.4, max_position=60),
        RetailTraderConfig(activity_rate=0.35, min_size=1, max_size=12,
                           trend_sensitivity=0.0, limit_order_pct=0.3, max_position=50),
        RetailTraderConfig(activity_rate=0.30, min_size=1, max_size=15,
                           trend_sensitivity=0.0, limit_order_pct=0.2, max_position=40),
        RetailTraderConfig(activity_rate=0.20, min_size=1, max_size=8,
                           trend_sensitivity=0.0, limit_order_pct=0.5, max_position=50),
    ]
    for i, cfg in enumerate(retail_cfgs):
        sim.add_agent(RetailTraderAgent(
            name=f"RT-{i}", asset=sim.asset, orderbook=sim.ob,
            id_generator=sim.id_generator, config=cfg,
            rng_seed=sim_cfg.seed + i + 1,
        ))

    manip = ManipulatorAgent(
        name="MANIP", asset=sim.asset, orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=ManipulatorConfig(
            phase_length=150, spoof_size=80, pump_size=25,
            dump_size=30, wash_size=5,
        ),
        rng_seed=sim_cfg.seed + 100,
    )
    sim.add_agent(manip)

    return sim, mm, manip


# ─────────────────────────────────────────────────────────────────────
# Dashboard class
# ─────────────────────────────────────────────────────────────────────

class LiveDashboard:

    def __init__(self, sim: Simulation, mm: MarketMakerAgent, manip: ManipulatorAgent):
        self.sim = sim
        self.mm = mm
        self.manip = manip
        self.step = 0
        self.prev_trade_count = 0

        # Rolling buffers
        self.steps      = deque(maxlen=WINDOW)
        self.mids       = deque(maxlen=WINDOW)
        self.bids       = deque(maxlen=WINDOW)
        self.asks       = deque(maxlen=WINDOW)
        self.spreads    = deque(maxlen=WINDOW)
        self.trade_rate = deque(maxlen=WINDOW)
        self.positions  = {a.name: deque(maxlen=WINDOW) for a in sim.agents}

        self._build_figure()

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

        self.fig.suptitle("CUATS Live Market Simulation", fontsize=16,
                          fontweight="bold", color="#00E5FF", y=0.97)

        gs = gridspec.GridSpec(3, 2, hspace=0.50, wspace=0.30,
                               left=0.07, right=0.96, top=0.91, bottom=0.06)

        # ── Panel 1: Price (full width) ──
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

        # ── Panel 2: Spread ──
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
            lw = 2.0 if a.name in ("MM", "MANIP") else 0.8
            al = 1.0 if a.name in ("MM", "MANIP") else 0.65
            ln, = self.ax_pos.plot([], [], color=AGENT_COLORS.get(a.name, "#aaa"),
                                   lw=lw, alpha=al, label=a.name)
            self.pos_lines[a.name] = ln
        self.ax_pos.legend(fontsize=7, ncol=3, loc="upper left",
                           facecolor="#1a1a2e", edgecolor="#333")
        self.ax_pos.axhline(0, color="white", lw=0.3, ls="--")
        self.ax_pos.grid(True, alpha=0.12)

        # ── Panel 5: Depth ──
        self.ax_depth = self.fig.add_subplot(gs[2, 1])
        self.ax_depth.set_title("Order Book Depth", fontsize=12, color="#90CAF9", pad=8)
        self.ax_depth.tick_params(labelsize=8)
        self.ax_depth.grid(True, alpha=0.12)

        # ── Stats bar ──
        self.stats_txt = self.fig.text(
            0.50, 0.995, "", fontsize=10, ha="center", va="top",
            color="#B0BEC5", family="monospace",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1a2e",
                      edgecolor="#333", alpha=0.9),
        )

    # ── Simulation tick ──────────────────────────────────────────────

    def _tick(self):
        """Run one simulation step and record data."""
        self.sim._rng.shuffle(self.sim.agents)
        for agent in self.sim.agents:
            agent.step(self.step)
        for agent in self.sim.agents:
            agent.reconcile()

        bb = self.sim.ob.get_best_bid(self.sim.asset)
        ba = self.sim.ob.get_best_ask(self.sim.asset)

        bid_p = bb[0] if bb else None
        ask_p = ba[0] if ba else None

        if bid_p is not None and ask_p is not None:
            mid = (bid_p + ask_p) / 2.0
        elif bid_p is not None:
            mid = bid_p
        elif ask_p is not None:
            mid = ask_p
        else:
            mid = None

        spread = (ask_p - bid_p) if (bid_p is not None and ask_p is not None) else 0.0

        tc = len(self.sim.ob._trades)
        new = tc - self.prev_trade_count
        self.prev_trade_count = tc

        self.steps.append(self.step)
        self.mids.append(mid)
        self.bids.append(bid_p)
        self.asks.append(ask_p)
        self.spreads.append(spread)
        self.trade_rate.append(new)

        for a in self.sim.agents:
            self.positions[a.name].append(a.state.position)

        self.step += 1
        return mid, spread

    # ── Animation callback ───────────────────────────────────────────

    def update(self, _frame):
        try:
            mid = spread = 0
            for _ in range(STEPS_PER_FRAME):
                mid, spread = self._tick()

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
                pad = max(ymax - ymin, 0.5) * 0.12
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

            bar_w = 0.06
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

            # — Stats bar —
            tc = len(self.sim.ob._trades)
            mid_s = f"{mid:.2f}" if mid else "---"
            self.stats_txt.set_text(
                f"  Step {self.step:,}  |  Mid {mid_s}  |  "
                f"Spread {spread:.3f}  |  Trades {tc:,}  |  "
                f"MM pos {self.mm.state.position:+d}  "
                f"fills {self.mm.state.total_fills:,}  |  "
                f"MANIP pos {self.manip.state.position:+d}  "
                f"phase {self.manip._current_phase}  "
            )

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
    sim, mm, manip = build_simulation()
    dash = LiveDashboard(sim, mm, manip)
    dash.run()
