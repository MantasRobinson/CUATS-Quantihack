"""Live simulation dashboard — subscribes to the WebSocket server and displays
the same engine state that client.html is sonifying.

Usage:
    1. python server.py          (in one terminal)
    2. python live_dashboard.py  (in another terminal)
    Open client.html in your browser — all three views share one simulation.

Press Q or close the window to stop.
"""

import sys
import threading
import traceback
import queue
import json

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

# WebSocket client (same package as the server uses)
try:
    import websockets.sync.client as ws_sync
except ImportError:
    print("ERROR: 'websockets' package not found.\n"
          "Install it with:  pip install websockets", file=sys.stderr)
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────

WS_URL         = "ws://localhost:8765"
WINDOW         = 300   # steps of history visible on screen
FRAME_MS       = 100   # milliseconds between animation frames (~10 fps)

AGENT_COLORS = {
    "MM":   "#E91E63",
    "RT-0": "#9C27B0",
    "RT-1": "#3F51B5",
    "RT-2": "#009688",
    "RT-3": "#FF9800",
    "RT-4": "#795548",
}
AGENT_NAMES = ["MM", "RT-0", "RT-1", "RT-2", "RT-3", "RT-4"]


# ─────────────────────────────────────────────────────────────────────
# Background WebSocket receiver thread
# ─────────────────────────────────────────────────────────────────────

_data_queue: queue.Queue = queue.Queue(maxsize=200)
_stop_event = threading.Event()


def _ws_thread():
    """Connects to the server, pushes parsed payloads into _data_queue."""
    while not _stop_event.is_set():
        try:
            print(f"[ws] Connecting to {WS_URL} …", flush=True)
            with ws_sync.connect(WS_URL) as sock:
                print("[ws] Connected.", flush=True)
                while not _stop_event.is_set():
                    raw = sock.recv(timeout=5.0)
                    try:
                        payload = json.loads(raw)
                        # Non-blocking put; drop oldest if full
                        if _data_queue.full():
                            try:
                                _data_queue.get_nowait()
                            except queue.Empty:
                                pass
                        _data_queue.put_nowait(payload)
                    except Exception:
                        pass
        except Exception as exc:
            if not _stop_event.is_set():
                print(f"[ws] Disconnected ({exc}). Retrying in 2 s …", flush=True)
                _stop_event.wait(timeout=2.0)


# ─────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────

class LiveDashboard:

    def __init__(self):
        self.seq = 0
        self.prev_trade_count = 0

        # Rolling buffers
        self.steps       = deque(maxlen=WINDOW)
        self.mids        = deque(maxlen=WINDOW)
        self.bids        = deque(maxlen=WINDOW)
        self.asks        = deque(maxlen=WINDOW)
        self.spreads_bps = deque(maxlen=WINDOW)
        self.trade_counts = deque(maxlen=WINDOW)
        self.positions   = {n: deque(maxlen=WINDOW) for n in AGENT_NAMES}

        self._build_figure()

    # ── Figure ───────────────────────────────────────────────────────

    def _build_figure(self):
        plt.style.use("dark_background")
        self.fig = plt.figure(figsize=(14, 8))
        self.fig.patch.set_facecolor("#0D1117")

        try:
            mgr = plt.get_current_fig_manager()
            mgr.window.state("zoomed")
        except Exception:
            try:
                plt.get_current_fig_manager().full_screen_toggle()
            except Exception:
                pass

        self.fig.suptitle("CUATS Live Market Simulation  (WebSocket feed)",
                          fontsize=15, fontweight="bold", color="#00E5FF", y=0.97)

        gs = gridspec.GridSpec(3, 2, hspace=0.50, wspace=0.30,
                               left=0.07, right=0.96, top=0.91, bottom=0.06)

        # — Price —
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

        # — Spread (bps) —
        self.ax_spread = self.fig.add_subplot(gs[1, 0])
        self.ax_spread.set_title("Spread (bps)", fontsize=12, color="#90CAF9", pad=8)
        self.ax_spread.set_ylabel("bps", fontsize=9)
        self.ax_spread.tick_params(labelsize=8)
        self.ln_spread, = self.ax_spread.plot([], [], color="#4CAF50", lw=0.9)
        self.ax_spread.axhline(40, color="#ffaa0066", lw=1, ls="--")   # ALERT
        self.ax_spread.axhline(80, color="#ff444466", lw=1, ls=":")    # HALT
        self.ax_spread.grid(True, alpha=0.12)

        # — Trade rate —
        self.ax_rate = self.fig.add_subplot(gs[1, 1])
        self.ax_rate.set_title("Cumulative Trades", fontsize=12, color="#90CAF9", pad=8)
        self.ax_rate.set_ylabel("Total trades", fontsize=9)
        self.ax_rate.tick_params(labelsize=8)
        self.ln_rate, = self.ax_rate.plot([], [], color="#FF5722", lw=0.9)
        self.ax_rate.grid(True, alpha=0.12)

        # — Positions —
        self.ax_pos = self.fig.add_subplot(gs[2, 0])
        self.ax_pos.set_title("Agent Inventory", fontsize=12, color="#90CAF9", pad=8)
        self.ax_pos.set_ylabel("Position", fontsize=9)
        self.ax_pos.tick_params(labelsize=8)
        self.pos_lines = {}
        for name in AGENT_NAMES:
            lw = 2.0 if name == "MM" else 0.8
            ln, = self.ax_pos.plot([], [], color=AGENT_COLORS.get(name, "#aaa"),
                                   lw=lw, alpha=1.0 if name == "MM" else 0.65,
                                   label=name)
            self.pos_lines[name] = ln
        self.ax_pos.legend(fontsize=7, ncol=3, loc="upper left",
                           facecolor="#1a1a2e", edgecolor="#333")
        self.ax_pos.axhline(0, color="white", lw=0.3, ls="--")
        self.ax_pos.grid(True, alpha=0.12)

        # — Depth —
        self.ax_depth = self.fig.add_subplot(gs[2, 1])
        self.ax_depth.set_title("Order Book Depth", fontsize=12, color="#90CAF9", pad=8)
        self.ax_depth.tick_params(labelsize=8)
        self.ax_depth.grid(True, alpha=0.12)

        # — Stats text —
        self.stats_txt = self.fig.text(
            0.50, 0.995, "Connecting …", fontsize=10, ha="center", va="top",
            color="#B0BEC5", family="monospace",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#1a1a2e",
                      edgecolor="#333", alpha=0.9),
        )

    # ── Animation callback ───────────────────────────────────────────

    def update(self, _frame):
        try:
            # Drain the queue — use only the latest payload per frame
            payload = None
            while True:
                try:
                    payload = _data_queue.get_nowait()
                except queue.Empty:
                    break

            if payload is None:
                return  # no new data yet

            d   = payload.get("debug", {})
            sm  = payload.get("smoothed_metrics", {})
            state = payload.get("state", "OK")

            mid = d.get("mid")
            bid = d.get("bid")
            ask = d.get("ask")
            total_trades = d.get("total_trades", 0)
            mm_pos  = d.get("mm_position", 0)
            mm_pnl  = d.get("mm_pnl", 0.0)
            retail  = d.get("retail_positions", [0] * 5)
            spread_bps = sm.get("spread_bps", 0.0)

            self.seq += 1
            self.steps.append(self.seq)
            self.mids.append(mid)
            self.bids.append(bid)
            self.asks.append(ask)
            self.spreads_bps.append(spread_bps)
            self.trade_counts.append(total_trades)

            all_positions = [mm_pos] + retail
            for name, pos in zip(AGENT_NAMES, all_positions):
                self.positions[name].append(pos)

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
                pad = max(ymax - ymin, 0.05) * 0.15
                self.ax_price.set_xlim(xs[0], xs[-1])
                self.ax_price.set_ylim(ymin - pad, ymax + pad)
            if vb:
                self.ln_bid.set_data(*zip(*vb))
            if va:
                self.ln_ask.set_data(*zip(*va))

            # — Spread —
            sp = list(self.spreads_bps)
            self.ln_spread.set_data(x, sp)
            self.ax_spread.set_xlim(x[0], x[-1])
            self.ax_spread.set_ylim(0, max(max(sp) * 1.3, 15) if sp else 50)

            # — Trade count —
            tc = list(self.trade_counts)
            self.ln_rate.set_data(x, tc)
            self.ax_rate.set_xlim(x[0], x[-1])
            self.ax_rate.set_ylim(0, max(tc) * 1.1 if tc else 1)

            # — Positions —
            all_p = []
            for name, ln in self.pos_lines.items():
                p = list(self.positions[name])
                ln.set_data(x, p)
                all_p.extend(p)
            self.ax_pos.set_xlim(x[0], x[-1])
            if all_p:
                lo, hi = min(all_p), max(all_p)
                pad = max(abs(hi - lo), 5) * 0.15
                self.ax_pos.set_ylim(lo - pad, hi + pad)

            # — Depth (full redraw every frame) —
            self.ax_depth.clear()
            self.ax_depth.set_title("Order Book Depth", fontsize=12,
                                    color="#90CAF9", pad=8)
            self.ax_depth.set_ylabel("Volume", fontsize=9)
            self.ax_depth.set_xlabel("Price", fontsize=9)
            self.ax_depth.tick_params(labelsize=8)
            self.ax_depth.grid(True, alpha=0.12)

            depth_bids = d.get("depth_bids", [])
            depth_asks = d.get("depth_asks", [])
            bp = [p for p, v in depth_bids]
            bv = [v for p, v in depth_bids]
            ap = [p for p, v in depth_asks]
            av = [v for p, v in depth_asks]
            bar_w = 0.03
            if bp:
                self.ax_depth.bar(bp, bv, width=bar_w, color="#4CAF50",
                                  alpha=0.85, label="Bids")
            if ap:
                self.ax_depth.bar(ap, av, width=bar_w, color="#F44336",
                                  alpha=0.85, label="Asks")
            if bp or ap:
                allp = bp + ap
                self.ax_depth.set_xlim(min(allp) - 0.3, max(allp) + 0.3)
                self.ax_depth.legend(fontsize=7, facecolor="#1a1a2e",
                                     edgecolor="#333")

            # — Stats —
            mid_s = f"{mid:.3f}" if mid is not None else "---"
            self.stats_txt.set_text(
                f"  State: {state}  |  Mid {mid_s}  |  Spread {spread_bps:.1f} bps  |  "
                f"Trades {total_trades:,}  |  MM pos {mm_pos:+d}  |  MM P&L {mm_pnl:+.1f}  "
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
        self.fig.canvas.mpl_connect(
            "close_event", lambda _: _stop_event.set()
        )
        plt.show()


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t = threading.Thread(target=_ws_thread, daemon=True)
    t.start()

    dash = LiveDashboard()
    try:
        dash.run()
    finally:
        _stop_event.set()
