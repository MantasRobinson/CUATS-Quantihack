"""Visualise simulation output: price series, spread, agent positions, trade volume."""

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

from agents.simulation import Simulation, SimulationConfig
from agents.market_maker import MarketMakerAgent, MarketMakerConfig
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig


def run_and_plot():
    # --- Run simulation ---
    sim_config = SimulationConfig(num_steps=2000, asset="ASSET", seed=42)
    sim = Simulation(sim_config)

    mm = MarketMakerAgent(
        name="MM", asset=sim.asset, orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=MarketMakerConfig(
            fair_value=100.0, half_spread=0.40, quote_size=15,
            max_inventory=200, skew_factor=0.02, num_levels=5,
            level_spacing=0.15, fair_value_ema=0.05,
        ),
    )
    sim.add_agent(mm)

    retail_configs = [
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
    for i, cfg in enumerate(retail_configs):
        rt = RetailTraderAgent(
            name=f"RT-{i}", asset=sim.asset, orderbook=sim.ob,
            id_generator=sim.id_generator, config=cfg,
            rng_seed=sim_config.seed + i + 1,
        )
        sim.add_agent(rt)

    print("Running simulation...")
    sim.run(verbose=True)
    print(f"Done. {len(sim.get_all_trades())} trades.")

    # --- Extract data ---
    steps = [r.step for r in sim.history]
    mids = [r.mid_price for r in sim.history]
    spreads = [r.spread if r.spread is not None else 0 for r in sim.history]
    trade_prices = [r.last_trade_price for r in sim.history]

    # Cumulative trade count per step
    cum_trades = [r.num_trades_total for r in sim.history]

    # Per-step new trades (delta)
    new_trades = [cum_trades[0]] + [cum_trades[i] - cum_trades[i-1] for i in range(1, len(cum_trades))]

    # Agent positions over time
    agent_names = [a.name for a in sim.agents]
    positions = {name: [] for name in agent_names}
    for r in sim.history:
        for name in agent_names:
            positions[name].append(r.agent_positions.get(name, 0))

    # Trade-level data for scatter
    all_trades = sim.get_all_trades()
    # We don't have timestamps on trades directly, but we can use the order's
    # timestamp from the exchange. Let's approximate by index.
    trade_prices_all = [t.price for t in all_trades]
    trade_sizes_all = [t.quantity for t in all_trades]

    # --- Plot ---
    fig = plt.figure(figsize=(16, 12))
    fig.suptitle("Market Simulation Dashboard", fontsize=16, fontweight="bold", y=0.98)
    gs = gridspec.GridSpec(3, 2, hspace=0.35, wspace=0.3)

    # 1. Price series
    ax1 = fig.add_subplot(gs[0, :])
    valid_steps = [s for s, m in zip(steps, mids) if m is not None]
    valid_mids = [m for m in mids if m is not None]
    ax1.plot(valid_steps, valid_mids, color="#2196F3", linewidth=0.8, label="Mid Price")
    # Overlay trade prices as faint dots
    valid_tp_steps = [s for s, tp in zip(steps, trade_prices) if tp is not None]
    valid_tp = [tp for tp in trade_prices if tp is not None]
    ax1.scatter(valid_tp_steps, valid_tp, s=1, alpha=0.3, color="#FF9800", label="Last Trade")
    ax1.set_xlabel("Step")
    ax1.set_ylabel("Price")
    ax1.set_title("Price Evolution")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    # 2. Spread over time
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.fill_between(steps, spreads, alpha=0.4, color="#4CAF50")
    ax2.plot(steps, spreads, linewidth=0.5, color="#388E3C")
    ax2.set_xlabel("Step")
    ax2.set_ylabel("Spread")
    ax2.set_title("Bid-Ask Spread")
    ax2.grid(True, alpha=0.3)

    # 3. Trade volume per step (rolling window)
    ax3 = fig.add_subplot(gs[1, 1])
    window = 20
    if len(new_trades) >= window:
        rolling_vol = np.convolve(new_trades, np.ones(window)/window, mode='valid')
        ax3.plot(steps[window-1:], rolling_vol, color="#FF5722", linewidth=0.8)
    else:
        ax3.bar(steps, new_trades, color="#FF5722", alpha=0.6)
    ax3.set_xlabel("Step")
    ax3.set_ylabel(f"Trades / step ({window}-step avg)")
    ax3.set_title("Trading Activity")
    ax3.grid(True, alpha=0.3)

    # 4. Agent positions
    ax4 = fig.add_subplot(gs[2, 0])
    colors = ["#E91E63", "#9C27B0", "#3F51B5", "#009688", "#FF9800", "#795548"]
    for i, name in enumerate(agent_names):
        c = colors[i % len(colors)]
        lw = 1.5 if name == "MM" else 0.7
        ax4.plot(steps, positions[name], label=name, color=c, linewidth=lw,
                 alpha=1.0 if name == "MM" else 0.7)
    ax4.set_xlabel("Step")
    ax4.set_ylabel("Position (units)")
    ax4.set_title("Agent Inventory")
    ax4.legend(fontsize=7, ncol=3)
    ax4.axhline(y=0, color="black", linewidth=0.5, linestyle="--")
    ax4.grid(True, alpha=0.3)

    # 5. Trade size distribution
    ax5 = fig.add_subplot(gs[2, 1])
    ax5.hist(trade_sizes_all, bins=30, color="#673AB7", alpha=0.7, edgecolor="white")
    ax5.set_xlabel("Trade Size")
    ax5.set_ylabel("Frequency")
    ax5.set_title(f"Trade Size Distribution (n={len(all_trades)})")
    ax5.grid(True, alpha=0.3)

    plt.savefig("simulation_dashboard.png", dpi=150, bbox_inches="tight")
    print("Saved simulation_dashboard.png")
    # plt.show()  # uncomment for interactive display


if __name__ == "__main__":
    run_and_plot()
