"""Entry point: run a market simulation with MM + retail + manipulator agents."""

from agents.simulation import Simulation, SimulationConfig
from agents.market_maker import MarketMakerAgent, MarketMakerConfig
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig
from agents.manipulator import ManipulatorAgent, ManipulatorConfig
from export import export_simulation


def main():
    # --- Configuration ---
    sim_config = SimulationConfig(num_steps=2000, asset="ASSET", seed=42)
    sim = Simulation(sim_config)

    # --- Market Maker ---
    mm = MarketMakerAgent(
        name="MM",
        asset=sim.asset,
        orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=MarketMakerConfig(
            fair_value=100.0,
            half_spread=0.40,
            quote_size=15,
            max_inventory=200,
            skew_factor=0.02,
            num_levels=5,
            level_spacing=0.15,
            fair_value_ema=0.05,
        ),
    )
    sim.add_agent(mm)

    # --- Retail Traders (5 with varying personalities) ---
    retail_configs = [
        # Cautious, low activity, mostly limit orders
        RetailTraderConfig(activity_rate=0.15, min_size=1, max_size=8,
                           trend_sensitivity=0.2, limit_order_pct=0.6, max_position=50),
        # Average retail
        RetailTraderConfig(activity_rate=0.25, min_size=1, max_size=10,
                           trend_sensitivity=0.3, limit_order_pct=0.4, max_position=60),
        # Active trader, moderate trend chaser
        RetailTraderConfig(activity_rate=0.35, min_size=1, max_size=12,
                           trend_sensitivity=0.5, limit_order_pct=0.3, max_position=50),
        # FOMO buyer — more trend sensitive, market orders
        RetailTraderConfig(activity_rate=0.30, min_size=1, max_size=15,
                           trend_sensitivity=0.6, limit_order_pct=0.2, max_position=40),
        # Contrarian — low trend sensitivity (mostly noise)
        RetailTraderConfig(activity_rate=0.20, min_size=1, max_size=8,
                           trend_sensitivity=0.1, limit_order_pct=0.5, max_position=50),
    ]

    for i, cfg in enumerate(retail_configs):
        rt = RetailTraderAgent(
            name=f"RT-{i}",
            asset=sim.asset,
            orderbook=sim.ob,
            id_generator=sim.id_generator,
            config=cfg,
            rng_seed=sim_config.seed + i + 1,
        )
        sim.add_agent(rt)

    # --- Manipulator ---
    manip = ManipulatorAgent(
        name="MANIP",
        asset=sim.asset,
        orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=ManipulatorConfig(
            phase_length=150,
            spoof_size=80,
            pump_size=25,
            dump_size=30,
            wash_size=5,
        ),
        rng_seed=sim_config.seed + 100,
    )
    sim.add_agent(manip)

    # --- Run ---
    print(f"Running simulation: {sim_config.num_steps} steps, "
          f"asset={sim.asset!r}, {len(sim.agents)} agents")
    print("-" * 60)

    sim.run(verbose=True)

    # --- Results ---
    print("\n" + "=" * 60)
    print("SIMULATION COMPLETE")
    print("=" * 60)

    summary = sim.summary()
    for name, stats in summary.items():
        print(f"\n  {name}:")
        for k, v in stats.items():
            print(f"    {k}: {v}")

    # Quick price series stats
    prices = [p for p in sim.get_price_series() if p is not None]
    if prices:
        print(f"\nPrice series: {len(prices)} observations")
        print(f"  Start: {prices[0]:.2f}")
        print(f"  End:   {prices[-1]:.2f}")
        print(f"  Min:   {min(prices):.2f}")
        print(f"  Max:   {max(prices):.2f}")
        print(f"  Range: {max(prices) - min(prices):.2f}")

    all_trades = sim.get_all_trades()
    print(f"\nTotal trades executed: {len(all_trades)}")

    # --- Export data ---
    export_simulation(sim)


if __name__ == "__main__":
    main()
