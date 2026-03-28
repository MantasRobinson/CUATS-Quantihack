"""Entry point: run a market simulation with MM + retail + manipulator agents."""

from agents.simulation import Simulation, SimulationConfig
from agents.market_maker import MarketMakerAgent
from agents.retail_trader import RetailTraderAgent, RetailTraderConfig
from agents.manipulator import ManipulatorAgent, ManipulatorConfig
from btc_sim_config import BTC_ASSET, build_btc_market_maker_config
from export import export_simulation


def main():
    # --- Configuration ---
    sim_config = SimulationConfig(num_steps=2000, asset=BTC_ASSET, seed=42)
    sim = Simulation(sim_config)

    # --- Market Maker ---
    mm = MarketMakerAgent(
        name="MM",
        asset=sim.asset,
        orderbook=sim.ob,
        id_generator=sim.id_generator,
        config=build_btc_market_maker_config(),
    )
    sim.add_agent(mm)

    # --- Retail Traders (5 with varying personalities) ---
    retail_configs = [
        # Cautious, low activity, mostly limit orders
        RetailTraderConfig(activity_rate=0.15, min_size=1, max_size=8,
                           trend_sensitivity=0.0, limit_order_pct=0.6, max_position=50),
        # Average retail
        RetailTraderConfig(activity_rate=0.25, min_size=1, max_size=10,
                           trend_sensitivity=0.0, limit_order_pct=0.4, max_position=60),
        # Active trader
        RetailTraderConfig(activity_rate=0.35, min_size=1, max_size=12,
                           trend_sensitivity=0.0, limit_order_pct=0.3, max_position=50),
        # Aggressive random trader
        RetailTraderConfig(activity_rate=0.30, min_size=1, max_size=15,
                           trend_sensitivity=0.0, limit_order_pct=0.2, max_position=40),
        # Passive random trader
        RetailTraderConfig(activity_rate=0.20, min_size=1, max_size=8,
                           trend_sensitivity=0.0, limit_order_pct=0.5, max_position=50),
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
