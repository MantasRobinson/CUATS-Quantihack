"""Shared BTC market-maker calibration for the local 7-agent simulations."""

from agents.market_maker import MarketMakerConfig


BTC_ASSET = "BTC"

# March 2026 BTC/USD has been trading around the high-60k to low-70k range,
# so we anchor the local simulations near 70k instead of using a toy price.
BTC_START_PRICE = 70_000.0

# Keep the existing GBM assumptions, but scale the quoting parameters so the
# order book remains sensible at BTC/USD price levels.
BTC_MM_HALF_SPREAD = 35.0
BTC_MM_LEVEL_SPACING = 15.0
BTC_MM_SKEW_FACTOR = 14.0
BTC_MM_STEPS_PER_YEAR = 35_040


def build_btc_market_maker_config() -> MarketMakerConfig:
    return MarketMakerConfig(
        fair_value=BTC_START_PRICE,
        half_spread=BTC_MM_HALF_SPREAD,
        quote_size=15,
        max_inventory=200,
        skew_factor=BTC_MM_SKEW_FACTOR,
        num_levels=5,
        level_spacing=BTC_MM_LEVEL_SPACING,
        fair_value_ema=0.05,
        annual_drift=0.05,
        annual_vol=0.60,
        steps_per_year=BTC_MM_STEPS_PER_YEAR,
    )
