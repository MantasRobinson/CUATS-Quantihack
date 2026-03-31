from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Callable, Optional

from exchange.orderbook import OrderBook
from .base import BaseAgent


@dataclass
class MarketMakerConfig:
    """Tunable parameters for the market maker."""

    fair_value: float = 100.0       # initial fair value estimate
    half_spread: float = 0.50       # half-spread applied each side of fair value
    quote_size: int = 10            # quantity per quote at each level
    max_inventory: int = 100        # absolute position limit (long or short)
    skew_factor: float = 0.02       # price shift per unit of inventory
    num_levels: int = 3             # number of price levels to quote on each side
    level_spacing: float = 0.20     # additional spread between successive levels
    fair_value_ema: float = 0.1     # EMA weight for updating fair value from trades
    annual_drift: float = 0.05      # risk-free rate (annual, e.g. 0.05 = 5%)
    annual_vol: float = 0.60        # annualised volatility (BTC-like)
    steps_per_year: int = 5040      # sim steps per year (for dt calculation)


class MarketMakerAgent(BaseAgent):
    """Continuously quotes both sides of the book around a fair value.

    Every step the MM:
      1. Updates its fair value from the last trade price (EMA), not mid
         (avoids self-referential feedback from own quotes).
      2. Cancels all existing resting orders.
      3. Computes an inventory skew — when long, shifts quotes down to
         encourage selling back; when short, shifts quotes up.
      4. Places limit orders at ``num_levels`` price levels on each side.

    The MM respects ``max_inventory``: it will not place bids if already
    at the long limit, and will not place asks if already at the short limit.
    """

    def __init__(
        self,
        name: str,
        asset: str,
        orderbook: OrderBook,
        id_generator: Callable[[], int],
        config: Optional[MarketMakerConfig] = None,
    ):
        super().__init__(name, asset, orderbook, id_generator)
        self.config = config or MarketMakerConfig()
        self._fair_value = self.config.fair_value
        self._reference_price: Optional[float] = None
        self._rng = random.Random()  # for drift noise

    @property
    def fair_value(self) -> float:
        return self._fair_value

    def sync_reference_price(self, price: float | None) -> None:
        """Anchor the fair value to an externally supplied reference price.

        This is used by the live dashboard so the market maker quotes around a
        live BTC feed instead of a purely simulated price path.
        """
        if price is None or price <= 0:
            return
        self._reference_price = float(price)

    def step(self, current_step: int) -> None:
        # 1. Update fair value from last trade (NOT mid — avoids feedback loop)
        self._update_fair_value()

        # 2. Cancel all existing quotes
        self.cancel_all_orders()

        # 3. Compute inventory skew
        #    Positive position (long) → shift quotes DOWN to attract sells
        #    Negative position (short) → shift quotes UP to attract buys
        skew = self.state.position * self.config.skew_factor

        # 4. Place new quotes at multiple levels
        cfg = self.config
        for level in range(cfg.num_levels):
            offset = cfg.half_spread + level * cfg.level_spacing

            bid_price = round(self._fair_value - offset - skew, 2)
            ask_price = round(self._fair_value + offset - skew, 2)

            # Ensure bid < ask (can happen with extreme skew)
            if bid_price >= ask_price:
                continue

            # Ensure positive prices
            if bid_price <= 0:
                continue

            # Respect inventory limits
            if self.state.position < cfg.max_inventory:
                self.submit_order("limit", "buy", cfg.quote_size, bid_price)

            if self.state.position > -cfg.max_inventory:
                self.submit_order("limit", "sell", cfg.quote_size, ask_price)

    def _update_fair_value(self) -> None:
        """Update fair value using geometric Brownian motion with risk-free drift.

        The fair value follows a GBM process:
            S(t+1) = S(t) * exp((mu - sigma^2/2)*dt + sigma*sqrt(dt)*Z)

        Also blends in the last trade price via EMA to keep the MM responsive
        to actual market activity.
        """
        cfg = self.config

        # When a live reference price is available, anchor directly to it.
        if self._reference_price is not None:
            self._fair_value = max(1.0, float(self._reference_price))
            return

        dt = 1.0 / cfg.steps_per_year
        mu = cfg.annual_drift
        sigma = cfg.annual_vol

        # GBM step: multiplicative random walk with drift
        drift = (mu - 0.5 * sigma * sigma) * dt
        diffusion = sigma * math.sqrt(dt) * self._rng.gauss(0, 1)
        self._fair_value *= math.exp(drift + diffusion)

        # Blend with last trade price (EMA) to stay responsive to market
        trades = self.get_recent_trades(1)
        if trades:
            last_price = trades[0].price
            alpha = cfg.fair_value_ema
            self._fair_value = alpha * last_price + (1.0 - alpha) * self._fair_value

        self._fair_value = max(1.0, self._fair_value)  # floor at 1.0
