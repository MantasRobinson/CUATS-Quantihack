from __future__ import annotations

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
    drift_std: float = 0.03         # random walk std dev per step (adds organic movement)
    anchor_strength: float = 0.005  # mean-reversion pull toward initial fair value


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
        self._rng = random.Random()  # for drift noise

    @property
    def fair_value(self) -> float:
        return self._fair_value

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
        """Update fair value from last trade price via EMA.

        Uses trade prices rather than mid-price to avoid a feedback loop
        where the MM's own quotes shift the mid, which then shifts fair value.
        Also adds a small random drift for organic price movement.
        """
        trades = self.get_recent_trades(1)
        if trades:
            last_price = trades[0].price
            alpha = self.config.fair_value_ema
            self._fair_value = alpha * last_price + (1.0 - alpha) * self._fair_value

        # Mean-reversion pull toward initial fair value (prevents runaway drift)
        if self.config.anchor_strength > 0:
            anchor = self.config.fair_value
            self._fair_value += self.config.anchor_strength * (anchor - self._fair_value)

        # Small random walk for organic movement
        if self.config.drift_std > 0:
            self._fair_value += self._rng.gauss(0, self.config.drift_std)
            self._fair_value = max(1.0, self._fair_value)  # floor at 1.0
