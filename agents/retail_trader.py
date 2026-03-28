from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Optional

from exchange.orderbook import OrderBook
from .base import BaseAgent


@dataclass
class RetailTraderConfig:
    """Tunable parameters for a retail trader."""

    activity_rate: float = 0.30       # probability of acting on any step
    min_size: int = 1                 # minimum order quantity
    max_size: int = 20                # maximum order quantity
    trend_sensitivity: float = 0.6    # 0 = pure noise, 1 = pure trend chaser
    limit_order_pct: float = 0.40     # fraction of orders placed as limits
    limit_offset_range: float = 1.0   # max distance of limit price from mid
    lookback: int = 5                 # number of recent trades for trend signal
    max_position: int = 200           # soft position cap — less likely to add


class RetailTraderAgent(BaseAgent):
    """Simulates a retail / noise trader.

    Behaviour:
      - Trades sporadically (controlled by ``activity_rate``).
      - Direction is a blend of trend-following signal and random noise.
      - Mix of market orders (aggressive) and limit orders (passive).
      - Has a soft position cap to prevent unbounded accumulation.
    """

    def __init__(
        self,
        name: str,
        asset: str,
        orderbook: OrderBook,
        id_generator: Callable[[], int],
        config: Optional[RetailTraderConfig] = None,
        rng_seed: Optional[int] = None,
    ):
        super().__init__(name, asset, orderbook, id_generator)
        self.config = config or RetailTraderConfig()
        self.rng = random.Random(rng_seed)

    def step(self, current_step: int) -> None:
        # 1. Random activity check — most steps, retail does nothing
        if self.rng.random() > self.config.activity_rate:
            return

        # 2. Need a reference price to trade around
        mid = self.get_mid_price()
        if mid is None:
            return

        # 3. Compute trend signal
        trend = self._trend_signal()

        # 4. Blend trend with noise to pick direction
        noise = self.rng.gauss(0.0, 1.0)
        cfg = self.config
        combined = cfg.trend_sensitivity * trend + (1.0 - cfg.trend_sensitivity) * noise

        # Soft position cap: reduce probability of adding when near limit
        if self.state.position > cfg.max_position and combined > 0:
            combined *= 0.2   # dampen buy signal when very long
        elif self.state.position < -cfg.max_position and combined < 0:
            combined *= 0.2   # dampen sell signal when very short

        side = "buy" if combined > 0 else "sell"

        # 5. Random order size
        qty = self.rng.randint(cfg.min_size, cfg.max_size)

        # 6. Choose order type: market or limit
        if self.rng.random() > cfg.limit_order_pct:
            # Market order — aggressive, crosses the spread
            self.submit_order("market", side, qty)
        else:
            # Limit order — placed near the spread
            offset = self.rng.uniform(0.0, cfg.limit_offset_range)
            if side == "buy":
                price = round(mid - offset, 2)
            else:
                price = round(mid + offset, 2)

            # Sanity: price must be positive
            if price <= 0:
                return

            self.submit_order("limit", side, qty, price)

    def _trend_signal(self) -> float:
        """Compute a simple trend signal from recent trade prices.

        Returns positive for uptrend, negative for downtrend, 0 for no data.
        Normalised as approximate percentage change.
        """
        trades = self.get_recent_trades(self.config.lookback)
        if len(trades) < 2:
            return 0.0

        # trades are most-recent-first
        newest_price = trades[0].price
        oldest_price = trades[-1].price

        if oldest_price == 0:
            return 0.0

        # Normalised pct change, clamped to [-2, 2] to prevent runaway feedback
        signal = (newest_price - oldest_price) / oldest_price * 100.0
        return max(-2.0, min(2.0, signal))
