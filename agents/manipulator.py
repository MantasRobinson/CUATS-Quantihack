from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Optional

from exchange.orderbook import OrderBook
from .base import BaseAgent


@dataclass
class ManipulatorConfig:
    """Tunable parameters for the market manipulator."""

    phase_length: int = 150         # steps per strategy phase
    cooldown_length: int = 50       # quiet steps between active phases

    # Spoofing
    spoof_size: int = 80            # quantity per spoof order (large to fake demand)
    spoof_levels: int = 4           # number of layered spoof orders
    spoof_offset: float = 0.30      # price spacing between spoof levels
    spoof_delay: int = 10           # steps to wait before cancelling spoof orders

    # Pump / dump
    pump_size: int = 25             # aggressive order size during pump
    pump_activity: float = 0.70     # probability of acting per step during pump
    dump_size: int = 30             # aggressive order size during dump

    # Wash trading
    wash_size: int = 5              # order size for wash trades
    wash_activity: float = 0.50     # probability of wash trade per step

    # Risk
    max_position: int = 300         # hard position cap


# Phase names for the cycle
_PHASES = ["spoof_buy", "pump", "dump", "wash", "cooldown",
           "spoof_sell", "pump_down", "dump_up", "wash", "cooldown"]


class ManipulatorAgent(BaseAgent):
    """A predatory trading agent that cycles through manipulation strategies.

    The manipulator creates visible, varied patterns in the trade data by
    cycling through five strategy types:

    1. **Spoof**: Place large fake orders to simulate demand/supply,
       then cancel and trade the other side.
    2. **Pump / Pump-down**: Aggressively buy (or sell) to drive the price.
    3. **Dump / Dump-up**: Unwind the accumulated position.
    4. **Wash trade**: Trade against own resting orders to inflate volume.
    5. **Cooldown**: Do nothing — creates visible quiet periods.

    The agent alternates between bullish and bearish manipulation cycles.
    """

    def __init__(
        self,
        name: str,
        asset: str,
        orderbook: OrderBook,
        id_generator: Callable[[], int],
        config: Optional[ManipulatorConfig] = None,
        rng_seed: Optional[int] = None,
    ):
        super().__init__(name, asset, orderbook, id_generator)
        self.config = config or ManipulatorConfig()
        self.rng = random.Random(rng_seed)

        self._phase_index = 0
        self._phase_step = 0
        self._spoof_order_ids: list[int] = []

    # ------------------------------------------------------------------
    # Phase management
    # ------------------------------------------------------------------

    @property
    def _current_phase(self) -> str:
        return _PHASES[self._phase_index % len(_PHASES)]

    def _advance_phase(self) -> None:
        """Move to the next phase, cancel all resting orders."""
        self.cancel_all_orders()
        self._spoof_order_ids.clear()
        self._phase_index = (self._phase_index + 1) % len(_PHASES)
        self._phase_step = 0

    def _phase_limit(self) -> int:
        if self._current_phase == "cooldown":
            return self.config.cooldown_length
        return self.config.phase_length

    # ------------------------------------------------------------------
    # Main step
    # ------------------------------------------------------------------

    def step(self, current_step: int) -> None:
        # Check phase transition
        if self._phase_step >= self._phase_limit():
            self._advance_phase()

        phase = self._current_phase

        if phase == "spoof_buy":
            self._step_spoof(side="buy")
        elif phase == "spoof_sell":
            self._step_spoof(side="sell")
        elif phase == "pump":
            self._step_aggressive("buy", self.config.pump_size)
        elif phase == "pump_down":
            self._step_aggressive("sell", self.config.pump_size)
        elif phase == "dump":
            self._step_unwind("sell", self.config.dump_size)
        elif phase == "dump_up":
            self._step_unwind("buy", self.config.dump_size)
        elif phase == "wash":
            self._step_wash()
        # cooldown: do nothing

        self._phase_step += 1

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    def _step_spoof(self, side: str) -> None:
        """Spoofing: place large fake orders, then cancel and trade the other side."""
        cfg = self.config
        mid = self.get_mid_price()
        if mid is None:
            return

        if self._phase_step == 0:
            # Place layered spoof orders
            self._spoof_order_ids.clear()
            for level in range(cfg.spoof_levels):
                offset = 0.10 + level * cfg.spoof_offset
                if side == "buy":
                    price = round(mid - offset, 2)
                else:
                    price = round(mid + offset, 2)

                if price <= 0:
                    continue

                result = self.submit_order("limit", side, cfg.spoof_size, price)
                if result.status in ("pending", "partial"):
                    self._spoof_order_ids.append(result.order_id)

        elif self._phase_step == cfg.spoof_delay:
            # Cancel all spoof orders and trade the other side aggressively
            for oid in self._spoof_order_ids:
                self.cancel_order(oid)
            self._spoof_order_ids.clear()

            # Trade the opposite side to profit from the price move
            opp_side = "sell" if side == "buy" else "buy"
            if self._position_ok(opp_side, cfg.pump_size):
                self.submit_order("market", opp_side, cfg.pump_size)

        elif self._phase_step > cfg.spoof_delay:
            # Continue trading the opposite side occasionally
            opp_side = "sell" if side == "buy" else "buy"
            if self.rng.random() < 0.3 and self._position_ok(opp_side, cfg.pump_size):
                self.submit_order("market", opp_side, cfg.pump_size // 2)

    def _step_aggressive(self, side: str, size: int) -> None:
        """Pump/pump-down: aggressive buying or selling to move the price."""
        if self.rng.random() > self.config.pump_activity:
            return

        if not self._position_ok(side, size):
            return

        self.submit_order("market", side, size)

    def _step_unwind(self, side: str, size: int) -> None:
        """Dump/dump-up: unwind accumulated position back toward zero."""
        # Only unwind if we have position to unwind
        if side == "sell" and self.state.position <= 0:
            return
        if side == "buy" and self.state.position >= 0:
            return

        # Size limited to remaining position
        actual_size = min(size, abs(self.state.position))
        if actual_size <= 0:
            return

        if self.rng.random() > self.config.pump_activity:
            return

        self.submit_order("market", side, actual_size)

    def _step_wash(self) -> None:
        """Wash trading: trade against own resting orders to inflate volume."""
        if self.rng.random() > self.config.wash_activity:
            return

        mid = self.get_mid_price()
        if mid is None:
            return

        cfg = self.config
        # Place a resting limit sell at the best ask (or slightly below)
        ask = self.get_best_ask()
        sell_price = round(ask[0] if ask else mid + 0.10, 2)

        result = self.submit_order("limit", "sell", cfg.wash_size, sell_price)

        # Now submit an aggressive buy that should cross it
        if result.status in ("pending", "partial"):
            self.submit_order("market", "buy", cfg.wash_size)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _position_ok(self, side: str, qty: int) -> bool:
        """Check if trading would breach the position cap."""
        pos = self.state.position
        if side == "buy" and pos + qty > self.config.max_position:
            return False
        if side == "sell" and pos - qty < -self.config.max_position:
            return False
        return True
