from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Optional

from exchange.orderbook import OrderBook
from exchange.models import OrderResult, Trade


@dataclass
class AgentState:
    """Tracks an agent's position, P&L, and activity counters."""

    position: int = 0                                    # net units held (+ long, - short)
    cash: float = 0.0                                    # running cash balance
    active_order_ids: set = field(default_factory=set)   # order IDs currently resting on book
    total_orders_submitted: int = 0
    total_fills: int = 0
    total_volume_traded: int = 0

    @property
    def unrealized_pnl(self) -> Callable:
        """Returns a function that computes unrealized P&L given a mid price."""
        return lambda mid: self.cash + self.position * mid


class BaseAgent(ABC):
    """Abstract base for all trading agents.

    Subclasses implement ``step(current_step)`` to observe the market and act.
    All order submission / cancellation should go through the convenience
    methods on this class so that state bookkeeping is handled automatically.
    """

    def __init__(
        self,
        name: str,
        asset: str,
        orderbook: OrderBook,
        id_generator: Callable[[], int],
    ):
        self.name = name
        self.asset = asset
        self.ob = orderbook
        self._next_id = id_generator
        self.state = AgentState()
        self._fills_processed: dict[int, int] = {}  # order_id -> num fills already counted

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def step(self, current_step: int) -> None:
        """Called once per simulation step.  Observe the market, then act."""
        ...

    # ------------------------------------------------------------------
    # Order helpers (agents should use these, not self.ob directly)
    # ------------------------------------------------------------------

    def next_order_id(self) -> int:
        return self._next_id()

    def submit_order(
        self,
        order_type: str,
        side: str,
        quantity: int,
        price: Optional[float] = None,
    ) -> OrderResult:
        """Submit an order and auto-update internal state."""
        oid = self.next_order_id()
        result = self.ob.submit_order(
            order_id=oid,
            order_owner=self.name,
            order_type=order_type,
            side=side,
            asset=self.asset,
            quantity=quantity,
            price=price,
        )
        self.state.total_orders_submitted += 1

        # Process any fills (aggressive side)
        self._process_fills(result.fills, side)

        # Mark these fills as already processed so reconcile() won't double-count
        self._fills_processed[oid] = len(result.fills)

        # Track resting orders
        if result.status in ("pending", "partial"):
            self.state.active_order_ids.add(oid)
        else:
            self.state.active_order_ids.discard(oid)

        return result

    def cancel_order(self, order_id: int) -> bool:
        success = self.ob.cancel_order(order_id)
        if success:
            self.state.active_order_ids.discard(order_id)
        return success

    def cancel_all_orders(self) -> int:
        """Cancel every resting order.  Returns number successfully cancelled."""
        cancelled = 0
        for oid in list(self.state.active_order_ids):
            if self.cancel_order(oid):
                cancelled += 1
        return cancelled

    # ------------------------------------------------------------------
    # Market data helpers
    # ------------------------------------------------------------------

    def get_mid_price(self) -> Optional[float]:
        bid = self.ob.get_best_bid(self.asset)
        ask = self.ob.get_best_ask(self.asset)
        if bid is not None and ask is not None:
            return (bid[0] + ask[0]) / 2.0
        if bid is not None:
            return bid[0]
        if ask is not None:
            return ask[0]
        return None

    def get_best_bid(self) -> Optional[tuple]:
        return self.ob.get_best_bid(self.asset)

    def get_best_ask(self) -> Optional[tuple]:
        return self.ob.get_best_ask(self.asset)

    def get_spread(self) -> Optional[float]:
        return self.ob.get_spread(self.asset)

    def get_depth(self, levels: int = 5) -> dict:
        return self.ob.get_market_depth(self.asset, levels)

    def get_recent_trades(self, n: int = 10) -> list:
        return self.ob.get_trade_history(self.asset, limit=n)

    # ------------------------------------------------------------------
    # Internal bookkeeping
    # ------------------------------------------------------------------

    def _process_fills(self, fills: list[Trade], side: str) -> None:
        for trade in fills:
            qty = trade.quantity
            price = trade.price
            if side == "buy":
                self.state.position += qty
                self.state.cash -= price * qty
            else:
                self.state.position -= qty
                self.state.cash += price * qty
            self.state.total_fills += 1
            self.state.total_volume_traded += qty

    def reconcile(self) -> None:
        """Check all active orders for passive fills and update state.

        When another agent's order matches against this agent's resting
        order, the exchange updates the Order object in-place, but this
        agent's state (position, cash) is not updated.  This method
        reads the current status of every active order from the exchange
        and applies any new fills.
        """
        to_remove = []
        for oid in list(self.state.active_order_ids):
            status = self.ob.get_order_status(oid)
            if status is None:
                to_remove.append(oid)
                continue

            # Find new trades we haven't processed yet.
            prev_count = self._fills_processed.get(oid, 0)
            all_trades = status.trades
            new_trades = all_trades[prev_count:]
            self._fills_processed[oid] = len(all_trades)

            # Determine side from the original order
            order_obj = self.ob._ordersByID.get(oid)
            if order_obj is None:
                to_remove.append(oid)
                continue

            for trade in new_trades:
                qty = trade.quantity
                price = trade.price
                if order_obj.side == "buy":
                    self.state.position += qty
                    self.state.cash -= price * qty
                else:
                    self.state.position -= qty
                    self.state.cash += price * qty
                self.state.total_fills += 1
                self.state.total_volume_traded += qty

            # Clean up fully filled / cancelled orders
            if status.status in ("filled", "cancelled"):
                to_remove.append(oid)

        for oid in to_remove:
            self.state.active_order_ids.discard(oid)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name!r}, "
            f"pos={self.state.position}, cash={self.state.cash:.2f}, "
            f"orders={self.state.total_orders_submitted}, "
            f"fills={self.state.total_fills})"
        )
