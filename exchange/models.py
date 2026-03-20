"""
models.py — Data models for the CUATS Exchange.

Defines the core value objects used throughout the matching engine:
    - Order: represents a single order submitted to the exchange.
    - Trade: represents a single fill (execution) between two orders.
    - OrderResult: returned by submit_order(); summarises what happened.
    - OrderStatus: returned by get_order_status(); snapshot of an order's state.

Design notes
------------
All models are plain Python objects with attribute access (no dataclasses or
third-party libraries) to stay within the standard-library constraint.

Each model implements __str__() as required by the submission guidelines so
that grading scripts can inspect them in human-readable form.

__repr__() delegates to __str__() for convenience in interactive use.
"""

from __future__ import annotations
from typing import List, Optional


class Order:
    """Represents a single order in the exchange.

    Attributes
    ----------
    order_id : int
        Unique identifier for the order.
    order_owner : str
        Identifier of the trader who placed the order.
    order_type : str
        One of ``"limit"``, ``"market"``, ``"fok"``, ``"ioc"``.
    side : str
        ``"buy"`` or ``"sell"``.
    asset : str
        The traded asset symbol (e.g. ``"ETHIOPIAN_YIRGACHEFFE"``).
    quantity : int
        Original quantity requested.
    price : float | None
        Limit price.  ``None`` for pure market orders.
    filled_quantity : int
        Cumulative quantity that has been executed so far.
    remaining_quantity : int
        Quantity still open (``quantity - filled_quantity``).
    status : str
        Current lifecycle state — one of ``"pending"``, ``"filled"``,
        ``"partial"``, ``"cancelled"``.
    timestamp : int
        Monotonic insertion counter assigned by the order book to enforce
        time priority.
    trades : list[Trade]
        Fills that this order has participated in.
    """

    #Saves memory since we have a fixed set of attributes and potentially many Order instances
    __slots__ = (
        "order_id", "order_owner", "order_type", "side", "asset",
        "quantity", "price", "filled_quantity", "remaining_quantity",
        "status", "timestamp", "trades",
    )

    def __init__(
        self,
        order_id: int,
        order_owner: str,
        order_type: str,
        side: str,
        asset: str,
        quantity: int,
        price: Optional[float] = None,
        timestamp: int = 0,
    ) -> None:
        self.order_id = order_id
        self.order_owner = order_owner
        self.order_type = order_type
        self.side = side
        self.asset = asset
        self.quantity = quantity
        self.price = price
        self.filled_quantity: int = 0
        self.remaining_quantity: int = quantity
        self.status: str = "pending"
        self.timestamp: int = timestamp
        self.trades: List[Trade] = []

    # --------------------------------------------------------------------- #
    # String representations                                                 #
    # --------------------------------------------------------------------- #
    def __str__(self) -> str:
        return (
            f"Order(order_id={self.order_id}, order_owner={self.order_owner!r}, "
            f"order_type={self.order_type!r}, side={self.side!r}, "
            f"asset={self.asset!r}, quantity={self.quantity}, "
            f"price={self.price}, filled={self.filled_quantity}, "
            f"remaining={self.remaining_quantity}, status={self.status!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class Trade:
    """A single execution (fill) between a buy order and a sell order.

    Attributes
    ----------
    asset : str
        The traded asset.
    price : float
        Execution price (always the resting order's price).
    quantity : int
        Number of units exchanged in this fill.
    buy_order_id : int
        The buyer's order id.
    sell_order_id : int
        The seller's order id.
    """

    __slots__ = ("asset", "price", "quantity", "buy_order_id", "sell_order_id")

    def __init__(
        self,
        asset: str,
        price: float,
        quantity: int,
        buy_order_id: int,
        sell_order_id: int,
    ) -> None:
        self.asset = asset
        self.price = price
        self.quantity = quantity
        self.buy_order_id = buy_order_id
        self.sell_order_id = sell_order_id

    def __str__(self) -> str:
        return (
            f"Trade(asset={self.asset!r}, price={self.price}, "
            f"quantity={self.quantity}, buy_order_id={self.buy_order_id}, "
            f"sell_order_id={self.sell_order_id})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class OrderResult:
    """Returned by ``submit_order`` to report what happened.

    Attributes
    ----------
    order_id : int
        The submitted order's id.
    status : str
        Final status after matching — ``"filled"``, ``"partial"``,
        ``"pending"``, or ``"cancelled"``.
    fills : list[Trade]
        The trades that occurred during matching.
    remaining_quantity : int
        Quantity left after matching (0 if fully filled).
    """

    __slots__ = ("order_id", "status", "fills", "remaining_quantity")

    def __init__(
        self,
        order_id: int,
        status: str,
        fills: Optional[List[Trade]] = None,
        remaining_quantity: int = 0,
    ) -> None:
        self.order_id = order_id
        self.status = status
        self.fills: List[Trade] = fills if fills is not None else []
        self.remaining_quantity = remaining_quantity

    def __str__(self) -> str:
        return (
            f"OrderResult(order_id={self.order_id}, status={self.status!r}, "
            f"fills={len(self.fills)}, remaining={self.remaining_quantity})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class OrderStatus:
    """Snapshot returned by ``get_order_status``.

    Attributes
    ----------
    order_id : int
    status : str
        ``"pending"``, ``"filled"``, ``"partial"``, ``"cancelled"``.
    filled_quantity : int
    remaining_quantity : int
    trades : list[Trade]
    """

    __slots__ = (
        "order_id", "status", "filled_quantity", "remaining_quantity", "trades",
    )

    def __init__(
        self,
        order_id: int,
        status: str,
        filled_quantity: int,
        remaining_quantity: int,
        trades: Optional[List[Trade]] = None,
    ) -> None:
        self.order_id = order_id
        self.status = status
        self.filled_quantity = filled_quantity
        self.remaining_quantity = remaining_quantity
        self.trades: List[Trade] = trades if trades is not None else []

    def __str__(self) -> str:
        return (
            f"OrderStatus(order_id={self.order_id}, status={self.status!r}, "
            f"filled={self.filled_quantity}, remaining={self.remaining_quantity}, "
            f"trades={len(self.trades)})"
        )

    def __repr__(self) -> str:
        return self.__str__()