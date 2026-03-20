from __future__ import annotations
from typing import List, Optional


class Order:
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