from __future__ import annotations
from typing import Literal


class OrderBook:

    def view_orderbook(self) -> OrderBook | None:
        """
        Returns the current state of the limit order book for all assets.
        The returned OrderBook must provide read-only access to the data
        (e.g., using the @property decorator). The structure should allow
        inspection of both bid and ask sides, organized by asset and price level.

        Returns:
            OrderBook: A data structure (e.g., an object, a list, ... that allows
            read-only view of all pending limit orders.
        """
        pass

    def view_last_orders(self, n: int = 10000) -> list[Order]:
        """
        Returns the n most recent orders submitted to the exchange.
        Orders should be sorted by submission time, with the most recent
        order at index 0.

        Args:
            n: Maximum number of orders to return (default 10,000)

        Returns:
            list[Order]: Up to n most recent orders, newest first. For this and for the
            following returns, you are free to define the Order object in any way that you
            see fit. Please implement a __str__() method with all details of the order
            Examplary, but not mandatory to use this code snippet, :
            def __str__(self) -> str:
                return f"{asset=}, {side=}, {order_id=}, {quantity=}, etc..."
        """
        pass

    def view_largest_orders(self, n: int = 10000) -> list[Order]:
        """
        Returns the n largest orders by volume that have NOT been cancelled.
        Includes both pending orders and fully/partially executed orders.
        Sorted by volume descending (largest first).

        Args:
            n: Maximum number of orders to return (default 10,000)

        Returns:
            list[Order]: Up to n largest non-cancelled orders
        """
        pass

    def view_smallest_orders(self, n: int = 10000) -> list[Order]:
        """
        Returns the n smallest orders by volume that have NOT been cancelled.
        Includes both pending orders and fully/partially executed orders.
        Sorted by volume ascending (smallest first).

        Args:
            n: Maximum number of orders to return (default 10,000)

        Returns:
            list[Order]: Up to n smallest non-cancelled orders
        """
        pass

    def get_best_bid(self, asset: str) -> tuple[float, int] | None:
        """
        Returns the best (highest) bid price and total volume at that price.

        Args:
            asset: The asset symbol (e.g., "ETHIOPIAN_YIRGACHEFFE")

        Returns:
            tuple[float, int]: (price, volume) or None if no bids exist
        """
        pass

    def get_best_ask(self, asset: str) -> tuple[float, int] | None:
        """
        Returns the best (lowest) ask price and total volume at that price.

        Args:
            asset: The asset symbol

        Returns:
            tuple[float, int]: (price, volume) or None if no asks exist
        """
        pass

    def get_spread(self, asset: str) -> float | None:
        """
        Returns the bid-ask spread for an asset.

        Args:
            asset: The asset symbol

        Returns:
            float: The spread (best_ask - best_bid), or None if either side is empty
        """
        pass

    def get_market_depth(self, asset: str, levels: int = 5) -> dict:
        """
        Returns the market depth up to a specified number of price levels.

        Args:
            asset: The asset symbol
            levels: Number of price levels to return for each side

        Returns:
            dict: {
                "bids": [(price, volume), ...],  # sorted by price descending
                "asks": [(price, volume), ...],  # sorted by price ascending
            }
        """
        pass

    def get_volume_at_price(self, asset: str, price: float, side: Literal["bid", "ask"]) -> int:
        """
        Returns the total order volume at a specific price level.

        Args:
            asset: The asset symbol
            price: The price level to query
            side: "bid" or "ask"

        Returns:
            int: Total volume at that price level (0 if none)
        """
        pass

    def submit_order(
        self,
        order_id: int,
        order_owner: str,
        order_type: Literal["market", "limit", "fok", "ioc"],
        side: Literal["buy", "sell"],
        asset: str,
        quantity: int,
        price: float | None = None  # Required for limit orders, ignored for market orders
    ) -> OrderResult:
        """
        Submits an order to the exchange.
        The engine should immediately attempt to match the order against the
        existing book. Any unmatched quantity (for limit orders) should be
        added to the book.

        Note: An order_owner CAN trade with themselves (self-matching is allowed).

        Args:
            order_id: Unique identifier for this order
            order_owner: Identifier of the trader submitting the order
            order_type: One of "market", "limit", "fok" (fill-or-kill), "ioc" (immediate-or-cancel)
            side: "buy" or "sell"
            asset: The asset symbol to trade
            quantity: Number of units to buy/sell
            price: Limit price (required for "limit" and "fok" orders)

        Returns:
            OrderResult: Contains execution details (fills, remaining quantity, status)
        """
        pass

    def cancel_order(self, order_id: int) -> bool:
        """
        Cancels a pending order.
        If the order exists and has remaining unfilled quantity, it is removed
        from the order book. Partially filled orders can be cancelled (only the
        remaining quantity is affected).

        Args:
            order_id: The ID of the order to cancel

        Returns:
            bool: True if the order was successfully cancelled, False if the order
            doesn't exist or was already fully filled/cancelled
        """
        pass

    def modify_order(
        self,
        order_id: int,
        new_quantity: int | None = None,
        new_price: float | None = None
    ) -> bool:
        """
        Modifies an existing pending order.
        Price modifications cause the order to lose its time priority (it goes
        to the back of the queue at the new price level). Quantity reductions
        preserve time priority; quantity increases lose time priority.

        Args:
            order_id: The ID of the order to modify
            new_quantity: New quantity (must be > 0 and > already filled quantity)
            new_price: New limit price

        Returns:
            bool: True if modification succeeded, False otherwise
        """
        pass

    def get_trade_history(
        self,
        asset: str | None = None,
        limit: int = 1000
    ) -> list[Trade]:
        """
        Returns recent trades, optionally filtered by asset.

        Args:
            asset: Filter by asset symbol, or None for all assets
            limit: Maximum number of trades to return

        Returns:
            list[Trade]: Recent trades, most recent first
        """
        pass

    def get_order_status(self, order_id: int) -> OrderStatus | None:
        """
        Returns the current status of an order.

        Args:
            order_id: The order ID to query

        Returns:
            OrderStatus: Status info including fills, remaining quantity, state
            Returns None if order_id not found
        """
        pass