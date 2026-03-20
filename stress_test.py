"""
stress_test.py — Rigorous correctness + performance tests for the OrderBook.

Sections
--------
1. Invariant checker        — verifies internal book consistency after any op
2. Correctness tests        — edge cases, all order types, matching semantics
3. Stress / load tests      — high volumes of orders, cancels, modifications
4. Empirical complexity     — measures wall-clock scaling to validate Big-O claims

Run:  python stress_test.py
"""

from __future__ import annotations

import random
import sys
import time
from collections import deque

from exchange import OrderBook

# ========================================================================= #
# Helpers                                                                     #
# ========================================================================= #

_oid = 0


def fresh_id() -> int:
    """Globally unique order id."""
    global _oid
    _oid += 1
    return _oid


def reset_ids() -> None:
    global _oid
    _oid = 0


def check_invariants(ob: OrderBook, label: str = "") -> None:
    """Assert that every internal invariant of the book holds.

    Raises AssertionError with a descriptive message on failure.
    """
    for asset, book in ob._books.items():
        for side_name, side in [("bids", book.bids), ("asks", book.asks)]:
            tag = f"[{label}] {asset}.{side_name}" if label else f"{asset}.{side_name}"

            # sorted_prices and price_map must have the same keys
            assert set(side.sorted_prices) == set(side.price_map.keys()), (
                f"{tag}: sorted_prices keys mismatch price_map keys. "
                f"sorted={side.sorted_prices}, map_keys={list(side.price_map.keys())}"
            )

            # sorted_prices must actually be sorted ascending
            assert side.sorted_prices == sorted(side.sorted_prices), (
                f"{tag}: sorted_prices not sorted: {side.sorted_prices}"
            )

            # No empty deques in price_map
            for p, q in side.price_map.items():
                assert len(q) > 0, f"{tag}: empty deque at price {p}"

                # Every order in the queue should be resting (not filled/cancelled)
                for o in q:
                    assert o.status in ("pending", "partial"), (
                        f"{tag}: order {o.order_id} in queue has status "
                        f"{o.status!r}, expected pending/partial"
                    )
                    assert o.remaining_quantity > 0, (
                        f"{tag}: order {o.order_id} has remaining_quantity=0 but "
                        f"is still in the queue"
                    )
                    assert o.price == p, (
                        f"{tag}: order {o.order_id} has price {o.price} but "
                        f"is in bucket for price {p}"
                    )

            # No duplicate prices in sorted_prices
            assert len(side.sorted_prices) == len(set(side.sorted_prices)), (
                f"{tag}: duplicate prices in sorted_prices: {side.sorted_prices}"
            )

    # Every order: filled + remaining == quantity
    # Exception: market/IOC orders that partially fill have their remainder
    # zeroed out (cancelled), so filled + remaining < quantity is expected.
    for oid, order in ob._orders_by_id.items():
        if order.order_type in ("market", "ioc") and order.status in ("partial", "cancelled"):
            # Remainder was force-cancelled — skip this check
            assert order.filled_quantity <= order.quantity, (
                f"Order {oid}: filled({order.filled_quantity}) > "
                f"quantity({order.quantity})"
            )
        else:
            assert order.filled_quantity + order.remaining_quantity == order.quantity, (
                f"Order {oid}: filled({order.filled_quantity}) + "
                f"remaining({order.remaining_quantity}) != "
                f"quantity({order.quantity})"
            )

    # Trade consistency: every trade's quantity should be positive
    for t in ob._trades:
        assert t.quantity > 0, f"Trade has non-positive quantity: {t}"


PASS_COUNT = 0
FAIL_COUNT = 0


def run_test(name: str, fn) -> None:
    """Run a single test function, catch and report failures."""
    global PASS_COUNT, FAIL_COUNT
    try:
        fn()
        PASS_COUNT += 1
        print(f"  PASS  {name}")
    except Exception as e:
        FAIL_COUNT += 1
        print(f"  FAIL  {name}: {e}")


# ========================================================================= #
# Section 1 — Correctness Tests                                              #
# ========================================================================= #

def test_basic_limit_match():
    """Sell resting, buy aggresses — should produce one trade."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "alice", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "bob", "limit", "buy", "X", 100, 10.0)
    assert r.status == "filled"
    assert len(r.fills) == 1
    assert r.fills[0].quantity == 100
    assert r.fills[0].price == 10.0
    check_invariants(ob, "basic_limit_match")


def test_price_time_priority():
    """Earlier order at the same price fills first."""
    ob = OrderBook()
    id1, id2 = fresh_id(), fresh_id()
    ob.submit_order(id1, "a", "limit", "sell", "X", 50, 10.0)
    ob.submit_order(id2, "b", "limit", "sell", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 50, 10.0)
    assert r.fills[0].sell_order_id == id1, "First seller should fill first (time priority)"
    # Second seller still resting
    s2 = ob.get_order_status(id2)
    assert s2.status == "pending"
    check_invariants(ob, "price_time_priority")


def test_price_priority():
    """Better price fills before worse price, regardless of time."""
    ob = OrderBook()
    id_expensive = fresh_id()
    id_cheap = fresh_id()
    ob.submit_order(id_expensive, "a", "limit", "sell", "X", 50, 12.0)
    ob.submit_order(id_cheap, "b", "limit", "sell", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 50, 12.0)
    # Should match the cheaper ask (10.0) first
    assert r.fills[0].price == 10.0, f"Should fill at 10.0, got {r.fills[0].price}"
    assert r.fills[0].sell_order_id == id_cheap
    check_invariants(ob, "price_priority")


def test_partial_fill_resting():
    """Buy partially fills a larger resting sell."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 40, 10.0)
    assert r.status == "filled"
    s = ob.get_order_status(sell_id)
    assert s.status == "partial"
    assert s.filled_quantity == 40
    assert s.remaining_quantity == 60
    check_invariants(ob, "partial_fill_resting")


def test_multi_level_sweep():
    """Buy sweeps through multiple ask price levels."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 11.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 12.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 25, 12.0)
    assert r.status == "filled"
    assert len(r.fills) == 3
    assert r.fills[0].price == 10.0
    assert r.fills[1].price == 11.0
    assert r.fills[2].price == 12.0
    assert r.fills[2].quantity == 5
    check_invariants(ob, "multi_level_sweep")


def test_no_crossing():
    """Buy at 9.0 should NOT match a sell at 10.0."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 50, 9.0)
    assert r.status == "pending"
    assert len(r.fills) == 0
    check_invariants(ob, "no_crossing")


def test_market_order_fill():
    """Market buy fills against resting sells."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "market", "buy", "X", 50)
    assert r.status == "filled"
    assert len(r.fills) == 1
    check_invariants(ob, "market_fill")


def test_market_order_empty_book():
    """Market order on an empty book should be cancelled."""
    ob = OrderBook()
    r = ob.submit_order(fresh_id(), "a", "market", "buy", "X", 100)
    assert r.status == "cancelled"
    check_invariants(ob, "market_empty")


def test_market_order_partial():
    """Market order with insufficient liquidity: partial fill then cancel remainder."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 30, 10.0)
    r = ob.submit_order(fresh_id(), "b", "market", "buy", "X", 100)
    assert r.status == "partial"
    assert r.fills[0].quantity == 30
    assert r.remaining_quantity == 0  # remainder cancelled
    check_invariants(ob, "market_partial")


def test_ioc_full_fill():
    """IOC order that can be fully filled."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "ioc", "buy", "X", 50, 10.0)
    assert r.status == "filled"
    check_invariants(ob, "ioc_full")


def test_ioc_partial_fill():
    """IOC order partially fills, remainder cancelled."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 30, 10.0)
    r = ob.submit_order(fresh_id(), "b", "ioc", "buy", "X", 100, 10.0)
    assert r.status == "partial"
    assert r.remaining_quantity == 0  # leftover cancelled
    check_invariants(ob, "ioc_partial")


def test_ioc_no_fill():
    """IOC order with no matching liquidity — cancelled."""
    ob = OrderBook()
    r = ob.submit_order(fresh_id(), "a", "ioc", "buy", "X", 100, 10.0)
    assert r.status == "cancelled"
    check_invariants(ob, "ioc_no_fill")


def test_fok_full_fill():
    """FOK order that can be fully filled — executes."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "buy", "X", 100, 10.0)
    assert r.status == "filled"
    assert len(r.fills) == 1
    check_invariants(ob, "fok_full")


def test_fok_insufficient_liquidity():
    """FOK order that cannot be fully filled — rejected, book untouched."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "buy", "X", 100, 10.0)
    assert r.status == "cancelled"
    assert len(r.fills) == 0
    # Resting order should be completely untouched
    s = ob.get_order_status(sell_id)
    assert s.filled_quantity == 0
    assert s.remaining_quantity == 50
    check_invariants(ob, "fok_rejected")


def test_fok_exact_fill():
    """FOK order exactly matching available liquidity."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 30, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 70, 10.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "buy", "X", 100, 10.0)
    assert r.status == "filled"
    assert sum(f.quantity for f in r.fills) == 100
    check_invariants(ob, "fok_exact")


def test_fok_multi_level():
    """FOK across multiple price levels."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 50, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 50, 11.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "buy", "X", 100, 11.0)
    assert r.status == "filled"
    assert len(r.fills) == 2
    check_invariants(ob, "fok_multi_level")


def test_cancel_pending():
    """Cancel a pending (unfilled) order."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.cancel_order(oid) is True
    s = ob.get_order_status(oid)
    assert s.status == "cancelled"
    # Book should be empty now
    assert ob.get_best_ask("X") is None
    check_invariants(ob, "cancel_pending")


def test_cancel_partial():
    """Cancel a partially filled order."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 40, 10.0)
    assert ob.cancel_order(sell_id) is True
    s = ob.get_order_status(sell_id)
    assert s.status == "cancelled"
    check_invariants(ob, "cancel_partial")


def test_cancel_filled():
    """Cannot cancel a fully filled order."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 100, 10.0)
    assert ob.cancel_order(sell_id) is False
    check_invariants(ob, "cancel_filled")


def test_cancel_unknown():
    """Cancel non-existent order."""
    ob = OrderBook()
    assert ob.cancel_order(999999) is False


def test_cancel_idempotent():
    """Cancelling the same order twice: second returns False."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.cancel_order(oid) is True
    assert ob.cancel_order(oid) is False
    check_invariants(ob, "cancel_idempotent")


def test_modify_price_loses_priority():
    """Modifying price should lose time priority (re-queued at back)."""
    ob = OrderBook()
    id1, id2 = fresh_id(), fresh_id()
    ob.submit_order(id1, "a", "limit", "sell", "X", 50, 10.0)
    ob.submit_order(id2, "b", "limit", "sell", "X", 50, 10.0)
    # Modify id1's price — should go to back
    ob.modify_order(id1, new_price=10.0)  # same price but triggers re-queue
    # Actually, same price won't trigger re-queue. Let's change the price.
    # Re-test: change price then change back
    ob2 = OrderBook()
    id3, id4 = fresh_id(), fresh_id()
    ob2.submit_order(id3, "a", "limit", "sell", "X", 50, 10.0)
    ob2.submit_order(id4, "b", "limit", "sell", "X", 50, 10.0)
    ob2.modify_order(id3, new_price=10.5)
    ob2.modify_order(id3, new_price=10.0)  # back to original price, but lost priority
    r = ob2.submit_order(fresh_id(), "c", "limit", "buy", "X", 50, 10.0)
    assert r.fills[0].sell_order_id == id4, "id4 should fill first (id3 lost priority)"
    check_invariants(ob2, "modify_price_priority")


def test_modify_qty_decrease_keeps_priority():
    """Decreasing quantity should keep time priority."""
    ob = OrderBook()
    id1, id2 = fresh_id(), fresh_id()
    ob.submit_order(id1, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(id2, "b", "limit", "sell", "X", 50, 10.0)
    ob.modify_order(id1, new_quantity=60)  # decrease: keep priority
    r = ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 60, 10.0)
    assert r.fills[0].sell_order_id == id1, "id1 should still have priority"
    check_invariants(ob, "modify_qty_decrease")


def test_modify_qty_increase_loses_priority():
    """Increasing quantity should lose time priority."""
    ob = OrderBook()
    id1, id2 = fresh_id(), fresh_id()
    ob.submit_order(id1, "a", "limit", "sell", "X", 50, 10.0)
    ob.submit_order(id2, "b", "limit", "sell", "X", 50, 10.0)
    ob.modify_order(id1, new_quantity=60)  # increase: lose priority
    r = ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 50, 10.0)
    assert r.fills[0].sell_order_id == id2, "id2 should fill first (id1 lost priority)"
    check_invariants(ob, "modify_qty_increase")


def test_modify_below_filled_fails():
    """Cannot reduce quantity to or below already-filled amount."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 60, 10.0)
    assert ob.modify_order(sell_id, new_quantity=60) is False  # == filled
    assert ob.modify_order(sell_id, new_quantity=30) is False  # < filled
    assert ob.modify_order(sell_id, new_quantity=70) is True   # > filled
    check_invariants(ob, "modify_below_filled")


def test_modify_cancelled_order():
    """Cannot modify a cancelled order."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    ob.cancel_order(oid)
    assert ob.modify_order(oid, new_quantity=50) is False


def test_self_trade():
    """Self-trade is allowed per spec."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "alice", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "alice", "limit", "buy", "X", 100, 10.0)
    assert r.status == "filled"
    check_invariants(ob, "self_trade")


def test_multiple_assets():
    """Orders for different assets don't interact."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "COFFEE", 100, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "TEA", 100, 5.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "COFFEE", 100, 10.0)
    assert r.status == "filled"
    assert r.fills[0].asset == "COFFEE"
    # TEA should be untouched
    ask = ob.get_best_ask("TEA")
    assert ask is not None and ask[1] == 100
    check_invariants(ob, "multiple_assets")


def test_spread_empty():
    """Spread returns None when book is empty or one-sided."""
    ob = OrderBook()
    assert ob.get_spread("X") is None
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 100, 9.0)
    assert ob.get_spread("X") is None
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 11.0)
    spread = ob.get_spread("X")
    assert spread == 2.0
    check_invariants(ob, "spread_empty")


def test_market_depth():
    """Market depth returns correct levels."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 20, 11.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 30, 12.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 15, 9.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 25, 8.0)
    depth = ob.get_market_depth("X", levels=5)
    assert len(depth["asks"]) == 3
    assert len(depth["bids"]) == 2
    assert depth["asks"][0] == (10.0, 10)  # best ask first
    assert depth["bids"][0] == (9.0, 15)   # best bid first
    check_invariants(ob, "market_depth")


def test_trade_at_resting_price():
    """Trade executes at the resting order's price, not the aggressor's."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 100, 15.0)
    assert r.fills[0].price == 10.0, "Should trade at resting price (10), not aggressor (15)"
    check_invariants(ob, "resting_price")


def test_trade_history_ordering():
    """Trade history returns newest first."""
    ob = OrderBook()
    # Use separate assets to avoid cross-matching
    ob.submit_order(fresh_id(), "a", "limit", "sell", "A", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "A", 50, 10.0)   # trade @ 10 on A
    ob.submit_order(fresh_id(), "a", "limit", "sell", "A", 100, 11.0)
    ob.submit_order(fresh_id(), "c", "limit", "buy", "A", 50, 11.0)   # matches resting 10.0 first!
    # Second buy at 11.0 fills against the remaining 50 at 10.0 (better price),
    # so both trades are at 10.0.  Test global ordering instead:
    trades = ob.get_trade_history()
    assert len(trades) >= 2
    # Newest first: last trade should appear at index 0
    assert trades[0].buy_order_id > trades[1].buy_order_id, (
        "Newest trade should appear first"
    )


def test_order_status_unknown():
    """get_order_status for unknown id returns None."""
    ob = OrderBook()
    assert ob.get_order_status(999999) is None


def test_view_last_orders():
    """view_last_orders returns newest first."""
    ob = OrderBook()
    ids = [fresh_id() for _ in range(5)]
    for i, oid in enumerate(ids):
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0 + i)
    last = ob.view_last_orders(3)
    assert len(last) == 3
    assert last[0].order_id == ids[-1]  # newest


def test_view_largest_orders():
    """view_largest_orders returns by descending quantity."""
    ob = OrderBook()
    for q in [10, 50, 30, 20, 40]:
        ob.submit_order(fresh_id(), "a", "limit", "sell", "X", q, 10.0)
    largest = ob.view_largest_orders(3)
    assert [o.quantity for o in largest] == [50, 40, 30]


def test_bid_ask_volume_aggregation():
    """Best bid/ask volume sums across multiple orders at the same price."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 30, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 70, 10.0)
    bid = ob.get_best_bid("X")
    assert bid == (10.0, 100)
    check_invariants(ob, "volume_aggregation")


def test_cancel_cleans_price_level():
    """Cancelling the last order at a price level removes that level entirely."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 11.0)
    ob.cancel_order(oid)
    ask = ob.get_best_ask("X")
    assert ask[0] == 11.0, "After cancel, best ask should be 11.0"
    check_invariants(ob, "cancel_cleans_level")


def test_many_cancels_then_match():
    """Cancel many orders, then ensure matching still works correctly."""
    ob = OrderBook()
    ids = []
    for i in range(100):
        oid = fresh_id()
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0 + i * 0.01)
        ids.append(oid)
    # Cancel all but the last
    for oid in ids[:-1]:
        ob.cancel_order(oid)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 10, 11.0)
    assert r.status == "filled"
    assert r.fills[0].quantity == 10
    check_invariants(ob, "many_cancels_then_match")


# ========================================================================= #
# Section 2 — Stress / Load Tests                                            #
# ========================================================================= #

def test_stress_high_volume_submit():
    """Submit 50k limit orders alternating sides — book should stay consistent."""
    ob = OrderBook()
    N = 50_000
    for i in range(N):
        side = "buy" if i % 2 == 0 else "sell"
        # Wide spread so nothing matches
        price = 5.0 if side == "buy" else 15.0
        ob.submit_order(fresh_id(), f"t{i}", "limit", side, "X", 10, price)
    check_invariants(ob, "high_volume_submit")
    assert ob.get_best_bid("X") == (5.0, 10 * (N // 2))
    assert ob.get_best_ask("X") == (15.0, 10 * (N // 2))


def test_stress_many_price_levels():
    """1000 distinct price levels per side — tests bisect performance."""
    ob = OrderBook()
    for i in range(1000):
        ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 100.0 + i * 0.01)
    for i in range(1000):
        ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 10, 50.0 + i * 0.01)
    check_invariants(ob, "many_price_levels")
    ask = ob.get_best_ask("X")
    assert ask[0] == 100.0
    bid = ob.get_best_bid("X")
    assert abs(bid[0] - 59.99) < 0.001


def test_stress_deep_single_level():
    """10k orders at the same price — tests deque performance."""
    ob = OrderBook()
    N = 10_000
    ids = []
    for i in range(N):
        oid = fresh_id()
        ob.submit_order(oid, f"t{i}", "limit", "sell", "X", 1, 10.0)
        ids.append(oid)
    # Cancel every other order
    for i in range(0, N, 2):
        ob.cancel_order(ids[i])
    check_invariants(ob, "deep_single_level")
    # Match against remaining
    r = ob.submit_order(fresh_id(), "buyer", "limit", "buy", "X", N // 2, 10.0)
    assert r.status == "filled"
    assert sum(f.quantity for f in r.fills) == N // 2
    check_invariants(ob, "deep_single_level_post_match")


def test_stress_rapid_submit_cancel():
    """Submit and immediately cancel 20k orders."""
    ob = OrderBook()
    for i in range(20_000):
        oid = fresh_id()
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0 + (i % 100) * 0.01)
        ob.cancel_order(oid)
    check_invariants(ob, "rapid_submit_cancel")
    # Book should be completely empty
    assert ob.get_best_ask("X") is None
    assert ob.get_best_bid("X") is None


def test_stress_large_sweep():
    """Single large market order sweeping 500 price levels."""
    ob = OrderBook()
    total_qty = 0
    for i in range(500):
        qty = random.randint(1, 20)
        total_qty += qty
        ob.submit_order(fresh_id(), "a", "limit", "sell", "X", qty, 10.0 + i * 0.01)
    r = ob.submit_order(fresh_id(), "b", "market", "buy", "X", total_qty)
    assert r.status == "filled"
    assert sum(f.quantity for f in r.fills) == total_qty
    # Book should be empty
    assert ob.get_best_ask("X") is None
    check_invariants(ob, "large_sweep")


def test_stress_multi_asset():
    """Orders across 50 different assets."""
    ob = OrderBook()
    assets = [f"ASSET_{i}" for i in range(50)]
    for a in assets:
        for j in range(100):
            ob.submit_order(fresh_id(), "a", "limit", "sell", a, 10, 10.0 + j * 0.1)
            ob.submit_order(fresh_id(), "b", "limit", "buy", a, 10, 5.0 + j * 0.1)
    check_invariants(ob, "multi_asset")
    for a in assets:
        assert ob.get_best_ask(a) is not None
        assert ob.get_best_bid(a) is not None


def test_stress_fok_repeated():
    """Repeatedly submit FOK orders that fail — book must remain untouched."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 50, 10.0)
    for _ in range(1000):
        r = ob.submit_order(fresh_id(), "b", "fok", "buy", "X", 100, 10.0)
        assert r.status == "cancelled"
    # Resting order should be completely untouched after 1000 failed FOKs
    s = ob.get_order_status(sell_id)
    assert s.filled_quantity == 0
    assert s.remaining_quantity == 50
    check_invariants(ob, "fok_repeated")


def test_stress_interleaved_operations():
    """Mixed submits, cancels, modifies, and queries under load."""
    ob = OrderBook()
    rng = random.Random(42)
    live_orders = []
    for i in range(10_000):
        op = rng.random()
        if op < 0.5 or not live_orders:
            # Submit
            oid = fresh_id()
            side = rng.choice(["buy", "sell"])
            price = round(rng.uniform(8.0, 12.0), 2)
            qty = rng.randint(1, 100)
            ob.submit_order(oid, "trader", "limit", side, "X", qty, price)
            live_orders.append(oid)
        elif op < 0.7:
            # Cancel random order
            oid = rng.choice(live_orders)
            ob.cancel_order(oid)
        elif op < 0.85:
            # Modify random order
            oid = rng.choice(live_orders)
            new_qty = rng.randint(1, 200)
            ob.modify_order(oid, new_quantity=new_qty)
        else:
            # Query
            ob.get_best_bid("X")
            ob.get_best_ask("X")
            ob.get_spread("X")
            ob.get_market_depth("X", levels=5)

    check_invariants(ob, "interleaved_operations")


# ========================================================================= #
# Section 3 — Adversarial & Hard Edge Cases                                  #
# ========================================================================= #

# ---- 3a: Order ID collision / reuse ------------------------------------

def test_order_id_collision_ghost_in_book():
    """Submit same order_id twice — first order becomes a ghost in the book.

    The second submit overwrites _orders_by_id, but the first order's
    Order object is still sitting in the deque.  This creates a ghost:
    an order that will match but whose status updates go to an object
    no longer reachable via get_order_status().
    """
    ob = OrderBook()
    SHARED_ID = fresh_id()
    # First submit: sell 100 @ 10.0 — rests on the book
    r1 = ob.submit_order(SHARED_ID, "alice", "limit", "sell", "X", 100, 10.0)
    assert r1.status == "pending"

    # Second submit with SAME id: buy 50 @ 5.0 — rests on book (no cross)
    r2 = ob.submit_order(SHARED_ID, "bob", "limit", "buy", "X", 50, 5.0)
    assert r2.status == "pending"

    # _orders_by_id now points to the buy order
    status = ob.get_order_status(SHARED_ID)
    assert status.status == "pending"

    # The original sell is still in the ask deque as a ghost.
    # A buy that crosses should match it:
    buyer_id = fresh_id()
    r3 = ob.submit_order(buyer_id, "charlie", "limit", "buy", "X", 100, 10.0)

    # The ghost sell DOES get matched — trade happens
    assert r3.status == "filled"
    assert r3.fills[0].quantity == 100

    # But get_order_status(SHARED_ID) still shows the SECOND order (the buy),
    # not the ghost sell that just filled. The ghost is invisible.
    status2 = ob.get_order_status(SHARED_ID)
    assert status2.filled_quantity == 0, (
        "The ghost sell filled, but get_order_status shows the overwritten buy"
    )


def test_order_id_collision_cancel():
    """Cancel after ID collision — cancels the second order, ghost remains."""
    ob = OrderBook()
    SHARED_ID = fresh_id()
    ob.submit_order(SHARED_ID, "alice", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(SHARED_ID, "bob", "limit", "buy", "X", 50, 5.0)

    # Cancel tries to remove the BUY (second) from the bid side
    result = ob.cancel_order(SHARED_ID)
    assert result is True

    # The ghost sell at 10.0 is still in the ask deque
    ask = ob.get_best_ask("X")
    assert ask is not None, "Ghost sell should still be resting on the ask side"
    assert ask[0] == 10.0


# ---- 3b: Floating point precision attacks ------------------------------

def test_float_precision_near_prices():
    """Prices that differ by floating point epsilon should be separate levels."""
    ob = OrderBook()
    p1 = 10.0
    p2 = 10.0 + 1e-15  # differ by machine epsilon
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, p1)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, p2)

    book = ob._get_book("X")
    # Depending on whether Python treats them as equal
    if p1 == p2:
        assert len(book.asks.sorted_prices) == 1
    else:
        assert len(book.asks.sorted_prices) == 2
    check_invariants(ob, "float_precision_near")


def test_float_accumulation_error():
    """0.1 * 10 != 1.0 in floating point — tests bisect with imprecise sums."""
    ob = OrderBook()
    # Build prices via accumulation: 10.0, 10.1, 10.2, ..., 10.9
    price = 10.0
    ids = []
    for i in range(10):
        oid = fresh_id()
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, round(price, 10))
        ids.append(oid)
        price += 0.1

    # The 10th price should be ~11.0 but floating point might disagree
    check_invariants(ob, "float_accumulation")
    ask = ob.get_best_ask("X")
    assert ask[0] == 10.0


def test_negative_and_zero_price():
    """Prices of 0.0 and negative values — should still be handled by bisect."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 0.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, -5.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 0.01)
    ask = ob.get_best_ask("X")
    assert ask[0] == -5.0, f"Best ask should be -5.0, got {ask[0]}"
    check_invariants(ob, "negative_zero_price")


# ---- 3c: Crossed / locked book via modify -----------------------------

def test_modify_creates_crossed_book():
    """Modify a bid price above the best ask — should create a crossed book.

    The engine does NOT re-match after modify, so this produces a state
    where bid >= ask (a crossed book). This is a known design choice but
    tests that the book doesn't crash.
    """
    ob = OrderBook()
    bid_id = fresh_id()
    ob.submit_order(bid_id, "a", "limit", "buy", "X", 100, 9.0)
    ob.submit_order(fresh_id(), "b", "limit", "sell", "X", 100, 10.0)

    # Modify bid up to 11.0 — crosses the ask at 10.0
    ob.modify_order(bid_id, new_price=11.0)

    bid = ob.get_best_bid("X")
    ask = ob.get_best_ask("X")
    assert bid[0] == 11.0
    assert ask[0] == 10.0
    spread = ob.get_spread("X")
    assert spread < 0, f"Book should be crossed (negative spread), got {spread}"

    # A new sell at 10.5 should match the resting bid at 11.0
    r = ob.submit_order(fresh_id(), "c", "limit", "sell", "X", 50, 10.5)
    assert r.status == "filled"
    assert r.fills[0].price == 11.0, "Should trade at resting bid price 11.0"
    check_invariants(ob, "crossed_book")


def test_modify_creates_locked_book():
    """Modify bid to exactly the ask price — locked book (spread = 0)."""
    ob = OrderBook()
    bid_id = fresh_id()
    ob.submit_order(bid_id, "a", "limit", "buy", "X", 100, 9.0)
    ob.submit_order(fresh_id(), "b", "limit", "sell", "X", 100, 10.0)
    ob.modify_order(bid_id, new_price=10.0)

    spread = ob.get_spread("X")
    assert spread == 0.0, f"Locked book should have spread=0, got {spread}"
    check_invariants(ob, "locked_book")


# ---- 3d: Sell-side order types (mirror of buy tests) -------------------

def test_market_sell():
    """Market sell against resting bids."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 100, 10.0)
    r = ob.submit_order(fresh_id(), "b", "market", "sell", "X", 60)
    assert r.status == "filled"
    assert r.fills[0].quantity == 60
    assert r.fills[0].price == 10.0
    check_invariants(ob, "market_sell")


def test_market_sell_empty():
    """Market sell on empty bid book."""
    ob = OrderBook()
    r = ob.submit_order(fresh_id(), "a", "market", "sell", "X", 100)
    assert r.status == "cancelled"


def test_market_sell_multi_level():
    """Market sell sweeps multiple bid levels (highest first)."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 10, 8.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 10, 9.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 10, 10.0)
    r = ob.submit_order(fresh_id(), "b", "market", "sell", "X", 25)
    assert r.status == "filled"
    assert r.fills[0].price == 10.0  # best bid first
    assert r.fills[1].price == 9.0
    assert r.fills[2].price == 8.0
    assert r.fills[2].quantity == 5
    check_invariants(ob, "market_sell_multi")


def test_ioc_sell():
    """IOC sell partially fills, remainder cancelled."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 30, 10.0)
    r = ob.submit_order(fresh_id(), "b", "ioc", "sell", "X", 100, 10.0)
    assert r.status == "partial"
    assert r.remaining_quantity == 0
    check_invariants(ob, "ioc_sell")


def test_fok_sell():
    """FOK sell succeeds when bids have enough volume."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 50, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "sell", "X", 100, 10.0)
    assert r.status == "filled"
    assert sum(f.quantity for f in r.fills) == 100
    check_invariants(ob, "fok_sell")


def test_fok_sell_rejected():
    """FOK sell rejected when insufficient bid liquidity."""
    ob = OrderBook()
    bid_id = fresh_id()
    ob.submit_order(bid_id, "a", "limit", "buy", "X", 50, 10.0)
    r = ob.submit_order(fresh_id(), "b", "fok", "sell", "X", 100, 10.0)
    assert r.status == "cancelled"
    s = ob.get_order_status(bid_id)
    assert s.filled_quantity == 0
    check_invariants(ob, "fok_sell_rejected")


def test_fok_sell_multi_level():
    """FOK sell across multiple descending bid levels."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 30, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 30, 9.0)
    ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 30, 8.0)
    # FOK sell 80 @ 8.0 — needs 10+9+8 levels, total 90 available
    r = ob.submit_order(fresh_id(), "b", "fok", "sell", "X", 80, 8.0)
    assert r.status == "filled"
    assert sum(f.quantity for f in r.fills) == 80
    check_invariants(ob, "fok_sell_multi_level")


# ---- 3e: Cancel / modify non-resting orders ---------------------------

def test_cancel_market_order():
    """A filled market order should not be cancellable."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 100, 10.0)
    mkt_id = fresh_id()
    ob.submit_order(mkt_id, "b", "market", "buy", "X", 100)
    assert ob.cancel_order(mkt_id) is False


def test_cancel_ioc_order():
    """A cancelled IOC order (no fill) should not be re-cancellable."""
    ob = OrderBook()
    ioc_id = fresh_id()
    ob.submit_order(ioc_id, "a", "ioc", "buy", "X", 100, 10.0)
    assert ob.cancel_order(ioc_id) is False


def test_modify_market_order():
    """Cannot modify a market order (it never rests)."""
    ob = OrderBook()
    mkt_id = fresh_id()
    ob.submit_order(mkt_id, "a", "market", "buy", "X", 100)
    assert ob.modify_order(mkt_id, new_quantity=50) is False


def test_modify_filled_order():
    """Cannot modify a fully filled order."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 100, 10.0)
    assert ob.modify_order(sell_id, new_quantity=200) is False
    assert ob.modify_order(sell_id, new_price=11.0) is False


# ---- 3f: Modify edge cases --------------------------------------------

def test_modify_price_only():
    """Modify only the price (no quantity change)."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.modify_order(oid, new_price=11.0) is True
    ask = ob.get_best_ask("X")
    assert ask[0] == 11.0
    s = ob.get_order_status(oid)
    assert s.remaining_quantity == 100
    check_invariants(ob, "modify_price_only")


def test_modify_both_price_and_quantity():
    """Modify price and quantity simultaneously."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.modify_order(oid, new_quantity=50, new_price=11.0) is True
    ask = ob.get_best_ask("X")
    assert ask == (11.0, 50)
    check_invariants(ob, "modify_both")


def test_modify_into_existing_price_level():
    """Modify price to a level that already has other orders — goes to back."""
    ob = OrderBook()
    id1 = fresh_id()
    id2 = fresh_id()
    ob.submit_order(id1, "a", "limit", "sell", "X", 50, 11.0)
    ob.submit_order(id2, "b", "limit", "sell", "X", 50, 10.0)
    # Move id1 from 11.0 to 10.0 — should be behind id2 at price 10.0
    ob.modify_order(id1, new_price=10.0)
    r = ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 50, 10.0)
    assert r.fills[0].sell_order_id == id2, "id2 was already at 10.0, should fill first"
    check_invariants(ob, "modify_into_existing")


def test_modify_partial_fill_then_modify():
    """Partial fill, then modify remaining quantity — check consistency."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 100, 10.0)
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 40, 10.0)  # partial fill
    # Remaining = 60. Modify to 80 total (increase from 100). filled=40, remaining->40
    assert ob.modify_order(sell_id, new_quantity=80) is True
    s = ob.get_order_status(sell_id)
    assert s.filled_quantity == 40
    assert s.remaining_quantity == 40  # 80 - 40
    check_invariants(ob, "modify_after_partial")


def test_modify_quantity_to_zero():
    """Modify quantity to 0 — should fail."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.modify_order(oid, new_quantity=0) is False
    check_invariants(ob, "modify_qty_zero")


def test_modify_negative_quantity():
    """Modify to negative quantity — should fail."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 100, 10.0)
    assert ob.modify_order(oid, new_quantity=-10) is False


# ---- 3g: Quantity edge cases -------------------------------------------

def test_quantity_one():
    """Single-unit orders and fills."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 1, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 1, 10.0)
    assert r.status == "filled"
    assert r.fills[0].quantity == 1
    check_invariants(ob, "qty_one")


def test_huge_quantity():
    """Very large quantities (Python arbitrary precision)."""
    ob = OrderBook()
    BIG = 10**15
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", BIG, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", BIG, 10.0)
    assert r.status == "filled"
    assert r.fills[0].quantity == BIG
    check_invariants(ob, "huge_qty")


def test_one_massive_vs_many_tiny():
    """1 buy for 10000 vs 10000 sells of 1 — stress the fill loop."""
    ob = OrderBook()
    N = 10_000
    for i in range(N):
        ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 1, 10.0)
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", N, 10.0)
    assert r.status == "filled"
    assert len(r.fills) == N
    assert sum(f.quantity for f in r.fills) == N
    check_invariants(ob, "massive_vs_tiny")


def test_many_tiny_vs_one_massive():
    """10000 buys of 1 vs 1 sell of 10000 — each buy partially fills the sell."""
    ob = OrderBook()
    N = 10_000
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", N, 10.0)
    for i in range(N):
        r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 1, 10.0)
        assert r.status == "filled"
    s = ob.get_order_status(sell_id)
    assert s.status == "filled"
    assert s.filled_quantity == N
    check_invariants(ob, "tiny_vs_massive")


# ---- 3h: Global trade conservation ------------------------------------

def test_trade_volume_conservation():
    """Total buy fill volume == total sell fill volume across all trades."""
    ob = OrderBook()
    rng = random.Random(99)
    for _ in range(5000):
        side = rng.choice(["buy", "sell"])
        otype = rng.choice(["limit", "market", "ioc"])
        price = round(rng.uniform(9.0, 11.0), 2) if otype != "market" else None
        qty = rng.randint(1, 50)
        ob.submit_order(fresh_id(), "t", otype, side, "X", qty, price)

    # Every trade has a buy side and sell side with the same quantity
    for t in ob._trades:
        assert t.quantity > 0
        assert t.buy_order_id != 0
        assert t.sell_order_id != 0

    # Verify orders: sum of filled quantities on buy side == sell side
    total_buy_filled = 0
    total_sell_filled = 0
    for oid, order in ob._orders_by_id.items():
        if order.side == "buy":
            total_buy_filled += order.filled_quantity
        else:
            total_sell_filled += order.filled_quantity

    # NOTE: ID collisions from reuse could break this — but fresh_id() is unique
    total_trade_volume = sum(t.quantity for t in ob._trades)
    assert total_buy_filled == total_trade_volume, (
        f"Buy filled {total_buy_filled} != trade volume {total_trade_volume}"
    )
    assert total_sell_filled == total_trade_volume, (
        f"Sell filled {total_sell_filled} != trade volume {total_trade_volume}"
    )
    check_invariants(ob, "volume_conservation")


def test_per_asset_trade_isolation():
    """Trades for asset A should not affect asset B's book or volumes."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "A", 100, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "B", 100, 10.0)

    # Buy on A — should only affect A
    ob.submit_order(fresh_id(), "b", "limit", "buy", "A", 100, 10.0)

    assert ob.get_best_ask("A") is None  # fully consumed
    ask_b = ob.get_best_ask("B")
    assert ask_b is not None and ask_b[1] == 100  # untouched

    trades = ob.get_trade_history("A")
    assert len(trades) == 1
    trades_b = ob.get_trade_history("B")
    assert len(trades_b) == 0
    check_invariants(ob, "asset_isolation")


# ---- 3i: Adversarial cancel patterns ----------------------------------

def test_cancel_head_of_queue():
    """Cancel the first (oldest) order in a deque."""
    ob = OrderBook()
    ids = [fresh_id() for _ in range(5)]
    for oid in ids:
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0)
    ob.cancel_order(ids[0])  # remove head
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 10, 10.0)
    assert r.fills[0].sell_order_id == ids[1]  # second order fills
    check_invariants(ob, "cancel_head")


def test_cancel_tail_of_queue():
    """Cancel the last (newest) order in a deque."""
    ob = OrderBook()
    ids = [fresh_id() for _ in range(5)]
    for oid in ids:
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0)
    ob.cancel_order(ids[-1])  # remove tail
    # Fill all remaining
    r = ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 40, 10.0)
    assert r.status == "filled"
    filled_ids = {f.sell_order_id for f in r.fills}
    assert ids[-1] not in filled_ids
    check_invariants(ob, "cancel_tail")


def test_cancel_all_at_price_level():
    """Cancel every order at a price level — level should be removed."""
    ob = OrderBook()
    ids = [fresh_id() for _ in range(10)]
    for oid in ids:
        ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 11.0)

    for oid in ids:
        ob.cancel_order(oid)

    ask = ob.get_best_ask("X")
    assert ask[0] == 11.0, "All orders at 10.0 cancelled, best ask should be 11.0"
    check_invariants(ob, "cancel_all_at_level")


def test_cancel_alternating_pattern():
    """Cancel every other order — tests deque.remove in different positions."""
    ob = OrderBook()
    N = 100
    ids = []
    for i in range(N):
        oid = fresh_id()
        ob.submit_order(oid, "a", "limit", "sell", "X", 1, 10.0)
        ids.append(oid)
    for i in range(0, N, 2):
        ob.cancel_order(ids[i])
    check_invariants(ob, "cancel_alternating")
    vol = ob.get_volume_at_price("X", 10.0, "ask")
    assert vol == N // 2


# ---- 3j: Realistic trading patterns -----------------------------------

def test_market_maker_pattern():
    """Symmetric bid/ask ladder, sweep, replenish — typical MM workflow."""
    ob = OrderBook()
    mm_ids = []

    # Place symmetric ladder: bids 9.0-9.9, asks 10.1-11.0
    for i in range(10):
        bid_id = fresh_id()
        ask_id = fresh_id()
        ob.submit_order(bid_id, "mm", "limit", "buy", "X", 100, 9.0 + i * 0.1)
        ob.submit_order(ask_id, "mm", "limit", "sell", "X", 100, 10.1 + i * 0.1)
        mm_ids.extend([bid_id, ask_id])

    # Aggressive buy sweeps 3 ask levels
    r = ob.submit_order(fresh_id(), "aggr", "market", "buy", "X", 300)
    assert r.status == "filled"
    assert len(r.fills) == 3
    assert r.fills[0].price == 10.1  # best ask first

    # MM replenishes
    for i in range(3):
        ob.submit_order(fresh_id(), "mm", "limit", "sell", "X", 100, 10.1 + i * 0.1)

    check_invariants(ob, "mm_pattern")
    ask = ob.get_best_ask("X")
    assert ask[0] == 10.1


def test_cascading_partial_fill_then_rest():
    """Buy partially fills across levels, then rests the remainder."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.5)
    # No sells at 11.0+

    # Buy 30 @ 11.0: fills 10 @ 10.0, fills 10 @ 10.5, rests 10 @ 11.0
    buy_id = fresh_id()
    r = ob.submit_order(buy_id, "b", "limit", "buy", "X", 30, 11.0)
    assert r.status == "partial"
    assert len(r.fills) == 2
    assert r.remaining_quantity == 10

    bid = ob.get_best_bid("X")
    assert bid == (11.0, 10), f"Remainder should rest at 11.0, got {bid}"
    check_invariants(ob, "cascade_then_rest")


def test_interleaved_fill_modify_fill():
    """Partially fill, modify, then fill more — tests order consistency."""
    ob = OrderBook()
    sell_id = fresh_id()
    ob.submit_order(sell_id, "a", "limit", "sell", "X", 200, 10.0)

    # First partial fill
    ob.submit_order(fresh_id(), "b", "limit", "buy", "X", 50, 10.0)
    s = ob.get_order_status(sell_id)
    assert s.filled_quantity == 50 and s.remaining_quantity == 150

    # Modify: decrease total to 120 (remaining becomes 120 - 50 = 70)
    ob.modify_order(sell_id, new_quantity=120)
    s = ob.get_order_status(sell_id)
    assert s.remaining_quantity == 70

    # Fill the rest
    ob.submit_order(fresh_id(), "c", "limit", "buy", "X", 70, 10.0)
    s = ob.get_order_status(sell_id)
    assert s.status == "filled"
    assert s.filled_quantity == 120
    check_invariants(ob, "fill_modify_fill")


# ---- 3k: Stress with adversarial patterns -----------------------------

def test_stress_worst_case_cancel_from_middle():
    """Cancel from the middle of a deep deque repeatedly — O(Q) each time."""
    ob = OrderBook()
    N = 5_000
    ids = []
    for i in range(N):
        oid = fresh_id()
        ob.submit_order(oid, "a", "limit", "sell", "X", 1, 10.0)
        ids.append(oid)
    # Cancel from the middle outwards (worst case for deque.remove each time)
    mid = N // 2
    for offset in range(N // 2):
        if mid + offset < N:
            ob.cancel_order(ids[mid + offset])
        if mid - offset - 1 >= 0:
            ob.cancel_order(ids[mid - offset - 1])
    check_invariants(ob, "cancel_from_middle")
    assert ob.get_best_ask("X") is None


def test_stress_price_oscillation():
    """Rapidly changing best price — tests sorted_prices churn."""
    ob = OrderBook()
    for cycle in range(100):
        ids = []
        # Add 10 levels
        for i in range(10):
            oid = fresh_id()
            ob.submit_order(oid, "a", "limit", "sell", "X", 10, 10.0 + i * 0.01)
            ids.append(oid)
        # Sweep them all
        ob.submit_order(fresh_id(), "b", "market", "buy", "X", 100)
    check_invariants(ob, "price_oscillation")
    assert ob.get_best_ask("X") is None


def test_stress_many_assets_simultaneous_matching():
    """Heavy matching across 20 assets simultaneously."""
    ob = OrderBook()
    assets = [f"A{i}" for i in range(20)]
    # Seed each asset with 500 sells
    for a in assets:
        for i in range(500):
            ob.submit_order(fresh_id(), "seller", "limit", "sell", a, 10,
                            10.0 + i * 0.01)
    # Sweep all of each asset with market orders
    for a in assets:
        r = ob.submit_order(fresh_id(), "buyer", "market", "buy", a, 5000)
        assert r.status == "filled"
        assert sum(f.quantity for f in r.fills) == 5000
    check_invariants(ob, "multi_asset_matching")


def test_stress_random_fok_ioc_market_mix():
    """High volume random mix of FOK, IOC, market, and limit orders."""
    ob = OrderBook()
    rng = random.Random(77)
    for _ in range(20_000):
        otype = rng.choice(["limit", "limit", "limit", "market", "ioc", "fok"])
        side = rng.choice(["buy", "sell"])
        qty = rng.randint(1, 50)
        price = round(rng.uniform(9.5, 10.5), 2) if otype != "market" else None
        ob.submit_order(fresh_id(), "t", otype, side, "X", qty, price)
    check_invariants(ob, "random_all_types")


def test_stress_modify_storm():
    """Rapidly modify the same order many times."""
    ob = OrderBook()
    oid = fresh_id()
    ob.submit_order(oid, "a", "limit", "sell", "X", 1000, 10.0)
    for i in range(1000):
        new_price = 10.0 + (i % 50) * 0.1
        new_qty = 500 + (i % 500)
        ob.modify_order(oid, new_quantity=new_qty, new_price=new_price)
    check_invariants(ob, "modify_storm")


def test_stress_view_orderbook_after_chaos():
    """view_orderbook after heavy mixed operations — snapshot must be consistent."""
    ob = OrderBook()
    rng = random.Random(123)
    live = []
    for _ in range(5000):
        op = rng.random()
        if op < 0.6 or not live:
            oid = fresh_id()
            side = rng.choice(["buy", "sell"])
            price = round(rng.uniform(8.0, 12.0), 2)
            ob.submit_order(oid, "t", "limit", side, "X", rng.randint(1, 100), price)
            live.append(oid)
        elif op < 0.8:
            ob.cancel_order(rng.choice(live))
        else:
            oid = rng.choice(live)
            ob.modify_order(oid, new_quantity=rng.randint(1, 200))

    snap = ob.view_orderbook()
    # Verify snapshot consistency: every order in snapshot should be live
    for asset, sides in snap.items():
        for level in sides["bids"]:
            for o in level["orders"]:
                assert o.remaining_quantity > 0
                assert o.status in ("pending", "partial")
        for level in sides["asks"]:
            for o in level["orders"]:
                assert o.remaining_quantity > 0
                assert o.status in ("pending", "partial")
    check_invariants(ob, "view_after_chaos")


# ---- 3l: Empty string / unusual asset names ---------------------------

def test_unusual_asset_names():
    """Empty string, unicode, and long asset names."""
    ob = OrderBook()
    for asset in ["", "X" * 1000, "\u2603", "ETHIOPIAN_YIRGACHEFFE"]:
        ob.submit_order(fresh_id(), "a", "limit", "sell", asset, 10, 10.0)
        ob.submit_order(fresh_id(), "b", "limit", "buy", asset, 10, 10.0)
    assert len(ob._trades) == 4
    check_invariants(ob, "unusual_assets")


# ---- 3m: Depth levels edge cases --------------------------------------

def test_depth_levels_exceeds_available():
    """Request more depth levels than available — should return what exists."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.0)
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 11.0)
    depth = ob.get_market_depth("X", levels=100)
    assert len(depth["asks"]) == 2
    assert len(depth["bids"]) == 0


def test_depth_zero_levels():
    """Request 0 levels — should return empty lists."""
    ob = OrderBook()
    ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 10.0)
    depth = ob.get_market_depth("X", levels=0)
    assert len(depth["asks"]) == 0


def test_depth_unknown_asset():
    """Market depth for asset with no orders."""
    ob = OrderBook()
    depth = ob.get_market_depth("NONEXISTENT", levels=5)
    assert depth == {"bids": [], "asks": []}


# ========================================================================= #
# Section 4 — Empirical Complexity Verification                              #
# ========================================================================= #

def benchmark(fn, label: str, sizes: list[int], setup_fn=None) -> list[tuple[int, float]]:
    """Run fn at increasing sizes and return (size, seconds) pairs."""
    results = []
    for n in sizes:
        if setup_fn:
            args = setup_fn(n)
        else:
            args = (n,)
        t0 = time.perf_counter()
        fn(*args)
        elapsed = time.perf_counter() - t0
        results.append((n, elapsed))
    return results


def print_scaling(label: str, results: list[tuple[int, float]]) -> None:
    """Print timing results and compute approximate scaling factor."""
    print(f"\n  {label}:")
    prev_n, prev_t = None, None
    for n, t in results:
        ratio_str = ""
        if prev_n and prev_t and prev_t > 0:
            size_ratio = n / prev_n
            time_ratio = t / prev_t
            if time_ratio > 0:
                # Estimate exponent: t ~ n^k => k = log(time_ratio) / log(size_ratio)
                import math
                k = math.log(time_ratio) / math.log(size_ratio) if size_ratio > 1 else 0
                ratio_str = f"  (scaling exponent ~ {k:.2f})"
        print(f"    N={n:>8,}  time={t:.4f}s{ratio_str}")
        prev_n, prev_t = n, t
    return results


def test_complexity_submit_scaling_with_P():
    """submit_order should scale with O(log P + P) for insertion — measure it."""
    print("\n  --- Complexity: submit_order vs price levels (P) ---")
    sizes = [100, 500, 1_000, 5_000, 10_000]
    results = []
    for P in sizes:
        ob = OrderBook()
        # Pre-populate P price levels
        t0 = time.perf_counter()
        for i in range(P):
            ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 10, 100.0 + i * 0.01)
        elapsed = time.perf_counter() - t0
        results.append((P, elapsed))
    print_scaling("submit P orders at distinct prices", results)
    # With bisect list: expect ~O(P^2) total (each insert is O(P) shift)
    # If scaling exponent >> 2, something is wrong


def test_complexity_submit_scaling_with_K():
    """Matching K fills: total time for a sweep should scale ~ O(K)."""
    print("\n  --- Complexity: matching sweep vs fills (K) ---")
    sizes = [1_000, 5_000, 10_000, 50_000]
    results = []
    for K in sizes:
        ob = OrderBook()
        for i in range(K):
            ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 1, 10.0)
        t0 = time.perf_counter()
        ob.submit_order(fresh_id(), "b", "market", "buy", "X", K)
        elapsed = time.perf_counter() - t0
        results.append((K, elapsed))
    print_scaling("sweep K orders at same price", results)


def test_complexity_cancel_scaling_with_Q():
    """cancel_order from middle of a deque: should be O(Q)."""
    print("\n  --- Complexity: cancel_order vs queue depth (Q) ---")
    sizes = [1_000, 5_000, 10_000, 50_000]
    results = []
    for Q in sizes:
        ob = OrderBook()
        ids = []
        for i in range(Q):
            oid = fresh_id()
            ob.submit_order(oid, "a", "limit", "sell", "X", 1, 10.0)
            ids.append(oid)
        # Cancel the middle order (worst case for deque.remove)
        target = ids[Q // 2]
        t0 = time.perf_counter()
        ob.cancel_order(target)
        elapsed = time.perf_counter() - t0
        results.append((Q, elapsed))
    print_scaling("cancel middle order from Q-deep queue", results)


def test_complexity_cancel_all_scaling():
    """Cancel all N orders: total time should reveal amortised cost."""
    print("\n  --- Complexity: cancel all N orders ---")
    sizes = [1_000, 5_000, 10_000]
    results = []
    for N in sizes:
        ob = OrderBook()
        ids = []
        for i in range(N):
            oid = fresh_id()
            ob.submit_order(oid, "a", "limit", "sell", "X", 1, 10.0 + i * 0.001)
            ids.append(oid)
        random.shuffle(ids)
        t0 = time.perf_counter()
        for oid in ids:
            ob.cancel_order(oid)
        elapsed = time.perf_counter() - t0
        results.append((N, elapsed))
    print_scaling("cancel all N orders (each at unique price)", results)
    # Each cancel is O(1) deque remove + O(P) price list remove
    # Total: O(N * P) but P shrinks as we cancel => O(N^2 / 2) total


def test_complexity_best_bid_ask():
    """get_best_bid/ask should be fast regardless of book size."""
    print("\n  --- Complexity: get_best_bid with large book ---")
    sizes = [1_000, 10_000, 50_000]
    results = []
    for N in sizes:
        ob = OrderBook()
        for i in range(N):
            ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 1, 10.0)
        ITERS = 10_000
        t0 = time.perf_counter()
        for _ in range(ITERS):
            ob.get_best_bid("X")
        elapsed = time.perf_counter() - t0
        results.append((N, elapsed))
    print_scaling(f"get_best_bid x{ITERS} (Q={{}}, single price)", results)
    # Expect O(Q) due to volume_at_price sum — should scale linearly with N


def test_complexity_best_bid_many_prices():
    """get_best_bid with many prices but only 1 order per level — should be ~O(1)."""
    print("\n  --- Complexity: get_best_bid with many price levels (1 order each) ---")
    sizes = [100, 1_000, 10_000]
    results = []
    for P in sizes:
        ob = OrderBook()
        for i in range(P):
            ob.submit_order(fresh_id(), "a", "limit", "buy", "X", 1, 10.0 + i * 0.001)
        ITERS = 100_000
        t0 = time.perf_counter()
        for _ in range(ITERS):
            ob.get_best_bid("X")
        elapsed = time.perf_counter() - t0
        results.append((P, elapsed))
    print_scaling(f"get_best_bid x{ITERS} (1 order per price level)", results)


def test_complexity_view_last_orders():
    """view_last_orders scales with total history, not just the requested n."""
    print("\n  --- Complexity: view_last_orders ---")
    sizes = [10_000, 50_000, 100_000]
    results = []
    for N in sizes:
        ob = OrderBook()
        for i in range(N):
            ob.submit_order(fresh_id(), "a", "limit", "sell", "X", 1, 10.0)
        t0 = time.perf_counter()
        ob.view_last_orders(10)  # only want 10, but it copies the whole deque
        elapsed = time.perf_counter() - t0
        results.append((N, elapsed))
    print_scaling("view_last_orders(10) with N total orders", results)
    # NOTE: Current impl is O(N) not O(n) — it copies the whole deque first


# ========================================================================= #
# Main runner                                                                 #
# ========================================================================= #

if __name__ == "__main__":
    random.seed(42)
    reset_ids()

    print("=" * 70)
    print("SECTION 1: Correctness Tests")
    print("=" * 70)
    correctness_tests = [
        ("basic limit match", test_basic_limit_match),
        ("price-time priority", test_price_time_priority),
        ("price priority", test_price_priority),
        ("partial fill (resting)", test_partial_fill_resting),
        ("multi-level sweep", test_multi_level_sweep),
        ("no crossing (bid < ask)", test_no_crossing),
        ("market order fill", test_market_order_fill),
        ("market order empty book", test_market_order_empty_book),
        ("market order partial", test_market_order_partial),
        ("IOC full fill", test_ioc_full_fill),
        ("IOC partial fill", test_ioc_partial_fill),
        ("IOC no fill", test_ioc_no_fill),
        ("FOK full fill", test_fok_full_fill),
        ("FOK insufficient liquidity", test_fok_insufficient_liquidity),
        ("FOK exact fill", test_fok_exact_fill),
        ("FOK multi-level", test_fok_multi_level),
        ("cancel pending", test_cancel_pending),
        ("cancel partial", test_cancel_partial),
        ("cancel filled", test_cancel_filled),
        ("cancel unknown", test_cancel_unknown),
        ("cancel idempotent", test_cancel_idempotent),
        ("modify price loses priority", test_modify_price_loses_priority),
        ("modify qty decrease keeps priority", test_modify_qty_decrease_keeps_priority),
        ("modify qty increase loses priority", test_modify_qty_increase_loses_priority),
        ("modify below filled fails", test_modify_below_filled_fails),
        ("modify cancelled order", test_modify_cancelled_order),
        ("self-trade", test_self_trade),
        ("multiple assets", test_multiple_assets),
        ("spread empty/one-sided", test_spread_empty),
        ("market depth", test_market_depth),
        ("trade at resting price", test_trade_at_resting_price),
        ("trade history ordering", test_trade_history_ordering),
        ("order status unknown", test_order_status_unknown),
        ("view last orders", test_view_last_orders),
        ("view largest orders", test_view_largest_orders),
        ("bid/ask volume aggregation", test_bid_ask_volume_aggregation),
        ("cancel cleans price level", test_cancel_cleans_price_level),
        ("many cancels then match", test_many_cancels_then_match),
    ]
    for name, fn in correctness_tests:
        run_test(name, fn)

    print()
    print("=" * 70)
    print("SECTION 2: Stress / Load Tests")
    print("=" * 70)
    stress_tests = [
        ("50k order submit", test_stress_high_volume_submit),
        ("1000 price levels per side", test_stress_many_price_levels),
        ("10k orders same price + cancel half + sweep", test_stress_deep_single_level),
        ("20k rapid submit-cancel cycles", test_stress_rapid_submit_cancel),
        ("large market sweep (500 levels)", test_stress_large_sweep),
        ("50 assets x 200 orders", test_stress_multi_asset),
        ("1000 failed FOK (book untouched)", test_stress_fok_repeated),
        ("10k interleaved mixed operations", test_stress_interleaved_operations),
    ]
    for name, fn in stress_tests:
        run_test(name, fn)

    print()
    print("=" * 70)
    print("SECTION 3: Adversarial & Hard Edge Cases")
    print("=" * 70)
    adversarial_tests = [
        # 3a: Order ID collision
        ("ID collision: ghost in book", test_order_id_collision_ghost_in_book),
        ("ID collision: cancel ghost", test_order_id_collision_cancel),
        # 3b: Float precision
        ("float near-epsilon prices", test_float_precision_near_prices),
        ("float accumulation error", test_float_accumulation_error),
        ("negative and zero prices", test_negative_and_zero_price),
        # 3c: Crossed / locked book
        ("modify creates crossed book", test_modify_creates_crossed_book),
        ("modify creates locked book", test_modify_creates_locked_book),
        # 3d: Sell-side order types
        ("market sell", test_market_sell),
        ("market sell empty", test_market_sell_empty),
        ("market sell multi-level", test_market_sell_multi_level),
        ("IOC sell partial", test_ioc_sell),
        ("FOK sell full fill", test_fok_sell),
        ("FOK sell rejected", test_fok_sell_rejected),
        ("FOK sell multi-level", test_fok_sell_multi_level),
        # 3e: Cancel/modify non-resting
        ("cancel filled market order", test_cancel_market_order),
        ("cancel cancelled IOC", test_cancel_ioc_order),
        ("modify market order", test_modify_market_order),
        ("modify filled order", test_modify_filled_order),
        # 3f: Modify edge cases
        ("modify price only", test_modify_price_only),
        ("modify both price & qty", test_modify_both_price_and_quantity),
        ("modify into existing level", test_modify_into_existing_price_level),
        ("partial fill then modify", test_modify_partial_fill_then_modify),
        ("modify qty to zero", test_modify_quantity_to_zero),
        ("modify negative qty", test_modify_negative_quantity),
        # 3g: Quantity edge cases
        ("quantity = 1", test_quantity_one),
        ("huge quantity (10^15)", test_huge_quantity),
        ("1 massive buy vs 10k tiny sells", test_one_massive_vs_many_tiny),
        ("10k tiny buys vs 1 massive sell", test_many_tiny_vs_one_massive),
        # 3h: Conservation
        ("trade volume conservation (5k ops)", test_trade_volume_conservation),
        ("per-asset trade isolation", test_per_asset_trade_isolation),
        # 3i: Cancel patterns
        ("cancel head of queue", test_cancel_head_of_queue),
        ("cancel tail of queue", test_cancel_tail_of_queue),
        ("cancel all at price level", test_cancel_all_at_price_level),
        ("cancel alternating pattern", test_cancel_alternating_pattern),
        # 3j: Realistic patterns
        ("market maker ladder/sweep/replenish", test_market_maker_pattern),
        ("cascading partial fill then rest", test_cascading_partial_fill_then_rest),
        ("fill -> modify -> fill", test_interleaved_fill_modify_fill),
        # 3k: Stress adversarial
        ("5k cancel from middle of deque", test_stress_worst_case_cancel_from_middle),
        ("100-cycle price oscillation", test_stress_price_oscillation),
        ("20 assets simultaneous matching", test_stress_many_assets_simultaneous_matching),
        ("20k random FOK/IOC/market/limit mix", test_stress_random_fok_ioc_market_mix),
        ("1k modify storm on single order", test_stress_modify_storm),
        ("view_orderbook after 5k chaotic ops", test_stress_view_orderbook_after_chaos),
        # 3l: Unusual inputs
        ("unusual asset names", test_unusual_asset_names),
        # 3m: Depth edge cases
        ("depth levels > available", test_depth_levels_exceeds_available),
        ("depth 0 levels", test_depth_zero_levels),
        ("depth unknown asset", test_depth_unknown_asset),
    ]
    for name, fn in adversarial_tests:
        run_test(name, fn)

    print()
    print("=" * 70)
    print("SECTION 4: Empirical Complexity Verification")
    print("=" * 70)
    complexity_tests = [
        ("submit scaling vs P", test_complexity_submit_scaling_with_P),
        ("matching sweep vs K", test_complexity_submit_scaling_with_K),
        ("cancel single vs Q", test_complexity_cancel_scaling_with_Q),
        ("cancel all N (unique prices)", test_complexity_cancel_all_scaling),
        ("get_best_bid (single price)", test_complexity_best_bid_ask),
        ("get_best_bid (many prices)", test_complexity_best_bid_many_prices),
        ("view_last_orders scaling", test_complexity_view_last_orders),
    ]
    for name, fn in complexity_tests:
        run_test(name, fn)

    # Summary
    print()
    print("=" * 70)
    total = PASS_COUNT + FAIL_COUNT
    print(f"Results: {PASS_COUNT}/{total} passed, {FAIL_COUNT} failed")
    if FAIL_COUNT:
        print("SOME TESTS FAILED")
    else:
        print("ALL TESTS PASSED")
    print("=" * 70)

    sys.exit(1 if FAIL_COUNT else 0)
