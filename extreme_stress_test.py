import random
import time
from exchange import OrderBook

# =========================================================
# 1. SAME PRICE LEVEL OVERLOAD (deque stress)
# =========================================================
def test_same_price_overload():
    print("\n[TEST] Same Price Level Overload")

    ob = OrderBook()
    N = 20000
    price = 10.0

    for i in range(N):
        ob.submit_order(i, "seller", "limit", "sell", "COFFEE", 1, price)

    ob.submit_order(N+1, "buyer", "market", "buy", "COFFEE", N)

    trades = ob.get_trade_history("COFFEE", limit=N)
    assert len(trades) == N, "Not all orders matched"

    print("✅ Passed")


# =========================================================
# 2. MANY PRICE LEVELS (bisect worst-case)
# =========================================================
def test_many_price_levels():
    print("\n[TEST] Many Price Levels")

    ob = OrderBook()
    N = 20000

    for i in range(N):
        ob.submit_order(i, "seller", "limit", "sell", "COFFEE", 1, 10 + i * 0.0001)

    ob.submit_order(N+1, "buyer", "market", "buy", "COFFEE", N)

    print("✅ Passed")


# =========================================================
# 3. CANCEL STORM
# =========================================================
def test_cancel_storm():
    print("\n[TEST] Cancel Storm")

    ob = OrderBook()
    ids = []

    for i in range(10000):
        ob.submit_order(i, "trader", "limit", "buy", "COFFEE", 10, 10.0)
        ids.append(i)

    random.shuffle(ids)

    for oid in ids:
        ob.cancel_order(oid)

    bid = ob.get_best_bid("COFFEE")
    assert bid is None, "Book not empty after cancels"

    print("✅ Passed")


# =========================================================
# 4. MODIFY PRIORITY TEST
# =========================================================
def test_modify_priority():
    print("\n[TEST] Modify Priority")

    ob = OrderBook()

    ob.submit_order(1, "A", "limit", "buy", "COFFEE", 10, 10)
    ob.submit_order(2, "B", "limit", "buy", "COFFEE", 10, 10)

    # Modify order 1 (quantity increase triggers re-queue → loses priority at price 10)
    ob.modify_order(1, new_quantity=20)

    ob.submit_order(3, "seller", "market", "sell", "COFFEE", 10)

    trades = ob.get_trade_history("COFFEE")
    assert trades[0].buy_order_id == 2, "Priority broken"

    print("✅ Passed")


# =========================================================
# 5. FOK STRICTNESS
# =========================================================
def test_fok_strict():
    print("\n[TEST] FOK Strictness")

    ob = OrderBook()

    ob.submit_order(1, "seller", "limit", "sell", "COFFEE", 5, 10)
    res = ob.submit_order(2, "buyer", "fok", "buy", "COFFEE", 10, 10)

    assert res.status == "cancelled"
    assert len(ob.get_trade_history()) == 0

    print("✅ Passed")


# =========================================================
# 6. IOC BEHAVIOR
# =========================================================
def test_ioc_behavior():
    print("\n[TEST] IOC Behavior")

    ob = OrderBook()

    ob.submit_order(1, "seller", "limit", "sell", "COFFEE", 5, 10)
    ob.submit_order(2, "buyer", "ioc", "buy", "COFFEE", 10, 10)

    bid = ob.get_best_bid("COFFEE")
    assert bid is None, "IOC incorrectly stayed in book"

    print("✅ Passed")


# =========================================================
# 7. SELF TRADE
# =========================================================
def test_self_trade():
    print("\n[TEST] Self Trade")

    ob = OrderBook()

    ob.submit_order(1, "A", "limit", "sell", "COFFEE", 10, 10)
    ob.submit_order(2, "A", "limit", "buy", "COFFEE", 10, 10)

    trades = ob.get_trade_history()
    assert len(trades) == 1

    print("✅ Passed")


# =========================================================
# 8. EMPTY MARKET ORDER
# =========================================================
def test_empty_market():
    print("\n[TEST] Empty Book Market")

    ob = OrderBook()

    res = ob.submit_order(1, "A", "market", "buy", "COFFEE", 10)
    assert res.status in ("cancelled", "partial", "pending")

    print("✅ Passed")


# =========================================================
# 9. FULL RANDOM STRESS (COMBINED CHAOS)
# =========================================================
def test_random_chaos():
    print("\n[TEST] Random Chaos Stress")

    ob = OrderBook()
    NUM = 30000

    for i in range(NUM):
        order_type = random.choice(["limit", "market", "ioc", "fok"])
        side = random.choice(["buy", "sell"])
        qty = random.randint(1, 100)
        price = None if order_type == "market" else round(random.uniform(5, 15), 2)

        ob.submit_order(
            i,
            f"trader_{random.randint(1,100)}",
            order_type,
            side,
            "COFFEE",
            qty,
            price,
        )

        if i % 50 == 0:
            ob.cancel_order(random.randint(0, i))

        if i % 100 == 0:
            ob.modify_order(
                random.randint(0, i),
                new_quantity=random.randint(1, 100),
                new_price=round(random.uniform(5, 15), 2),
            )

    # Invariant check
    bid = ob.get_best_bid("COFFEE")
    ask = ob.get_best_ask("COFFEE")

    if bid and ask:
        assert bid[0] <= ask[0], "Crossed book!"

    print("✅ Passed")


# =========================================================
# RUN ALL TESTS
# =========================================================
if __name__ == "__main__":
    start = time.time()

    test_same_price_overload()
    test_many_price_levels()
    test_cancel_storm()
    test_modify_priority()
    test_fok_strict()
    test_ioc_behavior()
    test_self_trade()
    test_empty_market()
    test_random_chaos()

    end = time.time()

    print("\nALL EXTREME TESTS PASSED")
    print(f"Total time: {end - start:.2f} seconds")