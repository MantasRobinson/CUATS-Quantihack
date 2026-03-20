from exchange.orderbook import OrderBook
ob = OrderBook()
# Submit two limit orders
r1 = ob.submit_order(1, "alice", "limit", "sell", "COFFEE", 100, 10.0)
r2 = ob.submit_order(2, "bob", "limit", "buy", "COFFEE", 200, 10.0)
# Check the trade happened
trades = ob.get_trade_history("COFFEE")
print(f"Trades: {len(trades)}")
status = ob.get_order_status(1)
print(f"Order 1 status: {status}")
bid = ob.get_best_bid("COFFEE")
ask = ob.get_best_ask("COFFEE")
print(f"Best bid: {bid}, Best ask: {ask}")
depth = ob.get_market_depth("COFFEE", levels=3)
print(f"Depth: {depth}")
cancelled = ob.cancel_order(1)
print(f"Cancel order 1: {cancelled}")
print("All basic checks passed!")