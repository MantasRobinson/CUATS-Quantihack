# CUATS Hacakthon 2026 Orderbook Documentation

## 1. Overview

The exchange is implemented as a single `OrderBook` class (entry point) backed by per-asset
sub-books.  Each sub-book holds a bid side and an ask side.  The engine supports four order
types — **limit**, **market**, **FOK** (fill-or-kill), and **IOC** (immediate-or-cancel) — with
strict price-time priority matching.

---

## 2. Data Structure Choices

### 2.1 Top-level asset dispatch — `dict[str, _AssetBook]`

A Python `dict` keyed by asset symbol routes every operation to the correct book in **O(1)**
average time.  Books are created lazily on first use, so there is zero allocation overhead for
assets that never receive an order.

**Why not a list?**  Linear scan over asset names would be O(A) per operation (A = number of
assets).  Hash-map dispatch is strictly better and simpler.

---

### 2.2 Price levels — `dict[float, deque[Order]]`

Each side (bid / ask) stores a mapping from price → queue of resting orders:

```
price_map : dict[float, deque[Order]]
```

- **`dict`** — O(1) lookup, insertion, and deletion of a price level.
- **`deque`** — O(1) `append` (new order at back) and `popleft` (consume best order at front),
  preserving FIFO order within a price level (time priority).  CPython's `collections.deque`
  is implemented in C and is substantially faster than `list` for left-side pops, which would
  be O(n).

**Why not a plain `list` for the queue?**  Removing the front element of a `list` is O(Q) due
to shifting; `deque.popleft()` is O(1).  With thousands of fills per second, this difference
is significant.

---

### 2.3 Per-level volume tracker — `dict[float, int]`

Each side maintains a volume counter per active price level:

```
_level_volume : dict[float, int]
```

Updated in O(1) on every add, cancel, fill, and modify.  This eliminates the need to iterate
the deque to compute volume, making `get_best_bid`, `get_best_ask`, `get_spread`,
`get_market_depth`, and `get_volume_at_price` all **O(1)** or **O(levels)**.

---

### 2.4 Sorted price index — `list[float]` + `bisect`

A plain sorted list of active price levels is maintained alongside `price_map`:

```
sorted_prices : list[float]   # ascending
```

- **Best price** — O(1): `sorted_prices[-1]` for bids (highest), `sorted_prices[0]` for asks
  (lowest).  No heap-pop required; the best price is always at a known, stable index.
- **Insertion of a new price level** — O(log P) search via `bisect.insort` + O(P) shift in
  the worst case, where P is the number of *distinct active price levels* on that side.
- **Removal of an exhausted price level** — O(log P) search via `bisect.bisect_left` + O(P)
  shift.

**Why not `heapq`?**  A min-heap gives O(1) peek and O(log P) insert, but has **no efficient
arbitrary deletion**.  When a price level is exhausted or cancelled mid-book, a heap requires
either a full O(P) rebuild or a lazy-deletion scheme.  Lazy deletion pollutes the heap with
stale entries and complicates the best-price query.  The `bisect` list avoids all of this.

**Why not `sortedcontainers.SortedList`?**  This is a third-party package; the challenge
requires standard-library only.  Implementing a self-balancing BST (red-black, AVL) would give
O(log P) insert *and* delete, but adds several hundred lines of complex code for a marginal
improvement over `bisect` when P (the number of distinct price levels) is far smaller than the
total number of orders — which is the common case in real markets.

**Trade-off accepted:**  O(P) worst-case list shift on insert / remove in exchange for O(1)
best-price access, O(1) queue access, and implementation simplicity.

---

### 2.5 Global order index — `dict[int, Order]`

```
_orders_by_id : dict[int, Order]
```

Maps every order ID to its `Order` object.  Provides **O(1)** lookup for `cancel_order`,
`modify_order`, and `get_order_status`.

---

### 2.6 Order history — `deque[Order]`

```
_order_history : deque[Order]
```

Orders are appended chronologically.  `view_last_orders(n)` uses `itertools.islice` on the
reversed deque — truly **O(n)** for the requested *n*, not O(N) for the full history.  A
`deque` is used rather than a `list` to leave open the option of capping history length with
`maxlen` in the future without changing the interface.

---

### 2.7 Trade tape — `list[Trade]`

```
_trades : list[Trade]
```

An append-only list.  `get_trade_history` iterates in reverse — O(T) where T is total trades,
bounded by the `limit` argument.  Filtering by asset is a single linear pass; a per-asset
index would reduce this to O(limit) at the cost of extra memory and bookkeeping.

---

## 3. Time Complexity Analysis

| Operation | Complexity | Notes |
|---|---|---|
| `submit_order` | **O(log P + K)** | `log P` for bisect insert; `K` fills each O(1) |
| `cancel_order` | **O(1)** amortised | lazy mark + O(1) volume decrement; level cleanup O(log P + P) when volume hits 0 |
| `modify_order` | **O(Q + log P)** | O(Q) eager deque removal + O(log P) bisect re-insert; qty-decrease-only is O(1) |
| `get_best_bid` | **O(1)** | index into sorted\_prices + O(1) \_level\_volume lookup |
| `get_best_ask` | **O(1)** | same as above |
| `get_spread` | **O(1)** | two O(1) best-price lookups |
| `get_market_depth` | **O(levels)** | iterates price levels, reads \_level\_volume dict |
| `get_volume_at_price` | **O(1)** | dict lookup in \_level\_volume |
| `view_orderbook` | **O(N_resting)** | full snapshot of all resting orders |
| `view_last_orders` | **O(n)** | `itertools.islice` on reversed deque — truly O(n), not O(N) |
| `view_largest_orders` | **O(N log n)** | `heapq.nlargest` — O(N log n) when n ≪ N |
| `view_smallest_orders` | **O(N log n)** | `heapq.nsmallest` — same |
| `get_trade_history` | **O(T)** | reversed iteration, bounded by `limit` |
| `get_order_status` | **O(F)** | O(1) lookup + O(F) fill list copy |

*P = distinct active price levels on one side; Q = orders at one price level; K = fills during
matching; N = total orders submitted; n = requested count; T = total trades; F = fills on one
order.*

---

## 4. Trade-offs Considered

### 4.1 `bisect` list vs. self-balancing BST

A self-balancing BST (e.g. red-black tree) would give O(log P) for both insert and delete,
eliminating the O(P) shift of a sorted list.  However:

- Python's standard library has no BST.
- Implementing one correctly adds ~300 lines and several edge cases (rotations, colour fixes).
- In practice P (distinct price levels) is small — typically tens to hundreds, rarely thousands.
  The O(P) shift on a list of 100 floats is far cheaper than a BST traversal with pointer
  chasing and Python object overhead.

**Decision:** Keep `bisect` + `list`.

### 4.2 Hybrid lazy-order / eager-level cancellation

Cancelled orders are **not** physically removed from their deque (which would be O(Q)).
Instead, the order is marked cancelled and the per-level volume counter is decremented in
O(1).  The matching loop already skips cancelled entries, popping them in O(1) each.

**Price levels** are eagerly cleaned when their tracked volume reaches zero — the entire
level (deque, sorted\_prices entry, volume counter) is removed.  This ensures `best_price()`
always returns a level with live volume, so `get_best_bid/ask` remain O(1).

This hybrid gives O(1) amortised cancel without the downsides of fully-lazy deletion (stale
price levels polluting queries).  The `modify_order` path still uses eager O(Q) deque
removal because the order must be physically moved to a new queue position.

### 4.3 Per-asset trade index vs. global tape

A `dict[str, list[Trade]]` per asset would make `get_trade_history(asset=X)` O(limit) instead
of O(T).  The trade-off is extra memory and an additional append per fill.

Given the challenge focuses on the matching engine and the trade history query is read-path
only, we chose a single global tape for simplicity.  Adding per-asset indexing is a
straightforward future optimisation.

### 4.4 FOK feasibility check

FOK orders require a two-phase approach: first check whether the full quantity can be filled,
then execute.  The alternative — optimistic matching with rollback — requires saving and
restoring state, which is more complex and error-prone.

The feasibility check (`_check_fok_feasibility`) walks the contra side without mutating state,
at the same O(K) cost as the match itself.  The total cost for a successful FOK is therefore
2× the matching cost — acceptable and bounded.

---

## 5. Edge Case Handling

| Scenario | Behaviour |
|---|---|
| Order ID collision | The new order overwrites the old in `_orders_by_id`. Callers are responsible for unique IDs. |
| Self-trade | Allowed — the spec explicitly permits it. |
| Market order on empty book | Immediately cancelled; `OrderResult.status = "cancelled"`. |
| FOK with insufficient liquidity | Rejected before matching; no fills, no book mutation. |
| IOC partially fills | Filled portion executes; remainder is cancelled; `status = "partial"`. |
| Cancel already-filled order | Returns `False`; order is unchanged. |
| Cancel already-cancelled order | Returns `False`. |
| Modify a filled / cancelled order | Returns `False`. |
| Modify quantity below already-filled amount | Returns `False` (new_quantity must exceed filled_quantity). |
| Quantity-decrease modify | Time priority preserved; no re-insertion. |
| Price-change or quantity-increase modify | Order goes to the back of the new/existing price queue with a new timestamp. |
| Query unknown asset | Returns `None` / empty structures — no `KeyError`. |
| Query unknown order ID | `get_order_status` returns `None`; cancel/modify return `False`. |
| Stale cancelled entries in a deque | Skipped atomically during the matching loop (`status == "cancelled"` check). |
| Zero-volume price level after matching | Eagerly cleaned from `price_map` and `sorted_prices` so `best_price()` remains correct. |
