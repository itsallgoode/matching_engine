import gc
import time
from dataclasses import dataclass
from typing import Callable

from market_sim.definitions import Order, Side, OrderType
from market_sim.matching_engine import MatchingEngine


@dataclass
class BenchmarkResult:
    name: str
    orders: int
    trades: int
    elapsed: float

    @property
    def orders_per_sec(self) -> float:
        return self.orders / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def trades_per_sec(self) -> float:
        return self.trades / self.elapsed if self.elapsed > 0 else 0.0


def make_order(
    *,
    owner_id: str,
    symbol: str,
    side: Side,
    order_type: OrderType,
    price_ticks: int | None,
    quantity: int,
) -> Order:
    return Order(
        owner_id=owner_id,
        symbol=symbol,
        side=side,
        order_type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )


def time_benchmark(
    name: str,
    fn: Callable[[], tuple[int, int]],
    disable_gc: bool = True,
) -> BenchmarkResult:
    if disable_gc:
        gc_was_enabled = gc.isenabled()
        gc.disable()
    else:
        gc_was_enabled = False

    start = time.perf_counter()
    orders, trades = fn()
    elapsed = time.perf_counter() - start

    if disable_gc and gc_was_enabled:
        gc.enable()

    return BenchmarkResult(
        name=name,
        orders=orders,
        trades=trades,
        elapsed=elapsed,
    )


def bench_resting_limit_adds(n: int) -> tuple[int, int]:
    """
    Measures pure resting limit order insertion.

    These orders should not cross, so they mostly test:
      - Order object creation
      - engine.process_order()
      - _add_to_book()
      - dict/deque/heap operations
    """
    engine = MatchingEngine()
    symbol = "AAPL"

    for i in range(n):
        side = Side.BUY if i % 2 == 0 else Side.SELL

        # Keep buys below asks so they do not cross.
        price = 9_900 if side == Side.BUY else 10_100

        order = make_order(
            owner_id="bench",
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            price_ticks=price,
            quantity=1,
        )
        engine.process_order(order)

    return n, 0


def bench_single_level_crosses(n: int) -> tuple[int, int]:
    """
    Seeds n resting sell orders at one price, then sends n market buys.

    Each market buy should generate one trade.
    """
    engine = MatchingEngine()
    symbol = "AAPL"
    price = 10_000

    # Seed asks outside timed section.
    for _ in range(n):
        engine.process_order(
            make_order(
                owner_id="maker",
                symbol=symbol,
                side=Side.SELL,
                order_type=OrderType.LIMIT,
                price_ticks=price,
                quantity=1,
            )
        )

    trades_count = 0

    for _ in range(n):
        trades = engine.process_order(
            make_order(
                owner_id="taker",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                price_ticks=None,
                quantity=1,
            )
        )
        trades_count += len(trades)

    return n, trades_count


def bench_market_orders_walk_levels(
    n_market_orders: int,
    levels_per_order: int = 10,
) -> tuple[int, int]:
    """
    Each market buy walks multiple price levels.

    Example:
      levels_per_order = 10 means each market order should create 10 trades.

    This tests the heavier path:
      - repeated best_ask cleanup/lookups
      - queue pops
      - Trade object creation
      - multiple fills per incoming order
    """
    engine = MatchingEngine()
    symbol = "AAPL"
    base_price = 10_000

    total_resting_orders = n_market_orders * levels_per_order

    # Seed enough asks so each market order walks `levels_per_order` levels.
    # Use one share per price level/order.
    for i in range(total_resting_orders):
        price = base_price + (i % levels_per_order)
        engine.process_order(
            make_order(
                owner_id="maker",
                symbol=symbol,
                side=Side.SELL,
                order_type=OrderType.LIMIT,
                price_ticks=price,
                quantity=1,
            )
        )

    trades_count = 0

    for _ in range(n_market_orders):
        trades = engine.process_order(
            make_order(
                owner_id="taker",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                price_ticks=None,
                quantity=levels_per_order,
            )
        )
        trades_count += len(trades)

    return n_market_orders, trades_count


def bench_mixed_steady_state(n: int, seed_liquidity: int = 10_000) -> tuple[int, int]:
    """
    Keeps the book roughly alive instead of only growing it.

    Pattern:
      - seed liquidity on both sides
      - alternate market orders and new resting limit orders

    This is closer to a running simulation than a pure append benchmark.
    """
    engine = MatchingEngine()
    symbol = "AAPL"

    bid_price = 9_999
    ask_price = 10_001

    # Seed book outside timed section.
    for _ in range(seed_liquidity):
        engine.process_order(
            make_order(
                owner_id="seed",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                price_ticks=bid_price,
                quantity=1,
            )
        )
        engine.process_order(
            make_order(
                owner_id="seed",
                symbol=symbol,
                side=Side.SELL,
                order_type=OrderType.LIMIT,
                price_ticks=ask_price,
                quantity=1,
            )
        )

    trades_count = 0

    for i in range(n):
        mod = i % 4

        if mod == 0:
            # Market buy consumes one ask.
            order = make_order(
                owner_id="taker",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                price_ticks=None,
                quantity=1,
            )
        elif mod == 1:
            # Replace ask liquidity.
            order = make_order(
                owner_id="maker",
                symbol=symbol,
                side=Side.SELL,
                order_type=OrderType.LIMIT,
                price_ticks=ask_price,
                quantity=1,
            )
        elif mod == 2:
            # Market sell consumes one bid.
            order = make_order(
                owner_id="taker",
                symbol=symbol,
                side=Side.SELL,
                order_type=OrderType.MARKET,
                price_ticks=None,
                quantity=1,
            )
        else:
            # Replace bid liquidity.
            order = make_order(
                owner_id="maker",
                symbol=symbol,
                side=Side.BUY,
                order_type=OrderType.LIMIT,
                price_ticks=bid_price,
                quantity=1,
            )

        trades = engine.process_order(order)
        trades_count += len(trades)

    return n, trades_count


def print_results(results: list[BenchmarkResult]) -> None:
    print()
    print(
        f"{'Benchmark':<38} "
        f"{'Orders':>12} "
        f"{'Trades':>12} "
        f"{'Seconds':>10} "
        f"{'Orders/sec':>15} "
        f"{'Trades/sec':>15}"
    )
    print("-" * 110)

    for r in results:
        print(
            f"{r.name:<38} "
            f"{r.orders:>12,} "
            f"{r.trades:>12,} "
            f"{r.elapsed:>10.3f} "
            f"{r.orders_per_sec:>15,.0f} "
            f"{r.trades_per_sec:>15,.0f}"
        )

    print()


def run_all(n: int = 100_000) -> None:
    """
    Start with n=100_000.
    Then try n=1_000_000 once you're happy with runtime.
    """
    results = []

    results.append(
        time_benchmark(
            "Resting limit adds",
            lambda: bench_resting_limit_adds(n),
        )
    )

    results.append(
        time_benchmark(
            "Single-level crosses",
            lambda: bench_single_level_crosses(n),
        )
    )

    # Use fewer market orders because this creates n * levels_per_order trades.
    walking_orders = max(1, n // 10)
    results.append(
        time_benchmark(
            "Market orders walk 10 levels",
            lambda: bench_market_orders_walk_levels(
                n_market_orders=walking_orders,
                levels_per_order=10,
            ),
        )
    )

    results.append(
        time_benchmark(
            "Mixed steady-state",
            lambda: bench_mixed_steady_state(n),
        )
    )

    print_results(results)


if __name__ == "__main__":
    run_all(n=100_0000)