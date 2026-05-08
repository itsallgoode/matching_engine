import asyncio
import json
import time
from dataclasses import dataclass

import websockets


WS_URL = "ws://127.0.0.1:8000/ws/order-entry/ws_bench"


@dataclass
class WsBenchmarkResult:
    name: str
    orders: int
    batches: int
    elapsed: float
    accepted: int
    rejected: int
    trades: int

    @property
    def orders_per_sec(self) -> float:
        return self.orders / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def trades_per_sec(self) -> float:
        return self.trades / self.elapsed if self.elapsed > 0 else 0.0


def make_batch(batch_id: int, batch_size: int) -> str:
    orders = []

    for i in range(batch_size):
        orders.append({
            "client_order_id": f"ws-{batch_id}-{i}",
            "symbol": "AAPL",
            "side": "buy",
            "order_type": "limit",
            "price_ticks": 9900,
            "quantity": 1,
        })

    return json.dumps({
        "type": "orders_batch",
        "orders": orders,
    })


async def benchmark_ws_batches(
    *,
    total_orders: int,
    batch_size: int,
) -> WsBenchmarkResult:
    total_batches = total_orders // batch_size

    accepted = 0
    rejected = 0
    trades = 0

    async with websockets.connect(
        WS_URL,
        max_size=None,
        ping_interval=None,
    ) as ws:
        start = time.perf_counter()

        for batch_id in range(total_batches):
            msg = make_batch(batch_id, batch_size)
            await ws.send(msg)

            raw_ack = await ws.recv()
            ack = json.loads(raw_ack)

            accepted += ack.get("accepted", 0)
            rejected += ack.get("rejected", 0)
            trades += ack.get("trade_count", 0)

        elapsed = time.perf_counter() - start

    return WsBenchmarkResult(
        name=f"WebSocket batch size={batch_size}",
        orders=total_batches * batch_size,
        batches=total_batches,
        elapsed=elapsed,
        accepted=accepted,
        rejected=rejected,
        trades=trades,
    )


def print_result(result: WsBenchmarkResult) -> None:
    print()
    print(
        f"{'Benchmark':<32} "
        f"{'Orders':>12} "
        f"{'Batches':>10} "
        f"{'Accepted':>12} "
        f"{'Rejected':>12} "
        f"{'Trades':>12} "
        f"{'Seconds':>10} "
        f"{'Orders/sec':>15}"
    )
    print("-" * 125)
    print(
        f"{result.name:<32} "
        f"{result.orders:>12,} "
        f"{result.batches:>10,} "
        f"{result.accepted:>12,} "
        f"{result.rejected:>12,} "
        f"{result.trades:>12,} "
        f"{result.elapsed:>10.3f} "
        f"{result.orders_per_sec:>15,.0f}"
    )
    print()


async def main() -> None:
    result = await benchmark_ws_batches(
        total_orders=100_0000,
        batch_size=1,
    )
    print_result(result)


if __name__ == "__main__":
    asyncio.run(main())