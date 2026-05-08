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


SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]

def make_batch(batch_id: int, batch_size: int) -> str:
    orders = []

    for i in range(batch_size):
        mod = i % 4

        if mod == 0:
            # Resting ask
            orders.append(["AAPL", 1, 0, 10100, 1])
        elif mod == 1:
            # Market buy consumes ask
            orders.append(["AAPL", 0, 1, None, 1])
        elif mod == 2:
            # Resting bid
            orders.append(["AAPL", 0, 0, 9900, 1])
        else:
            # Market sell consumes bid
            orders.append(["AAPL", 1, 1, None, 1])

    return json.dumps({
        "type": "orders_batch_compact_multi",
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

            
            if ack.get("type") != "batch_ack":
                raise RuntimeError(f"Expected batch_ack, got: {ack}")

            if ack.get("accepted", 0) + ack.get("rejected", 0) != batch_size:
                raise RuntimeError(f"Server did not process batch: {ack}")

            accepted += ack.get("accepted", 0)
            rejected += ack.get("rejected", 0)
            trades += ack.get("trade_count", 0)

        elapsed = time.perf_counter() - start

    return WsBenchmarkResult(
        name=f"WebSocket compact multi batch size={batch_size}",
        orders=accepted + rejected,
        batches=total_batches,
        elapsed=elapsed,
        accepted=accepted,
        rejected=rejected,
        trades=trades,
    )


def print_result(result: WsBenchmarkResult) -> None:
    print()
    print(
        f"{'Benchmark':<36} "
        f"{'Orders':>12} "
        f"{'Batches':>10} "
        f"{'Accepted':>12} "
        f"{'Rejected':>12} "
        f"{'Trades':>12} "
        f"{'Seconds':>10} "
        f"{'Orders/sec':>15}"
    )
    print("-" * 130)
    print(
        f"{result.name:<36} "
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