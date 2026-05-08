import asyncio
import json
import statistics
import time
from dataclasses import dataclass

import websockets


WS_URL = "ws://127.0.0.1:8000/ws/order-entry/latency_bench"


@dataclass
class LatencyStats:
    name: str
    samples: int
    batch_size: int
    min_ms: float
    p50_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    mean_ms: float


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0

    values_sorted = sorted(values)
    index = int((len(values_sorted) - 1) * pct)
    return values_sorted[index]


def make_batch(batch_id: int, batch_size: int) -> str:
    orders = []

    for i in range(batch_size):
        # Compact multi format:
        # [symbol, side_code, type_code, price_ticks, quantity]
        #
        # side_code: 0 = buy, 1 = sell
        # type_code: 0 = limit, 1 = market
        #
        # Use steady-state-ish pattern so book does not only grow.
        mod = i % 4

        if mod == 0:
            orders.append(["AAPL", 1, 0, 10100, 1])  # resting ask
        elif mod == 1:
            orders.append(["AAPL", 0, 1, None, 1])   # market buy
        elif mod == 2:
            orders.append(["AAPL", 0, 0, 9900, 1])   # resting bid
        else:
            orders.append(["AAPL", 1, 1, None, 1])   # market sell

    return json.dumps({
        "type": "orders_batch_compact_multi",
        "batch_id": batch_id,
        "orders": orders,
    })


async def measure_ws_batch_latency(
    *,
    samples: int,
    batch_size: int,
    warmup: int = 100,
) -> LatencyStats:
    latencies_ms: list[float] = []

    async with websockets.connect(
        WS_URL,
        max_size=None,
        ping_interval=None,
    ) as ws:
        # Warmup: let connection/server settle.
        for batch_id in range(warmup):
            await ws.send(make_batch(batch_id, batch_size))
            await ws.recv()

        for batch_id in range(samples):
            msg = make_batch(batch_id + warmup, batch_size)

            start = time.perf_counter_ns()
            await ws.send(msg)
            raw_ack = await ws.recv()
            end = time.perf_counter_ns()

            ack = json.loads(raw_ack)
            if ack.get("type") != "batch_ack":
                raise RuntimeError(f"Expected batch_ack, got {ack}")

            processed = ack.get("accepted", 0) + ack.get("rejected", 0)
            if processed != batch_size:
                raise RuntimeError(f"Expected {batch_size} processed, got {ack}")

            latencies_ms.append((end - start) / 1_000_000)

    return LatencyStats(
        name=f"WS batch latency size={batch_size}",
        samples=samples,
        batch_size=batch_size,
        min_ms=min(latencies_ms),
        p50_ms=percentile(latencies_ms, 0.50),
        p90_ms=percentile(latencies_ms, 0.90),
        p95_ms=percentile(latencies_ms, 0.95),
        p99_ms=percentile(latencies_ms, 0.99),
        max_ms=max(latencies_ms),
        mean_ms=statistics.mean(latencies_ms),
    )


def print_stats(stats: LatencyStats) -> None:
    print()
    print(f"{stats.name}")
    print("-" * 80)
    print(f"samples:    {stats.samples:,}")
    print(f"batch_size: {stats.batch_size:,}")
    print(f"min:        {stats.min_ms:,.3f} ms")
    print(f"p50:        {stats.p50_ms:,.3f} ms")
    print(f"p90:        {stats.p90_ms:,.3f} ms")
    print(f"p95:        {stats.p95_ms:,.3f} ms")
    print(f"p99:        {stats.p99_ms:,.3f} ms")
    print(f"max:        {stats.max_ms:,.3f} ms")
    print(f"mean:       {stats.mean_ms:,.3f} ms")
    print()


async def main() -> None:
    for batch_size in [1, 10, 100, 1_000, 10_000]:
        stats = await measure_ws_batch_latency(
            samples=1_000 if batch_size <= 100 else 200,
            batch_size=batch_size,
            warmup=50,
        )
        print_stats(stats)


if __name__ == "__main__":
    asyncio.run(main())