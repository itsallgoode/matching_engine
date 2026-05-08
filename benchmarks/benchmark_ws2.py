import asyncio
import json
import statistics
import time
from dataclasses import dataclass

import websockets


WS_URL = "ws://127.0.0.1:8000/ws/order-entry/ws_single_bench"


@dataclass
class LatencyStats:
    name: str
    samples: int
    min_ms: float
    p50_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    mean_ms: float


@dataclass
class ThroughputStats:
    name: str
    orders: int
    elapsed: float
    accepted: int
    rejected: int
    orders_per_sec: float
    max_in_flight: int


def percentile(values: list[float], pct: float) -> float:
    values_sorted = sorted(values)
    index = int((len(values_sorted) - 1) * pct)
    return values_sorted[index]


def make_single_order_message(i: int) -> str:
    # WebSocket compact multi format, but exactly 1 order.
    # row: [symbol, side_code, type_code, price_ticks, quantity]
    #
    # This pattern keeps book size roughly steady:
    #   0: add resting ask
    #   1: market buy consumes ask
    #   2: add resting bid
    #   3: market sell consumes bid
    mod = i % 4

    if mod == 0:
        order = ["AAPL", 1, 0, 10100, 1]  # sell limit
    elif mod == 1:
        order = ["AAPL", 0, 1, None, 1]   # buy market
    elif mod == 2:
        order = ["AAPL", 0, 0, 9900, 1]   # buy limit
    else:
        order = ["AAPL", 1, 1, None, 1]   # sell market

    return json.dumps({
        "type": "orders_batch_compact_multi",
        "request_id": i,
        "orders": [order],
    })


async def warmup(ws, n: int = 1_000) -> None:
    for i in range(n):
        await ws.send(make_single_order_message(i))
        raw = await ws.recv()
        ack = json.loads(raw)

        if ack.get("type") != "batch_ack":
            raise RuntimeError(f"Bad warmup ack: {ack}")

        processed = ack.get("accepted", 0) + ack.get("rejected", 0)
        if processed != 1:
            raise RuntimeError(f"Warmup order not processed: {ack}")


async def benchmark_sequential_latency(
    *,
    samples: int = 50_000,
    warmup_samples: int = 5_000,
) -> LatencyStats:
    latencies_ms: list[float] = []

    async with websockets.connect(
        WS_URL,
        max_size=None,
        ping_interval=None,
    ) as ws:
        await warmup(ws, warmup_samples)

        for i in range(samples):
            msg = make_single_order_message(i + warmup_samples)

            start = time.perf_counter_ns()
            await ws.send(msg)
            raw = await ws.recv()
            end = time.perf_counter_ns()

            ack = json.loads(raw)
            if ack.get("type") != "batch_ack":
                raise RuntimeError(f"Expected batch_ack, got {ack}")

            processed = ack.get("accepted", 0) + ack.get("rejected", 0)
            if processed != 1:
                raise RuntimeError(f"Expected 1 processed order, got {ack}")

            latencies_ms.append((end - start) / 1_000_000)

    return LatencyStats(
        name="WS single-order sequential latency",
        samples=samples,
        min_ms=min(latencies_ms),
        p50_ms=percentile(latencies_ms, 0.50),
        p90_ms=percentile(latencies_ms, 0.90),
        p95_ms=percentile(latencies_ms, 0.95),
        p99_ms=percentile(latencies_ms, 0.99),
        max_ms=max(latencies_ms),
        mean_ms=statistics.mean(latencies_ms),
    )


async def benchmark_pipelined_throughput(
    *,
    total_orders: int = 100_000,
    max_in_flight: int = 100,
    warmup_samples: int = 1_000,
) -> ThroughputStats:
    """
    Batch size is still 1.

    Difference from latency mode:
      - We do NOT wait for each ack before sending the next order.
      - We keep up to max_in_flight messages outstanding.
      - This measures realistic high-throughput single-order WebSocket usage.
    """
    accepted = 0
    rejected = 0

    sent = 0
    received = 0

    async with websockets.connect(
        WS_URL,
        max_size=None,
        ping_interval=None,
    ) as ws:
        await warmup(ws, warmup_samples)

        start = time.perf_counter()

        # Prime initial window.
        while sent < total_orders and sent - received < max_in_flight:
            await ws.send(make_single_order_message(sent))
            sent += 1

        while received < total_orders:
            raw = await ws.recv()
            ack = json.loads(raw)

            if ack.get("type") != "batch_ack":
                raise RuntimeError(f"Expected batch_ack, got {ack}")

            accepted += ack.get("accepted", 0)
            rejected += ack.get("rejected", 0)
            received += 1

            # Send more to keep pipeline full.
            while sent < total_orders and sent - received < max_in_flight:
                await ws.send(make_single_order_message(sent))
                sent += 1

        elapsed = time.perf_counter() - start

    processed = accepted + rejected
    if processed != total_orders:
        raise RuntimeError(
            f"Expected {total_orders} processed orders, got {processed}"
        )

    return ThroughputStats(
        name="WS single-order pipelined throughput",
        orders=total_orders,
        elapsed=elapsed,
        accepted=accepted,
        rejected=rejected,
        orders_per_sec=total_orders / elapsed,
        max_in_flight=max_in_flight,
    )


def print_latency(stats: LatencyStats) -> None:
    print()
    print(stats.name)
    print("-" * 80)
    print(f"samples: {stats.samples:,}")
    print(f"min:     {stats.min_ms:,.3f} ms")
    print(f"p50:     {stats.p50_ms:,.3f} ms")
    print(f"p90:     {stats.p90_ms:,.3f} ms")
    print(f"p95:     {stats.p95_ms:,.3f} ms")
    print(f"p99:     {stats.p99_ms:,.3f} ms")
    print(f"max:     {stats.max_ms:,.3f} ms")
    print(f"mean:    {stats.mean_ms:,.3f} ms")
    print()


def print_throughput(stats: ThroughputStats) -> None:
    print()
    print(stats.name)
    print("-" * 100)
    print(f"orders:        {stats.orders:,}")
    print(f"accepted:      {stats.accepted:,}")
    print(f"rejected:      {stats.rejected:,}")
    print(f"seconds:       {stats.elapsed:,.3f}")
    print(f"orders/sec:    {stats.orders_per_sec:,.0f}")
    print(f"max_in_flight: {stats.max_in_flight:,}")
    print()


async def main() -> None:
    latency = await benchmark_sequential_latency(
        samples=5_000,
        warmup_samples=1_000,
    )
    print_latency(latency)

    for max_in_flight in [1, 10, 50, 100, 500, 1_000]:
        throughput = await benchmark_pipelined_throughput(
            total_orders=100_000,
            max_in_flight=max_in_flight,
            warmup_samples=1_000,
        )
        print_throughput(throughput)


if __name__ == "__main__":
    asyncio.run(main())