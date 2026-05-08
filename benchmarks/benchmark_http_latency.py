import asyncio
import statistics
import time
from dataclasses import dataclass

import httpx


BASE_URL = "http://127.0.0.1:8000"


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


def percentile(values: list[float], pct: float) -> float:
    values_sorted = sorted(values)
    index = int((len(values_sorted) - 1) * pct)
    return values_sorted[index]


async def measure_http_order_latency(samples: int, warmup: int = 50) -> LatencyStats:
    latencies_ms = []

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        for i in range(warmup):
            await client.post("/orders", json={
                "owner_id": "http_latency",
                "client_order_id": f"warmup-{i}",
                "symbol": "AAPL",
                "side": "buy",
                "order_type": "limit",
                "price_ticks": 9900,
                "quantity": 1,
            })

        for i in range(samples):
            payload = {
                "owner_id": "http_latency",
                "client_order_id": f"sample-{i}",
                "symbol": "AAPL",
                "side": "buy",
                "order_type": "limit",
                "price_ticks": 9900,
                "quantity": 1,
            }

            start = time.perf_counter_ns()
            response = await client.post("/orders", json=payload)
            end = time.perf_counter_ns()

            if response.status_code != 200:
                raise RuntimeError(response.text)

            latencies_ms.append((end - start) / 1_000_000)

    return LatencyStats(
        name="HTTP single order latency",
        samples=samples,
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


async def main() -> None:
    stats = await measure_http_order_latency(samples=1_000)
    print_stats(stats)


if __name__ == "__main__":
    asyncio.run(main())