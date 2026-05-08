import asyncio
import time
from dataclasses import dataclass

import httpx


BASE_URL = "http://127.0.0.1:8000"


@dataclass
class HttpBenchmarkResult:
    name: str
    requests: int
    elapsed: float
    ok_count: int
    error_count: int

    @property
    def requests_per_sec(self) -> float:
        return self.requests / self.elapsed if self.elapsed > 0 else 0.0


async def post_order(
    client: httpx.AsyncClient,
    *,
    owner_id: str,
    client_order_id: str,
    symbol: str,
    side: str,
    order_type: str,
    price_ticks: int | None,
    quantity: int,
) -> bool:
    payload = {
        "owner_id": owner_id,
        "client_order_id": client_order_id,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "price_ticks": price_ticks,
        "quantity": quantity,
    }

    try:
        resp = await client.post("/orders", json=payload)
        return resp.status_code == 200
    except Exception:
        return False


async def run_concurrent_orders(
    *,
    name: str,
    total_requests: int,
    concurrency: int,
    side: str,
    order_type: str,
    price_ticks: int | None,
    quantity: int,
) -> HttpBenchmarkResult:
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )

    timeout = httpx.Timeout(10.0)

    ok_count = 0
    error_count = 0
    request_counter = 0
    lock = asyncio.Lock()

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        limits=limits,
        timeout=timeout,
    ) as client:

        async def worker(worker_id: int) -> None:
            nonlocal ok_count, error_count, request_counter

            while True:
                async with lock:
                    if request_counter >= total_requests:
                        return
                    i = request_counter
                    request_counter += 1

                ok = await post_order(
                    client,
                    owner_id="http_bench",
                    client_order_id=f"{name}-{worker_id}-{i}",
                    symbol="AAPL",
                    side=side,
                    order_type=order_type,
                    price_ticks=price_ticks,
                    quantity=quantity,
                )

                if ok:
                    ok_count += 1
                else:
                    error_count += 1

        start = time.perf_counter()

        await asyncio.gather(
            *(worker(worker_id) for worker_id in range(concurrency))
        )

        elapsed = time.perf_counter() - start

    return HttpBenchmarkResult(
        name=name,
        requests=total_requests,
        elapsed=elapsed,
        ok_count=ok_count,
        error_count=error_count,
    )


async def seed_resting_asks(n: int, price_ticks: int = 10_000) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        for i in range(n):
            resp = await client.post(
                "/orders",
                json={
                    "owner_id": "seed",
                    "client_order_id": f"seed-ask-{i}",
                    "symbol": "AAPL",
                    "side": "sell",
                    "order_type": "limit",
                    "price_ticks": price_ticks,
                    "quantity": 1,
                },
            )
            resp.raise_for_status()


async def benchmark_resting_limit_adds(
    total_requests: int,
    concurrency: int,
) -> HttpBenchmarkResult:
    return await run_concurrent_orders(
        name="HTTP resting limit adds",
        total_requests=total_requests,
        concurrency=concurrency,
        side="buy",
        order_type="limit",
        price_ticks=9_900,
        quantity=1,
    )


async def benchmark_market_crosses(
    total_requests: int,
    concurrency: int,
) -> HttpBenchmarkResult:
    print(f"Seeding {total_requests:,} resting asks...")
    await seed_resting_asks(total_requests)

    return await run_concurrent_orders(
        name="HTTP market crosses",
        total_requests=total_requests,
        concurrency=concurrency,
        side="buy",
        order_type="market",
        price_ticks=None,
        quantity=1,
    )

async def post_order_batch(
    client: httpx.AsyncClient,
    *,
    batch_id: int,
    batch_size: int,
) -> bool:
    orders = []

    for i in range(batch_size):
        orders.append({
            "owner_id": "http_batch_bench",
            "client_order_id": f"batch-{batch_id}-{i}",
            "symbol": "AAPL",
            "side": "buy",
            "order_type": "limit",
            "price_ticks": 9_900,
            "quantity": 1,
        })

    try:
        resp = await client.post("/orders/batch", json={"orders": orders})
        return resp.status_code == 200
    except Exception:
        return False
    
async def benchmark_batch_orders(
    *,
    total_orders: int,
    batch_size: int,
    concurrency: int,
) -> HttpBenchmarkResult:
    total_batches = total_orders // batch_size

    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )

    ok_batches = 0
    error_batches = 0
    batch_counter = 0
    lock = asyncio.Lock()

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        limits=limits,
        timeout=httpx.Timeout(30.0),
    ) as client:

        async def worker(worker_id: int) -> None:
            nonlocal ok_batches, error_batches, batch_counter

            while True:
                async with lock:
                    if batch_counter >= total_batches:
                        return
                    batch_id = batch_counter
                    batch_counter += 1

                ok = await post_order_batch(
                    client,
                    batch_id=batch_id,
                    batch_size=batch_size,
                )

                if ok:
                    ok_batches += 1
                else:
                    error_batches += 1

        start = time.perf_counter()
        await asyncio.gather(*(worker(i) for i in range(concurrency)))
        elapsed = time.perf_counter() - start

    return HttpBenchmarkResult(
        name=f"HTTP batch orders size={batch_size}",
        requests=total_orders,
        elapsed=elapsed,
        ok_count=ok_batches * batch_size,
        error_count=error_batches * batch_size,
    )

def print_result(result: HttpBenchmarkResult) -> None:
    print()
    print(
        f"{'Benchmark':<32} "
        f"{'Requests':>12} "
        f"{'OK':>12} "
        f"{'Errors':>12} "
        f"{'Seconds':>10} "
        f"{'Req/sec':>15}"
    )
    print("-" * 100)
    print(
        f"{result.name:<32} "
        f"{result.requests:>12,} "
        f"{result.ok_count:>12,} "
        f"{result.error_count:>12,} "
        f"{result.elapsed:>10.3f} "
        f"{result.requests_per_sec:>15,.0f}"
    )
    print()


async def main() -> None:
    total_requests = 10_000
    concurrency = 100

    print(f"BASE_URL={BASE_URL}")
    print(f"total_requests={total_requests:,}")
    print(f"concurrency={concurrency}")

    # result = await benchmark_resting_limit_adds(
    #     total_requests=total_requests,
    #     concurrency=concurrency,
    # )
    # print_result(result)
    result = await benchmark_batch_orders(
        total_orders=100_0000,
        batch_size=1,
        concurrency=20,
    )
    print_result(result)
    # Run this separately if you want crossing benchmark.
    # It mutates the same running server state heavily.
    #
    # result = await benchmark_market_crosses(
    #     total_requests=total_requests,
    #     concurrency=concurrency,
    # )
    # print_result(result)


if __name__ == "__main__":
    asyncio.run(main())