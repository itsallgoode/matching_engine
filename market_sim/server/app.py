from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Field

from market_sim.definitions import Order, OrderType, Side
from market_sim.matching_engine import MatchingEngine


app = FastAPI(
    title="Market Sim Engine",
    default_response_class=ORJSONResponse,
)

engine = MatchingEngine()
engine_lock = asyncio.Lock()


# -----------------------------
# Pydantic API Schemas
# -----------------------------

class OrderRequest(BaseModel):
    owner_id: str
    client_order_id: str | None = None
    symbol: str
    side: str
    order_type: str
    price_ticks: int | None = None
    quantity: int = Field(gt=0)


class OrderResponse(BaseModel):
    accepted: bool
    engine_order_id: int | None = None
    client_order_id: str | None = None
    reason: str | None = None
    trade_count: int = 0


class BatchOrderRequest(BaseModel):
    orders: list[OrderRequest]


class BatchOrderResponse(BaseModel):
    accepted: int
    rejected: int
    trade_count: int
    results: list[OrderResponse]


class FastBatchOrderResponse(BaseModel):
    accepted: int
    rejected: int
    trade_count: int
    first_error: str | None = None


class CancelRequest(BaseModel):
    symbol: str
    order_id: int


class CancelResponse(BaseModel):
    accepted: bool
    reason: str | None = None


class SnapshotResponse(BaseModel):
    symbol: str
    best_bid: int | None
    best_ask: int | None
    spread: int | None
    mid_price: float | None


# -----------------------------
# WebSocket Market Data Manager
# -----------------------------

class MarketDataConnectionManager:
    def __init__(self) -> None:
        self.active_connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.active_connections.discard(websocket)

    async def broadcast(self, message: dict[str, Any]) -> None:
        async with self._lock:
            connections = list(self.active_connections)

        dead: list[WebSocket] = []

        for ws in connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self.active_connections.discard(ws)


market_data = MarketDataConnectionManager()


# -----------------------------
# Helpers
# -----------------------------

def parse_side(raw_side: str) -> Side:
    try:
        return Side(raw_side.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid side: {raw_side}")


def parse_order_type(raw_order_type: str) -> OrderType:
    try:
        return OrderType(raw_order_type.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid order_type: {raw_order_type}")


def build_order(req: OrderRequest) -> Order:
    side = parse_side(req.side)
    order_type = parse_order_type(req.order_type)

    symbol = req.symbol.upper()

    if order_type == OrderType.LIMIT and req.price_ticks is None:
        raise HTTPException(status_code=400, detail="limit order must include price_ticks")

    if order_type == OrderType.MARKET and req.price_ticks is not None:
        raise HTTPException(status_code=400, detail="market order should not include price_ticks")

    return Order(
        owner_id=req.owner_id,
        symbol=symbol,
        side=side,
        order_type=order_type,
        # If your Order dataclass still uses `type`, replace the above line with:
        # type=order_type,
        price_ticks=req.price_ticks,
        quantity=req.quantity,
    )


def build_order_fast(owner_id: str, raw: dict[str, Any]) -> Order:
    side = Side(raw["side"].lower())
    order_type = OrderType(raw["order_type"].lower())

    price_ticks = raw.get("price_ticks")
    quantity = raw["quantity"]

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if order_type == OrderType.LIMIT and price_ticks is None:
        raise ValueError("limit order must include price_ticks")

    if order_type == OrderType.MARKET:
        price_ticks = None

    return Order(
        owner_id=owner_id,
        symbol=raw["symbol"].upper(),
        side=side,
        order_type=order_type,
        # If your Order dataclass still uses `type`, replace the above line with:
        # type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )


def serialize_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def serialize_enum(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    return value


def trade_to_dict(trade: Any) -> dict[str, Any]:
    data = trade.__dict__.copy()

    for key, value in list(data.items()):
        value = serialize_enum(value)
        value = serialize_datetime(value)
        data[key] = value

    return data


def snapshot_dict(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()

    return {
        "symbol": symbol,
        "best_bid": engine.best_bid(symbol),
        "best_ask": engine.best_ask(symbol),
        "spread": engine.spread(symbol),
        "mid_price": engine.mid_price(symbol),
    }


def depth_dict(symbol: str, levels: int) -> dict[str, Any]:
    return engine.levels(symbol.upper(), levels)


def market_update_event(symbol: str, trades: list[Any]) -> dict[str, Any]:
    symbol = symbol.upper()

    return {
        "type": "market_update",
        "symbol": symbol,
        "snapshot": snapshot_dict(symbol),
        "depth": depth_dict(symbol, levels=5),
        "trades": [trade_to_dict(trade) for trade in trades],
    }


# -----------------------------
# HTTP Endpoints
# -----------------------------

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/orders", response_model=OrderResponse)
async def submit_order(req: OrderRequest) -> OrderResponse:
    order = build_order(req)

    async with engine_lock:
        try:
            trades = engine.process_order(order)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        event = market_update_event(order.symbol, trades)

    await market_data.broadcast(event)

    return OrderResponse(
        accepted=True,
        engine_order_id=order.order_id,
        client_order_id=req.client_order_id,
        reason=None,
        trade_count=len(trades),
    )


@app.post("/orders/batch", response_model=BatchOrderResponse)
async def submit_orders_batch(req: BatchOrderRequest) -> BatchOrderResponse:
    results: list[OrderResponse] = []
    accepted = 0
    rejected = 0
    trade_count = 0
    affected_symbols: set[str] = set()
    all_trades: list[Any] = []

    async with engine_lock:
        for order_req in req.orders:
            try:
                order = build_order(order_req)
                trades = engine.process_order(order)

                accepted += 1
                trade_count += len(trades)
                affected_symbols.add(order.symbol)
                all_trades.extend(trades)

                results.append(
                    OrderResponse(
                        accepted=True,
                        engine_order_id=order.order_id,
                        client_order_id=order_req.client_order_id,
                        reason=None,
                        trade_count=len(trades),
                    )
                )

            except Exception as e:
                rejected += 1

                results.append(
                    OrderResponse(
                        accepted=False,
                        engine_order_id=None,
                        client_order_id=order_req.client_order_id,
                        reason=str(e),
                        trade_count=0,
                    )
                )

        events = [
            market_update_event(symbol, [t for t in all_trades if t.symbol == symbol])
            for symbol in affected_symbols
        ]

    for event in events:
        await market_data.broadcast(event)

    return BatchOrderResponse(
        accepted=accepted,
        rejected=rejected,
        trade_count=trade_count,
        results=results,
    )


@app.post("/orders/batch_fast", response_model=FastBatchOrderResponse)
async def submit_orders_batch_fast(req: BatchOrderRequest) -> FastBatchOrderResponse:
    accepted = 0
    rejected = 0
    trade_count = 0
    first_error: str | None = None
    affected_symbols: set[str] = set()
    all_trades: list[Any] = []

    async with engine_lock:
        for order_req in req.orders:
            try:
                order = build_order(order_req)
                trades = engine.process_order(order)

                accepted += 1
                trade_count += len(trades)
                affected_symbols.add(order.symbol)
                all_trades.extend(trades)

            except Exception as e:
                rejected += 1
                if first_error is None:
                    first_error = str(e)

        events = [
            market_update_event(symbol, [t for t in all_trades if t.symbol == symbol])
            for symbol in affected_symbols
        ]

    for event in events:
        await market_data.broadcast(event)

    return FastBatchOrderResponse(
        accepted=accepted,
        rejected=rejected,
        trade_count=trade_count,
        first_error=first_error,
    )


@app.post("/cancels", response_model=CancelResponse)
async def cancel_order(req: CancelRequest) -> CancelResponse:
    symbol = req.symbol.upper()

    async with engine_lock:
        accepted = engine.cancel_order(symbol, req.order_id)
        event = market_update_event(symbol, trades=[])

    await market_data.broadcast(event)

    if not accepted:
        return CancelResponse(
            accepted=False,
            reason="order not found or already inactive",
        )

    return CancelResponse(accepted=True)


@app.get("/snapshot/{symbol}", response_model=SnapshotResponse)
async def get_snapshot(symbol: str) -> SnapshotResponse:
    async with engine_lock:
        snap = snapshot_dict(symbol)

    return SnapshotResponse(**snap)


@app.get("/depth/{symbol}")
async def get_depth(symbol: str, levels: int = 5) -> dict[str, list[tuple[int, int]]]:
    if levels <= 0:
        raise HTTPException(status_code=400, detail="levels must be positive")

    async with engine_lock:
        return engine.levels(symbol.upper(), levels)


@app.get("/trades")
async def get_trades(symbol: str | None = None) -> list[dict[str, Any]]:
    async with engine_lock:
        trades = engine.trades

        if symbol is not None:
            symbol = symbol.upper()
            trades = [trade for trade in trades if trade.symbol == symbol]

        return [trade_to_dict(trade) for trade in trades]


# -----------------------------
# WebSocket Endpoints
# -----------------------------

@app.websocket("/ws/market-data")
async def websocket_market_data(websocket: WebSocket) -> None:
    await market_data.connect(websocket)

    try:
        while True:
            # Keep connection alive and allow simple ping messages.
            msg = await websocket.receive_json()

            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        await market_data.disconnect(websocket)
    
def build_order_from_array(owner_id: str, raw: list[Any]) -> Order:
    if len(raw) != 5:
        raise ValueError("order array must have 5 fields")

    symbol_raw, side_raw, order_type_raw, price_ticks, quantity = raw

    side = Side(side_raw)
    order_type = OrderType(order_type_raw)

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if order_type == OrderType.LIMIT and price_ticks is None:
        raise ValueError("limit order must include price_ticks")

    if order_type == OrderType.MARKET:
        price_ticks = None

    return Order(
        owner_id=owner_id,
        symbol=symbol_raw.upper(),
        side=side,
        order_type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )

def build_order_from_array(owner_id: str, raw: list[Any]) -> Order:
    if len(raw) != 5:
        raise ValueError("order array must have 5 fields: [symbol, side, order_type, price_ticks, quantity]")

    symbol_raw, side_raw, order_type_raw, price_ticks, quantity = raw

    side = Side(side_raw.lower())
    order_type = OrderType(order_type_raw.lower())

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if order_type == OrderType.LIMIT and price_ticks is None:
        raise ValueError("limit order must include price_ticks")

    if order_type == OrderType.MARKET:
        price_ticks = None

    return Order(
        owner_id=owner_id,
        symbol=symbol_raw.upper(),
        side=side,
        order_type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )

@app.websocket("/ws/order-entry/{owner_id}")
async def websocket_order_entry(websocket: WebSocket, owner_id: str) -> None:
    await websocket.accept()

    try:
        while True:
            msg = await websocket.receive_json()
            msg_type = msg.get("type")
            
            if msg_type == "ping":
                await websocket.send_json({"type": "pong"})
                continue

            if msg_type == "orders_batch_compact_multi":
                raw_orders = msg.get("orders", [])

                accepted = 0
                rejected = 0
                trade_count = 0
                first_error = None

                async with engine_lock:
                    for raw in raw_orders:
                        try:
                            order = build_order_from_compact_multi(owner_id, raw)
                            trades = engine.process_order(order)

                            accepted += 1
                            trade_count += len(trades)

                        except Exception as e:
                            rejected += 1
                            if first_error is None:
                                first_error = str(e)

                await websocket.send_json({
                    "type": "batch_ack",
                    "accepted": accepted,
                    "rejected": rejected,
                    "trade_count": trade_count,
                    "first_error": first_error,
                })
                continue
            if msg_type == "orders_batch_compact_v2":
                symbol = msg["symbol"].upper()
                raw_orders = msg.get("orders", [])

                accepted = 0
                rejected = 0
                trade_count = 0
                first_error = None

                async with engine_lock:
                    for raw in raw_orders:
                        try:
                            order = build_order_from_compact_v2(owner_id, symbol, raw)
                            trades = engine.process_order(order)

                            accepted += 1
                            trade_count += len(trades)

                        except Exception as e:
                            rejected += 1
                            if first_error is None:
                                first_error = str(e)

                await websocket.send_json({
                    "type": "batch_ack",
                    "accepted": accepted,
                    "rejected": rejected,
                    "trade_count": trade_count,
                    "first_error": first_error,
                })
                continue
            if msg_type != "orders_batch_array":
                await websocket.send_json({
                    "type": "error",
                    "reason": f"unknown message type: {msg_type}",
                })
                continue

            raw_orders = msg.get("orders", [])

            accepted = 0
            rejected = 0
            trade_count = 0
            first_error = None

            for raw in raw_orders:
                try:
                    order = build_order_from_array(owner_id, raw)
                    trades = engine.process_order(order)

                    accepted += 1
                    trade_count += len(trades)

                except Exception as e:
                    rejected += 1
                    if first_error is None:
                        first_error = str(e)

            await websocket.send_json({
                "type": "batch_ack",
                "accepted": accepted,
                "rejected": rejected,
                "trade_count": trade_count,
                "first_error": first_error,
            })

    except WebSocketDisconnect:
        return

def side_from_code(code: int) -> Side:
    if code == 0:
        return Side.BUY
    if code == 1:
        return Side.SELL
    raise ValueError(f"invalid side code: {code}")


def order_type_from_code(code: int) -> OrderType:
    if code == 0:
        return OrderType.LIMIT
    if code == 1:
        return OrderType.MARKET
    raise ValueError(f"invalid order type code: {code}")


def build_order_from_compact_v2(
    owner_id: str,
    symbol: str,
    raw: list[Any],
) -> Order:
    if len(raw) != 4:
        raise ValueError(
            "order array must have 4 fields: "
            "[side_code, type_code, price_ticks, quantity]"
        )

    side_code, type_code, price_ticks, quantity = raw

    side = side_from_code(side_code)
    order_type = order_type_from_code(type_code)

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if order_type == OrderType.LIMIT and price_ticks is None:
        raise ValueError("limit order must include price_ticks")

    if order_type == OrderType.MARKET:
        price_ticks = None

    return Order(
        owner_id=owner_id,
        symbol=symbol.upper(),
        side=side,
        order_type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )

def build_order_from_compact_multi(owner_id: str, raw: list[Any]) -> Order:
    if len(raw) != 5:
        raise ValueError(
            "order array must have 5 fields: "
            "[symbol, side_code, type_code, price_ticks, quantity]"
        )

    symbol_raw, side_code, type_code, price_ticks, quantity = raw

    side = side_from_code(side_code)
    order_type = order_type_from_code(type_code)

    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if order_type == OrderType.LIMIT and price_ticks is None:
        raise ValueError("limit order must include price_ticks")

    if order_type == OrderType.MARKET:
        price_ticks = None

    return Order(
        owner_id=owner_id,
        symbol=symbol_raw.upper(),
        side=side,
        order_type=order_type,
        price_ticks=price_ticks,
        quantity=quantity,
    )