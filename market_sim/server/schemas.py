from pydantic import BaseModel
from typing import Optional

class OrderRequest(BaseModel):
    owner_id: str
    client_order_id: str | None = None
    symbol: str
    side: str              # "buy" / "sell"
    order_type: str        # "limit" / "market"
    price_ticks: int | None = None
    quantity: int

class OrderResponse(BaseModel):
    accepted: bool
    engine_order_id: int | None = None
    reason: str | None = None
    trade_count: int = 0

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

class BatchOrderRequest(BaseModel):
    orders: list[OrderRequest]

class BatchOrderResponse(BaseModel):
    accepted: int
    rejected: int
    trade_count: int
    results: list[OrderResponse]