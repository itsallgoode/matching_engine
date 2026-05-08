from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum

class Side(Enum):
    BUY = "buy"
    SELL = "sell"

class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"

class OrderStatus(Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"

@dataclass
class Order:
    symbol: str
    owner_id: str
    side: Side
    order_type: OrderType
    price_ticks: int | None
    quantity: int
    order_id: int | None = None
    status: OrderStatus = OrderStatus.OPEN
    remaining_quantity: int = field(init=False)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        self.remaining_quantity = self.quantity

    def is_buy(self) -> bool:
        return self.side == Side.BUY
    
    def is_sell(self) -> bool:
        return self.side == Side.SELL
    
    def is_limit(self) -> bool:
        return self.order_type == OrderType.LIMIT
    
    def is_market(self) -> bool:
        return self.order_type == OrderType.MARKET
    
    def is_active(self) -> bool:
        return self.status in {
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        }
    
    def is_filled(self) -> bool:
        return self.remaining_quantity == 0
    
    def fill(self, quantity: int) -> None:
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        if quantity > self.remaining_quantity:
            raise ValueError("cannot fill more than remaining quantity")

        self.remaining_quantity -= quantity

        if self.is_filled():
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIALLY_FILLED

@dataclass
class Trade:
    symbol: str
    trade_id: int
    buy_owner_id: str
    sell_owner_id: str
    buy_order_id: int
    sell_order_id: int
    aggressor_order_id: int
    aggressor_side: Side
    resting_order_id: int
    price_ticks: int
    quantity: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))