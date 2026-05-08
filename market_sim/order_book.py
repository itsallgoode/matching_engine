from collections import defaultdict, deque
from market_sim.definitions import Order, Trade, OrderStatus, Side
from collections.abc import Iterator
import heapq

class OrderBook:
    def __init__(self) -> None:
        self.bids: dict[int, deque[Order]] = defaultdict(deque)
        self.asks: dict[int, deque[Order]] = defaultdict(deque)

        self.bid_prices: list[int] = []  # store negative prices
        self.ask_prices: list[int] = []  # store positive prices

        self.orders_by_id: dict[int, Order] = {}
    
    def process_order(self, order: Order, trade_id_generator: Iterator[int]) -> list[Trade]:
        if order.is_limit() and order.price_ticks is None:
            raise ValueError("Limit order must have price")

        if order.is_buy():
            trades = self._match_buy(order, trade_id_generator)
        elif order.is_sell():
            trades = self._match_sell(order, trade_id_generator)
        else:
            raise ValueError("Invalid order side")
        
        if order.is_limit() and order.is_active():
            self._add_to_book(order)

        if order.is_market() and order.is_active():
            order.status = OrderStatus.CANCELED

        return trades
    
    def cancel_order(self, order_id: int) -> bool:
        order = self.orders_by_id.pop(order_id, None)

        if order is None:
            return False
        
        order.status = OrderStatus.CANCELED

        return True

    def best_bid(self) -> int | None:
        while self.bid_prices:
            price = -self.bid_prices[0]

            if price not in self.bids:
                heapq.heappop(self.bid_prices)
                continue

            queue = self.bids[price]

            while queue and not queue[0].is_active():
                queue.popleft()
            
            if not queue:
                del self.bids[price]
                heapq.heappop(self.bid_prices)
                continue
            
            return price
        
        return None

    def best_ask(self) -> int | None:
        while self.ask_prices:
            price = self.ask_prices[0]

            if price not in self.asks:
                heapq.heappop(self.ask_prices)
                continue

            queue = self.asks[price]

            while queue and not queue[0].is_active():
                queue.popleft()
            
            if not queue:
                del self.asks[price]
                heapq.heappop(self.ask_prices)
                continue
            
            return price

        return None

    def _match_buy(self, order: Order, trade_id_generator: Iterator[int]) -> list[Trade]:
        trades = []

        while order.is_active():
            best_price = self.best_ask()

            if best_price is None or (order.is_limit() and best_price > order.price_ticks):
                break

            oldest_sell_order = self.asks[best_price][0]
            trade_quantity = min(oldest_sell_order.remaining_quantity, order.remaining_quantity)

            oldest_sell_order.fill(trade_quantity)
            order.fill(trade_quantity)

            trade = Trade(
                symbol=order.symbol,
                trade_id=next(trade_id_generator),
                buy_owner_id=order.owner_id,
                sell_owner_id=oldest_sell_order.owner_id,
                buy_order_id=order.order_id,
                sell_order_id=oldest_sell_order.order_id,
                aggressor_order_id=order.order_id,
                aggressor_side=Side.BUY,
                resting_order_id=oldest_sell_order.order_id,
                price_ticks=best_price,
                quantity=trade_quantity,
                )

            trades.append(trade)

            if not oldest_sell_order.is_active():
                del self.orders_by_id[oldest_sell_order.order_id]
                self.asks[best_price].popleft()

                if not self.asks[best_price]:
                    del self.asks[best_price]

        return trades

    def _match_sell(self, order: Order, trade_id_generator: Iterator[int]) -> list[Trade]:
        trades = []
        while order.is_active():
            best_price = self.best_bid()

            if best_price is None or (order.is_limit() and best_price < order.price_ticks):
                break

            oldest_buy_order = self.bids[best_price][0]
            trade_quantity = min(oldest_buy_order.remaining_quantity, order.remaining_quantity)

            oldest_buy_order.fill(trade_quantity)
            order.fill(trade_quantity)

            trade = Trade(
                symbol=order.symbol,
                trade_id=next(trade_id_generator),
                buy_owner_id=oldest_buy_order.owner_id,
                sell_owner_id=order.owner_id,
                buy_order_id=oldest_buy_order.order_id,
                sell_order_id=order.order_id,
                aggressor_order_id=order.order_id,
                aggressor_side=Side.SELL,
                resting_order_id=oldest_buy_order.order_id,
                price_ticks=best_price,
                quantity=trade_quantity
            )
            
            trades.append(trade)

            if not oldest_buy_order.is_active():
                del self.orders_by_id[oldest_buy_order.order_id]
                self.bids[best_price].popleft()

                if not self.bids[best_price]:
                    del self.bids[best_price]
        
        return trades

    def _add_to_book(self, order: Order) -> None:
        if order.is_market():
            raise ValueError("Market orders cannot be added to book")

        if order.remaining_quantity <= 0:
            raise ValueError("remaining_quantity must be > 0")

        if order.is_buy():
            if order.price_ticks not in self.bids:
                heapq.heappush(self.bid_prices, -order.price_ticks)
            self.bids[order.price_ticks].append(order)
            
        elif order.is_sell():
            if order.price_ticks not in self.asks:
                heapq.heappush(self.ask_prices, order.price_ticks)
            self.asks[order.price_ticks].append(order)
            
        self.orders_by_id[order.order_id] = order
    
    def levels(self, levels: int) -> dict[str, list[tuple[int, int]]]:
        bids = []
        asks = []
        
        for price in sorted(self.bids.keys(), reverse=True):
            queue = self.bids[price]

            quantity = sum(
                order.remaining_quantity
                for order in queue
                if order.is_active()
            )
            
            if quantity > 0:
                bids.append((price, quantity))
            
            if len(bids) == levels:
                break
        
        for price in sorted(self.asks.keys()):
            queue = self.asks[price]

            quantity = sum(
                order.remaining_quantity
                for order in queue
                if order.is_active()
            )

            if quantity > 0:
                asks.append((price, quantity))

            if len(asks) == levels:
                break

        return {"bids": bids, "asks": asks}
    
    def spread(self) -> int | None:
        bid = self.best_bid()
        ask = self.best_ask()

        return None if bid is None or ask is None else ask - bid

    def mid_price(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()

        return None if bid is None or ask is None else (bid + ask) / 2
