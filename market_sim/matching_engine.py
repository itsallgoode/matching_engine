import itertools
from collections import defaultdict
from market_sim.order_book import OrderBook
from market_sim.definitions import Order, Trade
import pandas as pd

class MatchingEngine:
    def __init__(self) -> None:
        self.order_id_generator = itertools.count(1)
        self.trade_id_generator = itertools.count(1)

        self.books: dict[str, OrderBook] = defaultdict(OrderBook)
        self.trades: list[Trade] = []
        
    def process_order(self, order: Order) -> list[Trade]:
        order.order_id = next(self.order_id_generator)
        
        book = self.books[order.symbol]
        trades = book.process_order(order, self.trade_id_generator)

        self.trades.extend(trades)
        
        return trades

    def cancel_order(self, symbol: str, order_id: int) -> bool:
        return self.books[symbol].cancel_order(order_id)

    def best_bid(self, symbol: str) -> int | None:
        return self.books[symbol].best_bid()

    def best_ask(self, symbol: str) -> int | None:
        return self.books[symbol].best_ask()

    def levels(self, symbol: str, levels: int) -> dict[str, list[tuple[int, int]]]:
        return self.books[symbol].levels(levels)

    def get_trades(self, symbol: str=None) -> pd.DataFrame:
        trades_df = pd.DataFrame(trade.__dict__ for trade in self.trades)

        if symbol is not None and not trades_df.empty():
            return trades_df[trades_df["symbol"] == symbol]
        
        return trades_df
    
    def spread(self, symbol: str) -> int | None:
        return self.books[symbol].spread()
    
    def mid_price(self, symbol: str) -> float | None:
        return self.books[symbol].mid_price()