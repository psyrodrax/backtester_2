from collections import deque
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf

from src.domain import commands, events
from src.domain.models import OrderSide, OrderType
from src.domain.ports import AbstractBroker, EventBusAdapter, Strategy
from src.analysis.candlesticks import CandlestickAggregator


class Test(EventBusAdapter, Strategy):
    def __init__(self, broker: AbstractBroker):
        super().__init__(broker=broker)
        self.close_history = deque(maxlen=2)
        self.last_price = 0.0
        self.timestamp = None
        self.symbol = None
        self.exposed = False
        self.high = 0.0
        self.quantity = 0
        self.vvix_cache = {}
        self.candlestick = CandlestickAggregator(timeframe=timedelta(seconds=1))
    
    @property
    def name(self):
        return "VVIX Dip Buy Strategy"

    def on_start(self, cmd: commands.StartStrategyCommand):
        print("\nTest strategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("\nTest strategy ended.")

    def on_day_start(self, event: events.DayStarted):
        self.close_history.append(self.last_price)
        # Cache VVIX for the day
        date_str = event.date.strftime("%Y-%m-%d")
        if date_str not in self.vvix_cache:
            try:
                self.vvix_cache[date_str] = self.get_open(event.date, "VVIX")
            except Exception:
                self.vvix_cache[date_str] = None

    def on_day_end(self, event: events.DayEnded):
        if self.exposed and self.quantity > 0:
            if self.high > self.close_history[1]:
                action_cmd = commands.PlaceOrderCommand(
                    symbol=self.symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=self.quantity,
                    timestamp=self.timestamp,
                )
                self.exposed = False
                self.event_bus.handle(action_cmd)
        else:
            date_str = event.date.strftime("%Y-%m-%d")
            vvix = self.vvix_cache.get(date_str, None)
            if (
                len(self.close_history) == 2
                and vvix is not None
                and vvix < 110
                and self.last_price < self.close_history[1]
                and self.close_history[1] < self.close_history[0]
            ):
                self.quantity = int(self.broker.available_cash // self.last_price)
                # print(f"\nLast price: {self.last_price}")

                action_cmd = commands.PlaceOrderCommand(
                    symbol=self.symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=self.quantity,
                    timestamp=self.timestamp,
                )
                self.exposed = True
                self.event_bus.handle(action_cmd)

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        candle_event = self.candlestick.aggregate_tick(event)
        if not candle_event:
            return None
        self.last_price = candle_event.close
        self.symbol = candle_event.symbol
        self.timestamp = candle_event.timestamp
        if candle_event.high > self.high:
            self.high = candle_event.high
        return None

    def on_order_changed(self, event: events.OrderFilled):
        order = event.order
        self.broker.add_comment(order.filled_at, f"\n{order.side} {order.quantity} {order.symbol} at {order.avg_fill_price} on {order.filled_at}")
        self.broker.add_comment(order.filled_at, f"Remaining cash: {self.broker.available_cash}")

    def get_open(self, datetime_started_utc: datetime, ticker: str = "VIX") -> float:
        vix = yf.Ticker(f"^{ticker}")
        date_str = datetime_started_utc.strftime("%Y-%m-%d")
        next_date_str = (datetime_started_utc + timedelta(days=1)).strftime("%Y-%m-%d")
        hist = vix.history(start=date_str, end=next_date_str, interval="1d")
        if hist.empty:
            return None
        return float(hist.iloc[0]["Open"])
