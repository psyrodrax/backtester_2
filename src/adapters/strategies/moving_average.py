# src/strategies/moving_average.py

from collections import deque
from typing import Optional
from src.domain import events, commands
from src.domain.ports import Strategy, EventBusAdapter
from src.domain.models import OrderSide, OrderType


class MovingAverageStrategy(EventBusAdapter, Strategy):
    def __init__(self, short_window=5, long_window=20, timeframe="1s"):
        super().__init__(timeframe=timeframe)
        self.short_window = short_window
        self.long_window = long_window
        self.prices = deque(maxlen=long_window)
        self.position = 0  # simple position tracker

    def on_start(self):
        print("MovingAverageStrategy started.")

    def on_end(self):
        print("MovingAverageStrategy ended.")

    def on_day_start(self, date):
        print(f"Trading day {date} started.")

    def on_day_end(self, date):
        print(f"Trading day {date} ended.")

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        # aggregate ticks into candles
        candle_event = self.aggregate_tick(event)

        if not candle_event:
            return

        self.prices.append(candle_event.close)
        if len(self.prices) < self.long_window:
            return

        short_ma = sum(list(self.prices)[-self.short_window :]) / self.short_window
        long_ma = sum(self.prices) / self.long_window

        # Simple logic: flip position on crossover
        if short_ma > long_ma and self.position <= 0:
            self.position = 1
            command = commands.PlaceOrderCommand(
                symbol=candle_event.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle_event.timestamp,
            )
            self.event_bus.handle(command)
        elif short_ma < long_ma and self.position >= 0:
            self.position = -1
            command = commands.PlaceOrderCommand(
                symbol=candle_event.symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle_event.timestamp,
            )
            self.event_bus.handle(command)

    def on_order_changed(self, event: events.Event):
        if isinstance(event, events.OrderFilled):
            print(
                f"Order filled: {event.symbol} {event.side} {event.quantity} @ {event.execution_price}"
            )
