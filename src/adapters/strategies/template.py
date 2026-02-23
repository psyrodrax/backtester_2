from typing import Optional
from src.domain import events, commands
from src.domain import commands, events
from datetime import datetime, timedelta
from src.domain.models import OrderSide, OrderType
from src.domain.ports import AbstractBroker, EventBusAdapter, Strategy
from src.analysis.candlesticks import CandlestickAggregator

class Template(EventBusAdapter, Strategy):
    def __init__(
        self,
        broker: AbstractBroker,
    ):
        super().__init__(broker=broker)
        self.candlestick = CandlestickAggregator(timeframe=timedelta(seconds=1))

    @property
    def name(self):
        return "Strat name"

    def on_start(self, cmd: commands.StartStrategyCommand):
        print("\nTemplate strategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("\nTemplate strategy ended.")

    def on_day_start(self, event: events.DayStarted):
        print(f"\nTrading day {event.date} started.")

    def on_day_end(self, event: events.DayEnded):
        print(f"\nTrading day {event.date} ended.")

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        pass

    def on_order_changed(self, event: events.Event):
        pass
