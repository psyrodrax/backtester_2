# src/adapters/strategies/buy_open_sell_close_strategy.py
from typing import Optional

from src.config import get_backtest_symbols
from src.domain import commands, events
from src.domain.models import OrderSide, OrderType
from src.domain.ports import AbstractBroker, EventBusAdapter, Strategy


class BuyOpenSellCloseStrategy(EventBusAdapter, Strategy):
    """Super simple strategy: Buy at day start, sell at day end."""

    def __init__(self, broker: AbstractBroker, timeframe: str = "1d"):
        super().__init__(broker=broker, timeframe=timeframe)
        self.position = {}
        self.symbols = get_backtest_symbols()

    def on_day_start(self, event: events.DayStarted):
        # Buy at the start of the day for all symbols
        for symbol in self.symbols:
            if self.position.get(symbol, 0) == 0:
                cmd = commands.PlaceOrderCommand(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=1,
                    timestamp=event.date,
                )
                self.position[symbol] = 1
                self.event_bus.handle(cmd)

    def on_day_end(self, event: events.DayEnded):
        # Sell at the end of the day for all symbols
        for symbol in self.symbols:
            if self.position[symbol] == 1:
                cmd = commands.PlaceOrderCommand(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=1,
                    timestamp=event.date,
                )
                self.position[symbol] = 0
                self.event_bus.handle(cmd)

    def on_tick_changed(
        self, event: events.QuoteReceived
    ) -> Optional[commands.PlaceOrderCommand]:
        return None

    def on_order_changed(self, event: events.Event):
        pass

    def on_start(self, cmd: commands.StartStrategyCommand):
        pass

    def on_end(self, cmd: commands.EndStrategyCommand):
        pass
