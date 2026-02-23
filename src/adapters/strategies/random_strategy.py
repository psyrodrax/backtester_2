# src/strategies/random_strategy.py
import random
from typing import Optional
from src.domain import events, commands
from src.domain.ports import AbstractBroker, Strategy, EventBusAdapter
from src.domain.models import OrderSide, OrderType

class RandomStrategy(EventBusAdapter, Strategy):
    """Random trading strategy:
    - At each tick, randomly decides to BUY, SELL, or HOLD.
    - Ignores all market indicators and price history.
    """
    def __init__(self, broker: AbstractBroker, timeframe: str = "1s"):
        super().__init__(broker=broker, timeframe=timeframe)
        self.position = {}  # type: dict[str, int]

    def on_start(self, cmd: commands.StartStrategyCommand):
        print("RandomStrategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("RandomStrategy ended.")

    def on_day_start(self, date):
        pass

    def on_day_end(self, date):
        pass

    def on_tick_changed(self, event: events.Event) -> Optional[commands.PlaceOrderCommand]:
        candle = self.aggregate_tick(event)
        if not candle:
            return None
        symbol = getattr(candle, "symbol", None)
        if symbol is None:
            return None
        # Randomly choose action: 0 = hold, 1 = buy, -1 = sell
        action = random.choice([0, 1, -1])
        if action == 0:
            return None  # Hold
        side = OrderSide.BUY if action == 1 else OrderSide.SELL
        self.position[symbol] = action
        action_cmd = commands.PlaceOrderCommand(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=1,
            timestamp=candle.timestamp,
        )
        self.event_bus.handle(action_cmd)
        return action_cmd

    def on_order_changed(self, event: events.Event):
        if isinstance(event, events.OrderFilled):
            print(
                f"RandomStrategy Order filled: {event.symbol} {event.side} {event.quantity} @ {event.execution_price}"
            )
