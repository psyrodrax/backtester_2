# src/strategies/ma_volatility_strategy.py
from collections import deque, defaultdict
from typing import Optional, Dict
from src.domain import events, commands
from src.domain.ports import AbstractBroker, Strategy, EventBusAdapter
from src.domain.models import OrderSide, OrderType
import numpy as np

class MAVolatilityStrategy(EventBusAdapter, Strategy):
    """Moving Average Crossover with Volatility Filter:
    - Buy when short MA crosses above long MA and volatility is high.
    - Sell when short MA crosses below long MA and volatility is high.
    - Avoid trading when volatility is low.
    """
    def __init__(
        self,
        broker: AbstractBroker,
        short_window: int = 10,
        long_window: int = 50,
        vol_window: int = 20,
        vol_threshold: float = 0.5,
        timeframe: str = "1m",
    ):
        super().__init__(broker=broker, timeframe=timeframe)
        self.short_window = short_window
        self.long_window = long_window
        self.vol_window = vol_window
        self.vol_threshold = vol_threshold
        self.closes = defaultdict(lambda: deque(maxlen=max(long_window, vol_window) + 1))
        self.position = defaultdict(int)  # -1 short, 0 flat, 1 long
        self.last_signal = defaultdict(lambda: None)

    def on_start(self, cmd: commands.StartStrategyCommand):
        print("MAVolatilityStrategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("MAVolatilityStrategy ended.")

    def on_day_start(self, date):
        pass

    def on_day_end(self, date):
        pass

    def _compute_indicators(self, symbol: str):
        closes = self.closes[symbol]
        if len(closes) < max(self.long_window, self.vol_window):
            return None, None, None
        short_ma = np.mean(list(closes)[-self.short_window:])
        long_ma = np.mean(list(closes)[-self.long_window:])
        returns = np.diff(list(closes)[-self.vol_window:]) / np.array(list(closes)[-self.vol_window:-1])
        volatility = np.std(returns)
        return short_ma, long_ma, volatility

    def on_tick_changed(self, event: events.Event) -> Optional[commands.PlaceOrderCommand]:
        candle = self.aggregate_tick(event)
        if not candle:
            return None
        symbol = getattr(candle, "symbol", None)
        if symbol is None:
            return None
        self.closes[symbol].append(candle.close)
        short_ma, long_ma, volatility = self._compute_indicators(symbol)
        if short_ma is None:
            return None
        pos = self.position[symbol]
        last_signal = self.last_signal[symbol]
        action_cmd = None
        # Only trade if volatility is above threshold
        if volatility is not None and volatility > self.vol_threshold:
            # Long entry
            if pos <= 0 and last_signal is not None and last_signal < 0 and short_ma > long_ma:
                self.position[symbol] = 1
                action_cmd = commands.PlaceOrderCommand(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=1,
                    timestamp=candle.timestamp,
                )
            # Short entry
            elif pos >= 0 and last_signal is not None and last_signal > 0 and short_ma < long_ma:
                self.position[symbol] = -1
                action_cmd = commands.PlaceOrderCommand(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=1,
                    timestamp=candle.timestamp,
                )
        # Track last signal direction
        self.last_signal[symbol] = np.sign(short_ma - long_ma)
        if action_cmd:
            self.event_bus.handle(action_cmd)
        return action_cmd

    def on_order_changed(self, event: events.Event):
        if isinstance(event, events.OrderFilled):
            print(
                f"MA Volatility Order filled: {event.symbol} {event.side} {event.quantity} @ {event.execution_price}"
            )
