# src/strategies/rsi_strategy.py
from collections import deque, defaultdict
from typing import Optional, Dict
from src.domain import events, commands
from src.domain.ports import AbstractBroker, Strategy, EventBusAdapter
from src.domain.models import OrderSide, OrderType


class RSIStrategy(EventBusAdapter, Strategy):
    """Simple RSI strategy:
    - Computes RSI over a lookback window (default 14) on candle closes.
    - Generates BUY when RSI crosses below oversold threshold then back above.
    - Generates SELL when RSI crosses above overbought threshold then back below.
    Position logic here is symmetric: flat -> long on oversold bounce; long -> flat -> short on overbought drop.
    """

    def __init__(
        self,
        broker: AbstractBroker,
        lookback: int = 14,
        oversold: float = 30.0,
        overbought: float = 70.0,
        timeframe: str = "1s",
    ):
        super().__init__(broker=broker, timeframe=timeframe)
        self.lookback = lookback
        self.oversold = oversold
        self.overbought = overbought
        # Per-symbol state
        self.closes = defaultdict(
            lambda: deque(maxlen=lookback + 1)
        )  # type: Dict[str, deque]
        self.position = defaultdict(
            int
        )  # type: Dict[str, int]  # -1 short, 0 flat, 1 long
        self.last_rsi = defaultdict(lambda: None)  # type: Dict[str, Optional[float]]

    # Lifecycle hooks
    def on_start(self, cmd: commands.StartStrategyCommand):
        print("RSIStrategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("RSIStrategy ended.")

    def on_day_start(self, date):
        pass

    def on_day_end(self, date):
        pass

    def _compute_rsi(self, symbol: str) -> Optional[float]:
        closes = self.closes[symbol]
        if len(closes) < self.lookback + 1:
            return None  # not enough data yet
        gains = 0.0
        losses = 0.0
        prev = None
        for price in closes:
            if prev is not None:
                delta = price - prev
                if delta > 0:
                    gains += delta
                elif delta < 0:
                    losses -= delta  # add absolute value
            prev = price
        if gains == 0 and losses == 0:
            return 50.0  # no movement
        avg_gain = gains / self.lookback
        avg_loss = losses / self.lookback
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        candle = self.aggregate_tick(event)
        if not candle:
            return None

        symbol = getattr(candle, "symbol", None)
        if symbol is None:
            return None

        # Update per-symbol close history
        self.closes[symbol].append(candle.close)
        rsi = self._compute_rsi(symbol)
        if rsi is None:
            return None

        prev_rsi = self.last_rsi[symbol]
        pos = self.position[symbol]
        action_cmd = None

        # Long entry condition
        if pos <= 0 and prev_rsi is not None and prev_rsi < self.oversold <= rsi:
            self.position[symbol] = 1
            action_cmd = commands.PlaceOrderCommand(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle.timestamp,
            )
        # Short entry condition
        elif pos >= 0 and prev_rsi is not None and prev_rsi > self.overbought >= rsi:
            self.position[symbol] = -1
            action_cmd = commands.PlaceOrderCommand(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle.timestamp,
            )

        # Store last RSI
        self.last_rsi[symbol] = rsi
        if action_cmd:
            self.event_bus.handle(action_cmd)
        return action_cmd

    def on_order_changed(self, event: events.Event):
        if isinstance(event, events.OrderFilled):
            print(
                f"RSI Order filled: {event.symbol} {event.side} {event.quantity} @ {event.execution_price}"
            )
