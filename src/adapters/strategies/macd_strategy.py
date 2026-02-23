# src/adapters/strategies/macd_strategy.py
from collections import deque, defaultdict
from typing import Optional, Dict
from src.domain import events, commands
from src.domain.ports import AbstractBroker, Strategy, EventBusAdapter
from src.domain.models import OrderSide, OrderType
import numpy as np


class MACDStrategy(EventBusAdapter, Strategy):
    """MACD Trading Strategy with 200-day MA filter and optional support/resistance confirmation."""

    def __init__(
        self,
        broker: AbstractBroker,
        short_ema: int = 12,
        long_ema: int = 26,
        signal_ema: int = 9,
        ma_trend_window: int = 50,
        support_resistance: Optional[Dict[str, Dict[str, float]]] = None,
        rr_ratio: float = 1.5,
        timeframe: str = "1d",
    ):
        super().__init__(broker=broker, timeframe=timeframe)
        self.short_ema = short_ema
        self.long_ema = long_ema
        self.signal_ema = signal_ema
        self.ma_trend_window = ma_trend_window
        self.rr_ratio = rr_ratio
        self.closes = defaultdict(lambda: deque(maxlen=self.ma_trend_window + 30))
        self.position = defaultdict(int)  # -1 short, 0 flat, 1 long
        self.last_macd = defaultdict(lambda: None)
        self.last_signal = defaultdict(lambda: None)
        self.support_resistance = support_resistance or defaultdict(dict)
        self.stop_loss = defaultdict(lambda: None)
        self.profit_target = defaultdict(lambda: None)

    def _ema(self, arr, window):
        arr = np.array(arr)
        if len(arr) < window:
            return None
        weights = np.exp(np.linspace(-1.0, 0.0, window))
        weights /= weights.sum()
        a = np.convolve(arr, weights, mode="valid")
        return a[-1]

    def _compute_indicators(self, symbol: str):
        closes = list(self.closes[symbol])
        if len(closes) < max(self.long_ema, self.signal_ema, self.ma_trend_window):
            return None, None, None, None, None
        macd_line = self._ema(closes, self.short_ema) - self._ema(closes, self.long_ema)
        signal_line = self._ema(
            [self._ema(closes, self.short_ema) - self._ema(closes, self.long_ema) for _ in range(self.signal_ema)],
            self.signal_ema,
        )
        histogram = macd_line - signal_line if signal_line is not None else None
        zero_line = 0.0
        ma_trend = np.mean(closes[-self.ma_trend_window:])
        return macd_line, signal_line, histogram, zero_line, ma_trend

    def _is_support(self, symbol, price):
        return price <= self.support_resistance[symbol].get("support", float("-inf"))

    def _is_resistance(self, symbol, price):
        return price >= self.support_resistance[symbol].get("resistance", float("inf"))

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        candle = self.aggregate_tick(event)
        if not candle:
            return None
        symbol = getattr(candle, "symbol", None)
        if symbol is None:
            return None
        price = candle.close
        self.closes[symbol].append(price)
        macd_line, signal_line, histogram, _, ma_trend = self._compute_indicators(
            symbol
        )
        if macd_line is None or signal_line is None or histogram is None:
            return None
        pos = self.position[symbol]
        action_cmd = None
        # Trend filter
        uptrend = price > ma_trend
        downtrend = price < ma_trend
        # MACD cross logic (no zero line or support/resistance required)
        macd_cross_up = (
            self.last_macd[symbol] is not None
            and self.last_macd[symbol] < self.last_signal[symbol]
            and macd_line > signal_line
        )
        macd_cross_down = (
            self.last_macd[symbol] is not None
            and self.last_macd[symbol] > self.last_signal[symbol]
            and macd_line < signal_line
        )
        # Entry logic: trade on MACD cross and trend direction
        if pos <= 0 and macd_cross_up and uptrend:
            self.position[symbol] = 1
            stop = ma_trend * 0.99
            target = price + (price - stop) * self.rr_ratio
            self.stop_loss[symbol] = stop
            self.profit_target[symbol] = target
            action_cmd = commands.PlaceOrderCommand(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle.timestamp,
            )
        elif pos >= 0 and macd_cross_down and downtrend:
            self.position[symbol] = -1
            stop = ma_trend * 1.01
            target = price - (stop - price) * self.rr_ratio
            self.stop_loss[symbol] = stop
            self.profit_target[symbol] = target
            action_cmd = commands.PlaceOrderCommand(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=1,
                timestamp=candle.timestamp,
            )
        # Exit logic: stop loss or profit target hit
        if pos == 1 and (
            price <= self.stop_loss[symbol] or price >= self.profit_target[symbol]
        ):
            self.position[symbol] = 0
        elif pos == -1 and (
            price >= self.stop_loss[symbol] or price <= self.profit_target[symbol]
        ):
            self.position[symbol] = 0
        self.last_macd[symbol] = macd_line
        self.last_signal[symbol] = signal_line
        if action_cmd:
            self.event_bus.handle(action_cmd)
        return action_cmd
    
    def on_start(self, cmd):
        pass

    def on_end(self, cmd):
        pass

    def on_day_start(self, date):
        pass

    def on_day_end(self, date):
        pass

    def on_order_changed(self, event: events.Event):
        pass
        # if isinstance(event, events.OrderFilled):
        #     print(
        #         f"MACD Order filled: {event.symbol} {event.side} {event.quantity} @ {event.execution_price}"
        #     )
