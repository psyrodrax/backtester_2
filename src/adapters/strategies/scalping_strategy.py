from collections import deque
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd

from src.analysis.candlesticks import HeikinAshiAggregator
from src.analysis.indicators import ZLSMA, ChandelierExit
from src.domain import commands, events
from src.domain.models import OrderSide, OrderType
from src.domain.ports import AbstractBroker, EventBusAdapter, Strategy


class ScalpingStrategy(EventBusAdapter, Strategy):
    def __init__(
        self,
        broker: AbstractBroker,
    ):
        super().__init__(broker=broker)

        self.heikin_ashi_aggregator = HeikinAshiAggregator(
            timeframe=timedelta(minutes=5)
        )
        self.zlsma = ZLSMA(length=150)
        self.chandelier_exit = ChandelierExit(period=22, mult=2.0)

        # Rolling buffers (avoid full DataFrame work)
        self._closes = deque(maxlen=2048)
        self._highs = deque(maxlen=2048)
        self._lows = deque(maxlen=2048)

        self.position = None  # "long", "short", or None
        self.stop_loss = None

    def on_start(self, cmd: commands.StartStrategyCommand):
        print("\nScalping strategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("\nScalping strategy ended.")

    def on_day_start(self, event: events.DayStarted):
        pass

    def on_day_end(self, event: events.DayEnded):
        pass

    def on_tick_changed(self, event: events.Event):
        # Aggregate tick to timeframe candle
        candle = self.heikin_ashi_aggregator.aggregate_tick(event)
        if not candle:
            return

        # Append to rolling arrays
        self._closes.append(float(candle["close"]))
        self._highs.append(float(candle["high"]))
        self._lows.append(float(candle["low"]))

        need_z = self.zlsma.length * 2
        need_ce = self.chandelier_exit.period + 2
        need_sw = 10

        if len(self._closes) < max(need_z, need_sw):
            return

        # ZLSMA on the last 2*length closes (vectorized)
        closes_np = np.fromiter(self._closes, dtype=float)
        closes_np = closes_np[-need_z:]
        zlsma_series = self.zlsma(pd.Series(closes_np)).dropna()
        if zlsma_series.empty:
            return
        zlsma_val = float(zlsma_series.iloc[-1])
        price = float(closes_np[-1])

        # Swing levels (last 10 candles)
        highs_np = np.fromiter(self._highs, dtype=float)[-need_sw:]
        lows_np = np.fromiter(self._lows, dtype=float)[-need_sw:]
        swing_high = float(np.max(highs_np))
        swing_low = float(np.min(lows_np))

        # Exit logic first (no CE needed)
        if self.position == "long" and price < zlsma_val:
            pos = self.broker.positions.get(event.symbol)
            qty = int(abs(pos.quantity)) if pos and pos.quantity else 0
            if qty > 0:
                command = commands.PlaceOrderCommand(
                    symbol=event.symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=qty,
                    timestamp=event.timestamp,
                )
                self.event_bus.handle(command)
            return

        if self.position == "short" and price > zlsma_val:
            pos = self.broker.positions.get(event.symbol)
            qty = int(abs(pos.quantity)) if pos and pos.quantity else 0
            if qty > 0:
                command = commands.PlaceOrderCommand(
                    symbol=event.symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=qty,
                    timestamp=event.timestamp,
                )
                self.event_bus.handle(command)
            return

        # Entry logic (compute CE only if MA precondition holds)
        if self.position is None:
            pre_long = price > zlsma_val
            pre_short = price < zlsma_val
            if not pre_long and not pre_short:
                return

            if len(self._closes) < max(need_ce, need_z):
                return

            highs_ce = np.fromiter(self._highs, dtype=float)[-need_ce:]
            lows_ce = np.fromiter(self._lows, dtype=float)[-need_ce:]
            closes_ce = np.fromiter(self._closes, dtype=float)[-need_ce:]
            df_ce = pd.DataFrame({"high": highs_ce, "low": lows_ce, "close": closes_ce})

            ce = self.chandelier_exit(df_ce)
            if ce.empty:
                return
            buy_signal = bool(ce["buy_signal"].iloc[-1])
            sell_signal = bool(ce["sell_signal"].iloc[-1])

            # Position sizing with buffer to avoid cash=0
            cash_to_use = max(self.broker.available_cash * 0.3, 0.0)
            qty = int(cash_to_use // price)
            if qty <= 0:
                return

            if pre_long and buy_signal:
                command = commands.PlaceOrderCommand(
                    symbol=event.symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=qty,
                    stop_loss=swing_low,
                    timestamp=event.timestamp,
                )
                self.event_bus.handle(command)
                return

            if pre_short and sell_signal:
                command = commands.PlaceOrderCommand(
                    symbol=event.symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.MARKET,
                    quantity=qty,
                    stop_loss=swing_high,
                    timestamp=event.timestamp,
                )
                self.event_bus.handle(command)
                return

    def on_order_changed(self, event: events.Event):
        # Update state only when orders are actually filled
        if isinstance(event, events.OrderFilled):
            order = event.order
            if order.stop_loss is None and order.order_type == OrderType.MARKET:
                # exit fill
                self.position = None
                self.stop_loss = None
            else:
                # entry fill
                self.position = "long" if order.side == OrderSide.BUY else "short"
                self.stop_loss = order.stop_loss
