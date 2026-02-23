from datetime import timedelta
from typing import Dict, Optional
from src.domain import events


class CandlestickAggregator:
    def __init__(self, timeframe: str = "1m"):
        if isinstance(timeframe, str):
            self.timeframe = self._parse_timeframe(timeframe)
        elif isinstance(timeframe, timedelta):
            self.timeframe = timeframe
        else:
            raise TypeError(f"Unsupported timeframe type: {type(timeframe)}")
        self._current_candle: Optional[Dict] = None

    def _parse_timeframe(self, timeframe: str) -> timedelta:
        if timeframe.endswith("m"):
            return timedelta(minutes=int(timeframe[:-1]))
        if timeframe.endswith("s"):
            return timedelta(seconds=int(timeframe[:-1]))
        if timeframe.endswith("h"):
            return timedelta(hours=int(timeframe[:-1]))
        if timeframe.endswith("d"):
            return timedelta(days=int(timeframe[:-1]))
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def aggregate_tick(self, event: events.Event) -> Optional[events.CandleReceived]:
        """
        Called inside strategy.on_tick_changed().
        If enough ticks complete a candle, return a CandleReceived event.
        """
        price = None
        if hasattr(event, "price"):
            price = event.price
        elif hasattr(event, "bid_price") and hasattr(event, "ask_price"):
            price = (event.bid_price + event.ask_price) / 2

        if price is None:
            return None

        ts = event.timestamp
        bucket_start = ts - timedelta(
            seconds=ts.second % self.timeframe.seconds,
            microseconds=ts.microsecond,
        )

        if self._current_candle is None or bucket_start > self._current_candle["start"]:
            finalized = None
            if self._current_candle:
                finalized = events.CandleReceived(
                    symbol=event.symbol,
                    timestamp=self._current_candle["end"],
                    open=self._current_candle["open"],
                    high=self._current_candle["high"],
                    low=self._current_candle["low"],
                    close=self._current_candle["close"],
                    volume=self._current_candle["volume"],
                )

            self._current_candle = {
                "start": bucket_start,
                "end": bucket_start + self.timeframe,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": getattr(event, "quantity", 0),
            }
            return finalized

        # update candle
        self._current_candle["high"] = max(self._current_candle["high"], price)
        self._current_candle["low"] = min(self._current_candle["low"], price)
        self._current_candle["close"] = price
        self._current_candle["volume"] += getattr(event, "quantity", 0)

        return
