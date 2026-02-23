from datetime import timedelta, datetime
from typing import Optional, Dict, List
from src.domain import events

class HeikinAshiAggregator:
    def __init__(self, timeframe: str = "5m"):
        if isinstance(timeframe, str):
            self.timeframe = self._parse_timeframe(timeframe)
        elif isinstance(timeframe, timedelta):
            self.timeframe = timeframe
        else:
            raise TypeError(f"Unsupported timeframe type: {type(timeframe)}")
        self._current_candle: Optional[Dict] = None
        self.last_ha_open = None
        self.last_ha_close = None

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

    def _bucket_start(self, ts: datetime) -> datetime:
        # Robust floor to timeframe boundaries using epoch seconds
        tf_sec = int(self.timeframe.total_seconds())
        sec = int(ts.timestamp())
        start_sec = sec - (sec % tf_sec)
        return datetime.fromtimestamp(start_sec, tz=ts.tzinfo)

    def aggregate_tick(self, event: events.Event) -> Optional[Dict]:
        """
        Called inside strategy.on_tick_changed().
        If enough ticks complete a candle, return a Heikin Ashi candle dict.
        """
        price = None
        if hasattr(event, "price"):
            price = event.price
        elif hasattr(event, "bid_price") and hasattr(event, "ask_price"):
            price = (event.bid_price + event.ask_price) / 2

        if price is None:
            return None

        ts = event.timestamp
        bucket_start = self._bucket_start(ts)  # use robust bucketing

        if self._current_candle is None or bucket_start > self._current_candle["start"]:
            finalized = None
            if self._current_candle:
                finalized = self._finalize_heikin_ashi(self._current_candle)

            self._current_candle = {
                "start": bucket_start,
                "end": bucket_start + self.timeframe,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": float(getattr(event, "quantity", 0) or 0.0),
            }
            return finalized

        # update candle
        self._current_candle["high"] = max(self._current_candle["high"], price)
        self._current_candle["low"] = min(self._current_candle["low"], price)
        self._current_candle["close"] = price
        self._current_candle["volume"] += float(getattr(event, "quantity", 0) or 0.0)

        return None

    def _finalize_heikin_ashi(self, candle: Dict) -> Dict:
        o = candle["open"]
        h = candle["high"]
        l = candle["low"]
        c = candle["close"]

        ha_close = (o + h + l + c) / 4

        if self.last_ha_open is None or self.last_ha_close is None:
            ha_open = (o + c) / 2
        else:
            ha_open = (self.last_ha_open + self.last_ha_close) / 2

        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        self.last_ha_open = ha_open
        self.last_ha_close = ha_close

        return {
            "start": candle["start"],
            "end": candle["end"],
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
        }

    def aggregate_candles(self, candles: List[Dict]) -> Optional[Dict]:
        """
        Aggregates a list of standard OHLC candles into a single Heikin Ashi candle.
        Each candle dict must have: 'open', 'high', 'low', 'close'.
        Returns a dict with Heikin Ashi OHLC.
        """
        if not candles:
            return None

        opens = [c["open"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        closes = [c["close"] for c in candles]

        o = opens[0]
        c = closes[-1]
        h = max(highs)
        l = min(lows)

        ha_close = (o + h + l + c) / 4

        if self.last_ha_open is None or self.last_ha_close is None:
            ha_open = (o + c) / 2
        else:
            ha_open = (self.last_ha_open + self.last_ha_close) / 2

        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        # Update for next aggregation
        self.last_ha_open = ha_open
        self.last_ha_close = ha_close

        return {
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close
        }
