import pandas as pd
import numpy as np

class ChandelierExit:
    def __init__(self, period: int = 22, mult: float = 3.0, use_close: bool = True):
        """
        Chandelier Exit indicator.
        """
        self.period = int(period)
        self.mult = float(mult)
        self.use_close = bool(use_close)

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        high, low, close = df["high"], df["low"], df["close"]
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(self.period, min_periods=self.period).mean() * self.mult

        if self.use_close:
            highest_close = close.rolling(self.period, min_periods=self.period).max()
            long_stop = highest_close - atr
        else:
            highest_high = high.rolling(self.period, min_periods=self.period).max()
            long_stop = highest_high - atr

        long_stop_prev = long_stop.shift(1)
        long_stop = np.where(close.shift(1) > long_stop_prev,
                             np.maximum(long_stop, long_stop_prev),
                             long_stop)
        long_stop = pd.Series(long_stop, index=df.index)

        if self.use_close:
            lowest_close = close.rolling(self.period, min_periods=self.period).min()
            short_stop = lowest_close + atr
        else:
            lowest_low = low.rolling(self.period, min_periods=self.period).min()
            short_stop = lowest_low + atr

        short_stop_prev = short_stop.shift(1)
        short_stop = np.where(close.shift(1) < short_stop_prev,
                              np.minimum(short_stop, short_stop_prev),
                              short_stop)
        short_stop = pd.Series(short_stop, index=df.index)

        dir_ = np.where(close > short_stop_prev, 1,
                        np.where(close < long_stop_prev, -1, np.nan))
        dir_ = pd.Series(dir_, index=df.index).ffill().fillna(0).astype(int)

        buy_signal = (dir_ == 1) & (dir_.shift(1) == -1)
        sell_signal = (dir_ == -1) & (dir_.shift(1) == 1)

        return pd.DataFrame({
            "long_stop": long_stop,
            "short_stop": short_stop,
            "dir": dir_,
            "buy_signal": buy_signal,
            "sell_signal": sell_signal
        }, index=df.index)
