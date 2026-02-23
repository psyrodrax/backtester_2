import pandas as pd
import numpy as np

class ZLSMA:
    """
    Zero Lag Least Squares Moving Average (ZLSMA) indicator.
    Vectorized rolling linear regression for speed.
    """

    def __init__(self, length: int = 32, offset: int = 0):
        self.length = int(length)
        self.offset = int(offset)

    def _linreg_fast(self, s: pd.Series) -> pd.Series:
        n = self.length
        x = s.to_numpy(dtype=float)
        N = len(x)
        out = np.full(N, np.nan, dtype=float)
        if N < n:
            return pd.Series(out, index=s.index)

        i = np.arange(n, dtype=float)
        Si = i.sum()
        Sii = (i * i).sum()
        denom = n * Sii - Si * Si  # constant for fixed window

        ones = np.ones(n, dtype=float)
        Sx = np.convolve(x, ones, mode="valid")
        Six = np.convolve(x, i, mode="valid")

        m = (n * Six - Si * Sx) / denom
        b = (Sx - m * Si) / n
        y_end = m * (n - 1 + self.offset) + b  # predict at last index of each window

        out[n - 1 :] = y_end
        return pd.Series(out, index=s.index)

    def __call__(self, src: pd.Series) -> pd.Series:
        # First pass
        lsma = self._linreg_fast(src)
        # Fill so second pass has enough values
        lsma_filled = lsma.ffill()
        # Second pass (zero-lag correction)
        lsma2 = self._linreg_fast(lsma_filled)
        return lsma + (lsma - lsma2)
