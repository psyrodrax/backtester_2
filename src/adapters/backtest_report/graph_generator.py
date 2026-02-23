from src.domain.ports.broker import AbstractBroker
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Iterable, Optional, Dict


class GraphGenerator:
    def __init__(self, broker: AbstractBroker):
        self.broker = broker
        self.initial_cash = float(broker.initial_cash)
        self.start_date = broker.start_date
        self.end_date = broker.end_date
        self.equity_curve = broker.equity_curve
        self._bh_cache: Dict[str, pd.Series] = {}

    def _to_datetime_str(self, dt) -> str:
        # Normalize to tz-naive UTC date string (YYYY-MM-DD)
        ts = pd.Timestamp(dt)
        if ts.tz is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts.strftime("%Y-%m-%d")

    # NEW: helpers to normalize tz handling
    def _as_naive_utc_ts(self, dt) -> pd.Timestamp:
        ts = pd.Timestamp(dt)
        if ts.tz is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return ts

    def _naive_utc_series(self, s: pd.Series) -> pd.Series:
        if isinstance(s.index, pd.DatetimeIndex) and s.index.tz is not None:
            s = s.copy()
            s.index = s.index.tz_convert("UTC").tz_localize(None)
        return s

    def _strategy_equity_series(self) -> pd.Series:
        """
        Convert broker.equity_curve to a pandas Series indexed by date (tz-naive UTC).
        """
        ec = self.equity_curve

        # Build Series 's' from supported inputs
        if isinstance(ec, pd.Series):
            s = ec.copy()
            s.name = "Strategy"
        elif isinstance(ec, pd.DataFrame):
            cols = {c.lower(): c for c in ec.columns}
            time_col = cols.get("timestamp") or cols.get("date")
            val_col = cols.get("equity") or cols.get("value")
            if not time_col or not val_col:
                raise ValueError("equity_curve DataFrame must have timestamp/date and equity/value columns")
            s = ec.set_index(pd.to_datetime(ec[time_col]))[val_col].astype(float)
            s.name = "Strategy"
        elif isinstance(ec, (list, tuple)) and len(ec) > 0:
            first = ec[0]
            if isinstance(first, dict):
                time_key = "timestamp" if "timestamp" in first else "date"
                s = pd.Series(
                    data=[float(d.get("equity", d.get("value", np.nan))) for d in ec],
                    index=pd.to_datetime([d[time_key] for d in ec]),
                    name="Strategy",
                )
            else:
                s = pd.Series(
                    data=[float(v[1]) for v in ec],
                    index=pd.to_datetime([v[0] for v in ec]),
                    name="Strategy",
                )
        else:
            # Fallback: try to coerce
            s = pd.Series(ec, name="Strategy")
            if not isinstance(s.index, pd.DatetimeIndex):
                s.index = pd.to_datetime(s.index)

        # Normalize index to tz-naive UTC to avoid comparison errors
        s = self._naive_utc_series(s)
        s = s.sort_index()
        return s

    def get_buy_and_hold_curve(
        self, ticker: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> pd.Series:
        """
        Returns a Series of portfolio value if all initial_cash was invested in 'ticker'
        on the first available trading day in [start, end], using adjusted close prices.
        """
        key = (
            ticker.upper(),
            start or self._to_datetime_str(self.start_date),
            end or self._to_datetime_str(self.end_date),
        )
        if key in self._bh_cache:
            return self._bh_cache[key].copy()

        s = start or self._to_datetime_str(self.start_date)
        e = end or self._to_datetime_str(self.end_date)

        # Auto-adjust=True => Close is adjusted for splits/dividends
        df = yf.download(
            ticker,
            start=s,
            end=e,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if df is None or df.empty:
            return pd.Series(dtype=float, name=ticker)

        # Safely extract a 1D price series
        prices = df["Close"]
        if isinstance(prices, pd.DataFrame):
            prices = prices.iloc[:, 0]
        prices = prices.astype(float).sort_index()

        # Coerce first_price to a scalar to avoid ambiguous truth values
        first_raw = prices.iloc[0] if len(prices) else np.nan
        try:
            first_price = float(first_raw)
        except Exception:
            first_price = np.nan

        if not np.isfinite(first_price) or first_price <= 0.0:
            return pd.Series(dtype=float, name=ticker)

        shares = float(self.initial_cash) / first_price if self.initial_cash else 0.0
        if shares <= 0.0:
            return pd.Series(dtype=float, name=ticker)

        values = prices * shares
        values.name = ticker.upper()

        self._bh_cache[key] = values
        return values.copy()

    def plot_equity_comparison(
        self,
        tickers: Iterable[str] = ("SPY", "QQQ"),
        title: str = "Equity Curve Comparison",
        outfile: Optional[str] = None,
        normalize: bool = False,
        figsize=(12, 6),
    ) -> Optional[str]:
        strat = self._strategy_equity_series().sort_index()
        if strat.empty:
            return None
        
        for t in self.broker.last_prices.keys():
            if t not in tickers and t != "VXX":
                tickers = list(tickers) + [t]

        # Ensure strat index is tz-naive UTC before slicing (extra guard)
        strat = self._naive_utc_series(strat)

        # Use tz-naive UTC bounds to match series index
        start_ts = self._as_naive_utc_ts(self.start_date)
        end_ts = self._as_naive_utc_ts(self.end_date)

        # Slice without tz mismatch
        strat = strat.loc[(strat.index >= start_ts) & (strat.index <= end_ts)]

        curves = {"Strategy": strat}
        for t in tickers:
            bh = self.get_buy_and_hold_curve(
                t,
                start=self._to_datetime_str(start_ts),
                end=self._to_datetime_str(end_ts),
            )
            if not bh.empty:
                curves[t.upper()] = bh

        df = pd.DataFrame(curves).sort_index().ffill().dropna(how="all")
        if df.empty:
            return None

        if normalize:
            # Normalize each series by its own first valid value, then take log.
            # This prevents missing Strategy curve when the first row has NaNs.
            def first_valid(x: pd.Series):
                idx = x.first_valid_index()
                return x.loc[idx] if idx is not None else np.nan

            base = df.apply(first_valid, axis=0)
            # Drop columns that never have valid data
            valid_cols = base.index[base.notna()]
            df = df[valid_cols].divide(base[valid_cols])
            df = np.log(df.clip(lower=1e-12))

        plt.figure(figsize=figsize, dpi=120)
        for col in df.columns:
            z = 10 if col == "Strategy" else 1
            plt.plot(df.index, df[col], label=col, linewidth=1.8, zorder=z)

        plt.title(title)
        plt.xlabel("Date")
        plt.ylabel("Log Growth (ln(value/initial))" if normalize else "Portfolio Value ($)")
        plt.grid(True, alpha=0.25)
        plt.legend()
        plt.tight_layout()

        if outfile:
            out_path = Path(outfile)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(out_path.as_posix())
            plt.close()
            return out_path.as_posix()
        else:
            plt.show()
            return None

    def generate_comparative_graphs(
        self, tickers: Iterable[str] = ("SPY", "QQQ"), outfile: Optional[str] = None
    ):
        """
        Convenience wrapper that plots Strategy vs. given tickers.
        """
        title = f"Equity Comparison ({self._to_datetime_str(self.start_date)} to {self._to_datetime_str(self.end_date)})"
        return self.plot_equity_comparison(
            tickers=tickers, title=title, outfile=outfile, normalize=False
        )
