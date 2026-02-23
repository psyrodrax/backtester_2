import glob
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Sequence

import pandas as pd
import yfinance as yf
from curl_cffi import requests
from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from tqdm.auto import tqdm

from src.domain import commands, events
from src.domain.ports import EventBusAdapter, MarketDataFeed, SubscribeType


def _normalize_symbols(input_symbols: Iterable[str]) -> List[str]:
    out = []
    for s in input_symbols:
        if s is None:
            continue
        if not isinstance(s, str):
            # coerce, just in case
            s = str(s)
        # split on comma, semicolon, whitespace-separated tokens
        parts = [p.strip() for p in s.replace(";", ",").split(",")]
        for p in parts:
            if not p:
                continue
            # collapse multiple whitespace within token
            p = " ".join(p.split())
            out.append(p)
    return out


class DailyQuoteLoader:
    def __init__(
        self,
        data_root: str,
        tickers: list[str],
        start_date: datetime,
        end_date: datetime,
    ):
        self.data_root = data_root
        self.tickers = tickers
        self.start_date = start_date.date()
        self.end_date = end_date.date()

    def discover_files(self):
        """Collect parquet files grouped by date across tickers."""
        files_by_date = defaultdict(dict)

        for ticker in self.tickers:
            pattern = os.path.join(
                self.data_root, ticker, "*", "*", f"{ticker}_*.parquet"
            )
            files = glob.glob(pattern)

            for f in files:
                date = self._extract_date_from_filename(f).date()
                if self.start_date <= date <= self.end_date:
                    files_by_date[date][ticker] = f

        # return dates in chronological order
        return dict(sorted(files_by_date.items()))

    @staticmethod
    def _extract_date_from_filename(path: str) -> datetime:
        # Example filename: AAPL_27 Dec 2024_USUSD_dukascopy.parquet
        base = os.path.basename(path)
        parts = base.split("_")

        date_portion = parts[1]

        return datetime.strptime(date_portion, "%d %b %Y")


class YfinBacktestAdapter(EventBusAdapter, MarketDataFeed):
    def __init__(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        types: Sequence[SubscribeType] = ("QUOTES",),
        speed: float = 1.0,
        show_progress: bool = True,
    ):
        """
        Adapter that uses yfinance daily OHLC to produce 4 synthetic ticks per trading day:
        Open, High, Low, Close (timestamp = the date, time component ignored).
        """
        super().__init__()
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date).date()
        self.end_date = pd.to_datetime(end_date).date()
        self.types = types
        self.speed = speed
        self.show_progress = show_progress
        self.daily_data: Dict[str, pd.DataFrame] = (
            {}
        )  # symbol -> DataFrame with Open/High/Low/Close

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only OHLC, normalize index to date (no time), drop rows with all NaNs."""
        if df is None or df.empty:
            return pd.DataFrame()
        # Ensure datetime index and normalize to midnight (drop time)
        df = df.copy()
        df.index = pd.to_datetime(df.index).normalize()
        # Only keep required columns if present
        required = ["Open", "High", "Low", "Close"]
        available = [c for c in required if c in df.columns]
        df = df[available]
        # Drop rows where all OHLC are NaN
        df = df.dropna(how="all")
        return df

    def download_data(self):
        """Download daily OHLC for all symbols using yf.download. Do not save to disk."""
        start_str = self.start_date.isoformat()
        # yfinance 'end' is exclusive in many uses; to be safe add one day
        end_exclusive = (
            (pd.to_datetime(self.end_date) + pd.Timedelta(days=1)).date().isoformat()
        )

        print(
            f"[YFIN] downloading {len(self.symbols)} symbols from {start_str} to {end_exclusive} (exclusive)"
        )
        data = None
        try:
            # Try a bulk download first (returns MultiIndex columns for >1 symbol)
            data = yf.download(
                tickers=self.symbols,
                start=start_str,
                end=end_exclusive,
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
        except Exception as e:
            print(
                f"[YFIN] bulk download failed: {e}. Will attempt per-symbol download."
            )

        if isinstance(data, pd.DataFrame) and not data.empty:
            # If multiple symbols, columns will be MultiIndex (symbol, field)
            if data.columns.nlevels == 2:
                for symbol in self.symbols:
                    if symbol in data.columns.levels[0]:
                        df_sym = data[symbol]
                        df_sym = self._normalize_df(df_sym)
                        if not df_sym.empty:
                            self.daily_data[symbol] = df_sym.reindex(
                                pd.date_range(
                                    start=self.start_date, end=self.end_date, freq="D"
                                ).normalize(),
                                method=None,
                            ).dropna(how="all")
                        else:
                            print(f"[YFIN] no OHLC for {symbol} in bulk data.")
                    else:
                        print(f"[YFIN] symbol {symbol} missing from bulk payload.")
            else:
                # Single symbol (bulk returned single-level columns)
                # We must identify which symbol this corresponds to (user probably passed single symbol)
                single_df = data
                single_df = self._normalize_df(single_df)
                if len(self.symbols) == 1:
                    self.daily_data[self.symbols[0]] = single_df
                else:
                    # Ambiguous: try to assign to each symbol by checking presence of 'Open' etc.
                    for symbol in self.symbols:
                        # try per-symbol fallback
                        print(
                            f"[YFIN] bulk returned flat columns; attempting per-symbol download for {symbol}"
                        )
                        self._download_single_symbol(symbol, start_str, end_exclusive)
        else:
            # Bulk failed or empty — try per-symbol downloads
            for symbol in self.symbols:
                self._download_single_symbol(symbol, start_str, end_exclusive)

        # Print summary
        for s in self.symbols:
            if s in self.daily_data:
                print(f"[YFIN] cached {len(self.daily_data[s])} rows for {s}")
            else:
                print(f"[YFIN] no data for {s}")

    def _download_single_symbol(self, symbol: str, start_str: str, end_exclusive: str):
        """Download a single symbol safely and store normalized OHLC in daily_data if found."""
        try:
            df = yf.download(
                tickers=symbol,
                start=start_str,
                end=end_exclusive,
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
        except Exception as e:
            print(f"[YFIN] failed to download {symbol}: {e}")
            return

        df = self._normalize_df(df)
        if df.empty:
            # final fallback: try yf.Ticker.history with try/except but often triggers info fetch (may fail)
            try:
                session = requests.Session(impersonate="chrome")
                t = yf.Ticker(symbol, session=session)
                df2 = t.history(
                    start=start_str, end=end_exclusive, interval="1d", auto_adjust=True
                )
                df2 = self._normalize_df(df2)
                if not df2.empty:
                    self.daily_data[symbol] = df2
                    return
            except Exception as e2:
                print(f"[YFIN] fallback history() failed for {symbol}: {e2}")

            print(f"[YFIN] no OHLC data for {symbol}")
            return

        self.daily_data[symbol] = df

    def _run(self):
        """Replay daily OHLC as 4 QuoteReceived events per symbol/day (Open, High, Low, Close)."""
        # Build union of all available dates across symbols and limit to the requested date range
        all_dates = pd.DatetimeIndex([])
        for df in self.daily_data.values():
            all_dates = all_dates.union(df.index)
        # limit to start/end range
        if all_dates.empty:
            print("[YFIN] no trading dates found in downloaded data.")
            # Still emit start/end strategy commands
            self.event_bus.handle(
                commands.StartStrategyCommand(timestamp=self.start_date)
            )
            self.event_bus.handle(commands.EndStrategyCommand(timestamp=self.end_date))
            return

        # Keep only dates within requested bounding box
        all_dates = all_dates[
            (all_dates.date >= self.start_date) & (all_dates.date <= self.end_date)
        ]
        all_dates = sorted(set(all_dates))

        # Prepare replay helpers
        handle_event = self.event_bus.handle
        sleep = time.sleep
        need_sleep = self.speed > 0

        class _NullBar:
            def __init__(self, total=0, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def set_description(self, *args, **kwargs):
                pass

            def update(self, *args, **kwargs):
                pass

            def __exit__(self, exc_type, exc, tb):
                return False

        day_bar_cm = tqdm if self.show_progress else _NullBar

        # Strategy start
        handle_event(commands.StartStrategyCommand(timestamp=self.start_date))

        with day_bar_cm(
            total=len(all_dates), desc="Backtest days", unit="day"
        ) as day_bar:
            for day_ts in all_dates:
                # convert to pure date-timestamp (time component not important)
                day_date = pd.to_datetime(day_ts).normalize()
                day_bar.set_description(f"Day {day_date.date()}")

                # Fire first tick
                for symbol in self.symbols:
                    df = self.daily_data.get(symbol)
                    if df is None:
                        continue
                    if day_date not in df.index:
                        continue

                    row = df.loc[day_date]
                    # pick the first available OHLC field to use for the "first tick"
                    first_field = next(
                        (
                            k
                            for k in ["Open", "High", "Low", "Close"]
                            if k in row.index and not pd.isna(row[k])
                        ),
                        None,
                    )
                    if first_field is not None:
                        price = float(row[first_field])
                        handle_event(
                            events.QuoteReceived(
                                symbol=symbol,
                                timestamp=day_date.to_pydatetime(),
                                bid_price=price,
                                ask_price=price,
                                bid_size=0.0,
                                ask_size=0.0,
                            )
                        )
                        if need_sleep:
                            sleep(1.0 / self.speed)

                # Fire DayStarted (use datetime)
                handle_event(events.DayStarted(date=day_date.to_pydatetime()))

                # For every symbol that has OHLC on that date, emit four QuoteReceived events
                for symbol in self.symbols:
                    df = self.daily_data.get(symbol)
                    if df is None:
                        continue
                    if day_date not in df.index:
                        continue

                    row = df.loc[day_date]
                    # In case the df has only subset of OHLC columns, iterate keys present
                    # We ensure Open,High,Low,Close order if present
                    ordered_keys = [
                        k for k in ["Open", "High", "Low", "Close"] if k in row.index
                    ]
                    for field in ordered_keys:
                        price = row[field]
                        if pd.isna(price):
                            continue
                        handle_event(
                            events.QuoteReceived(
                                symbol=symbol,
                                timestamp=day_date.to_pydatetime()
                                + timedelta(seconds=1),
                                bid_price=float(price),
                                ask_price=float(price),
                                bid_size=0.0,
                                ask_size=0.0,
                            )
                        )
                        if need_sleep:
                            sleep(1.0 / self.speed)

                # Fire DayEnded
                handle_event(events.DayEnded(date=day_date.to_pydatetime()))

                if "Close" in row.index and not pd.isna(row["Close"]):
                    price = float(row["Close"])
                    handle_event(
                        events.QuoteReceived(
                            symbol=symbol,
                            timestamp=day_date.to_pydatetime() + timedelta(seconds=2),
                            bid_price=price,
                            ask_price=price,
                            bid_size=0.0,
                            ask_size=0.0,
                        )
                    )
                day_bar.update(1)

        # Strategy end
        handle_event(commands.EndStrategyCommand(timestamp=self.end_date))

    def connect(
        self,
        symbols: List[str] = None,
        types: Sequence[SubscribeType] = ("BARS", "QUOTES", "TRADES"),
    ):
        """Entry point to fetch data and start replaying events."""
        if symbols:
            self.symbols = symbols
        if types:
            self.types = types

        self.download_data()
        if self.show_progress:
            print("=== Running Backtest ===")
        self._run()

    def close(self):
        """No-op for backtest adapter."""
        return None
