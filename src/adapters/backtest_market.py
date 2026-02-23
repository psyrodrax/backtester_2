from datetime import datetime
import os, glob, time
from typing import List, Sequence, Dict
from collections import defaultdict
import pandas as pd
from tqdm.auto import tqdm
import yfinance as yf

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from src.domain import events, commands
from src.domain.ports import MarketDataFeed, SubscribeType, EventBusAdapter


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


class FindatapyBacktestAdapter(EventBusAdapter, MarketDataFeed):
    def __init__(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        data_dir: str = "./data",
        data_source: str = "dukascopy",
        types: Sequence[SubscribeType] = ("QUOTES",),
        append_text: str = "USUSD",
        speed: float = 1.0,
        show_progress: bool = True,
    ):
        """
        Args:
            symbols: List of symbols to subscribe to
            start_date: Start date for the data
            end_date: End date for the data
            data_dir: Directory to store downloaded data
            data_source: Data source to use for downloading data
            types: Types of data to subscribe to
            append_text: Text to append to symbol names
            speed: Replay speed relative to real-time (1.0 = realtime, 10.0 = 10x faster, 0 = no delay)
        """
        super().__init__()
        self.speed = speed
        self.data_dir = data_dir
        self.data_source = data_source
        self.append_text = append_text
        self.types = types
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        # init market + progress flag
        self.market = Market(market_data_generator=MarketDataGenerator())
        self.show_progress = show_progress
        self.loader = DailyQuoteLoader(
            data_root=data_dir,
            tickers=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        self._splits: Dict[str, pd.Series] = {}

    def get_market_holidays(self, ticker="SPY"):
        """
        Finds market holidays by checking for missing trading days in historical data.
        Args:
            ticker (str): The stock ticker to fetch data for (default is SPY).
        Returns:
            list: A list of datetime objects representing market holidays.
        """
        print("Fetching market holidays...")
        # Fetch historical data
        data = yf.download(ticker, start=self.start_date, end=self.end_date)
        # All expected business days in range
        all_days = pd.date_range(self.start_date, self.end_date, freq="B")
        # Actual trading days from data
        trading_days = pd.to_datetime(data.index)
        # Holidays are business days not in trading days
        holidays = sorted(set(all_days) - set(trading_days))
        return holidays

    def download_data(self):
        """
        Download market data for the given symbols and date range, saving one parquet per symbol-day slice.
        Fetches all symbols for each day in a single request, then splits and saves per symbol.
        """
        start_dt = pd.to_datetime(self.start_date)
        end_dt = pd.to_datetime(self.end_date)
        days = pd.date_range(start=start_dt, end=end_dt, freq="D")
        market_holidays = self.get_market_holidays()

        for symbol in self.symbols:
            for day in days:
                if day.dayofweek >= 5:
                    continue

                if day in market_holidays:
                    print("[HOLIDAY] {}".format(day.strftime("%Y-%m-%d")))
                    continue

                date_str = day.strftime("%d %b %Y")
                end_date_str = (day + pd.Timedelta(days=1)).strftime("%d %b %Y")
                file_path = os.path.join(
                    self.data_dir,
                    symbol,
                    str(day.year),
                    str(day.month),
                    f"{symbol}_{date_str}_{self.append_text}_{self.data_source}.parquet",
                )
                if os.path.exists(file_path):
                    print("[CACHE HIT] {}".format(file_path))
                    continue

                print("[DOWNLOAD] {} - {}".format(date_str, end_date_str))
                md_request = MarketDataRequest(
                    start_date=date_str,
                    finish_date=end_date_str,
                    fields=["bid", "ask"],
                    vendor_fields=["bid", "ask"],
                    freq="tick",
                    data_source=self.data_source,
                    tickers=[symbol],
                    vendor_tickers=[symbol + self.append_text],
                )

                df: pd.DataFrame = self.market.fetch_market(md_request)

                if df is None or df.empty:
                    print("[MISSING] {}".format(file_path))
                    continue

                # remove symbol prefix
                df.columns = df.columns.str.replace(f"{symbol}.", "")

                # add symbol column
                df["symbol"] = symbol

                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                print("[CACHE MISS] {}".format(file_path))
                df.to_parquet(file_path)

    def _adjust_for_splits(self, df: pd.DataFrame):
        ticker = df["symbol"].iloc[0]
        splits = self._splits.get(ticker)
        if splits is None:
            splits = yf.Ticker(ticker).splits
            self._splits[ticker] = splits

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        for date, ratio in splits.items():
            split_date = pd.to_datetime(date).date()
            # Adjust prices *before* the split
            df.loc[df["timestamp"].dt.date < split_date, ["bid", "ask"]] /= ratio

        return df

    def _run(self):
        """Replay tick data day by day with minimal per-tick overhead."""
        files_by_date = self.loader.discover_files()
        all_dates = list(files_by_date.keys())

        # Helper context manager for optional progress bars
        class _NullBar:
            def __init__(self, total=0, *args, **kwargs):
                self.total = total

            def __enter__(self):
                return self

            def set_description(self, *args, **kwargs):
                pass

            def update(self, *args, **kwargs):
                pass

            def __exit__(self, exc_type, exc, tb):
                return False

        day_bar_cm = tqdm if self.show_progress else _NullBar
        tick_bar_cm = tqdm if self.show_progress else _NullBar

        handle_event = self.event_bus.handle  # local binding
        speed = self.speed
        sleep = time.sleep
        need_sleep = speed > 0

        # send strategy start event
        self.event_bus.handle(commands.StartStrategyCommand(timestamp=self.start_date))

        with day_bar_cm(
            total=len(all_dates), desc="Backtest days", unit="day"
        ) as day_bar:
            for day in all_dates:
                day_bar.set_description(f"Day {day}")

                # Load only the day's data
                day_frames = []
                for ticker, fpath in files_by_date[day].items():
                    df = pd.read_parquet(fpath)
                    if df.index.name is not None:
                        # Preserve original index as timestamp then drop index to avoid .iterrows overhead
                        df["timestamp"] = df.index
                        df = df.reset_index(drop=True)
                    elif "timestamp" not in df.columns:
                        raise ValueError("No timestamp information found in data.")
                    df["symbol"] = ticker
                    df = self._adjust_for_splits(df)
                    day_frames.append(df)

                if not day_frames:
                    # No ticks for this day, use midnight as timestamp
                    handle_event(
                        events.DayStarted(
                            date=datetime.combine(day, datetime.min.time())
                        )
                    )
                    handle_event(
                        events.DayEnded(date=datetime.combine(day, datetime.max.time()))
                    )
                    day_bar.update(1)
                    continue

                day_df = pd.concat(day_frames, ignore_index=True)
                day_df.sort_values("timestamp", inplace=True)

                # scale prices once (vectorized)
                if {"bid", "ask"}.issubset(day_df.columns):
                    day_df[["bid", "ask"]] = day_df[["bid", "ask"]] / 1000.0

                # Use itertuples for speed (avoid Series creation per row)
                columns_present = set(day_df.columns)

                # Use first and last tick timestamp for day started/ended
                first_tick_ts = pd.to_datetime(day_df["timestamp"].iloc[0])
                last_tick_ts = pd.to_datetime(day_df["timestamp"].iloc[-1])
                second_last_tick_ts = (
                    pd.to_datetime(day_df["timestamp"].iloc[-2])
                    if len(day_df) > 1
                    else first_tick_ts
                )

                if day_df.empty:
                    continue

                # build a one-row "first tick" from the day's first row
                first_row = day_df.iloc[0]

                # send the first tick BEFORE DayStarted
                handle_event(
                    events.QuoteReceived(
                        symbol=first_row.symbol,
                        timestamp=getattr(first_row, "timestamp", 0),
                        bid_price=float(getattr(first_row, "bid", 0.0)),
                        ask_price=float(getattr(first_row, "ask", 0.0)),
                        bid_size=float(getattr(first_row, "bid_size", 0.0)),
                        ask_size=float(getattr(first_row, "ask_size", 0.0)),
                    )
                )

                handle_event(events.DayStarted(date=first_tick_ts))

                with tick_bar_cm(
                    total=len(day_df), desc=f"Ticks {day}", leave=False, unit="tick"
                ) as tick_bar:
                    for row in day_df.itertuples(index=False):
                        handle_event(
                            events.QuoteReceived(
                                symbol=row.symbol,
                                timestamp=getattr(row, "timestamp", 0),
                                bid_price=float(getattr(row, "bid", 0.0)),
                                ask_price=float(getattr(row, "ask", 0.0)),
                                bid_size=float(getattr(row, "bid_size", 0.0)),
                                ask_size=float(getattr(row, "ask_size", 0.0)),
                            )
                        )
                        if need_sleep:
                            sleep(1.0 / speed)
                        tick_bar.update(1)

                        if getattr(row, "timestamp", 0) == second_last_tick_ts:
                            # End of day event after last tick
                            handle_event(events.DayEnded(date=second_last_tick_ts))

                day_bar.update(1)

        # send strategy end event
        self.event_bus.handle(commands.EndStrategyCommand(timestamp=self.end_date))

    def connect(
        self,
        symbols: List[str] = None,
        types: Sequence[SubscribeType] = ("BARS", "QUOTES", "TRADES"),
    ):
        """Satisfy MarketDataFeed interface; optionally override symbols/types.

        Steps:
          1. Optionally override symbols/types (QUOTES only effectively used).
          2. Ensure required data slices cached.
          3. Replay ticks via event bus.
        """
        if symbols:
            self.symbols = symbols
        if types:
            # Only QUOTES stream is produced; ignore others silently.
            self.types = types

        self.download_data()
        if self.show_progress:
            print("=== Running Backtest ===")
        self._run()

    def close(self):
        """No-op for backtest adapter (placeholder for interface)."""
        return None
