# src/adapters/backtest_report.py
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import matplotlib
import numpy as np
import pandas as pd
import yfinance as yf
from matplotlib import pyplot as plt

from src.adapters.backtest_report.excel_generator import ExcelReportGenerator
from src.config import read_backtest_config
from src.domain.ports import AbstractBroker, Strategy
from src.domain.models.position import Position
from typing import List, Dict, Union

matplotlib.use("Agg")

logger = logging.getLogger(__name__)


@dataclass
class BacktestMetrics:
    strategy_name: str
    start_date: datetime
    end_date: datetime
    total_time: timedelta
    initial_cash: float
    final_cash: float
    final_equity: float
    total_fees: float
    equity_curve: pd.DataFrame
    cagr: float
    no_of_drawdowns: int
    max_drawdown: float
    max_drawdown_start: Optional[datetime]
    max_drawdown_end: Optional[datetime]
    peak_equity: float
    trough_equity: float
    average_drawdown: float
    median_drawdown: float
    win_rate: float
    portfolio_return_pct: float
    total_trades: int
    total_wins: int
    total_losses: int
    average_profit: float
    average_loss: float
    expected_value: float
    risk_reward_ratio: float
    risk_free_rate: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    profit_factor: float
    benchmark: List[Dict[str, Union[str, float]]]
    leftover_positions: Dict[str, Position]


class PerformanceReport:
    def __init__(self, strategy: Strategy, broker: AbstractBroker):
        self.strategy = strategy
        self.broker = broker
        self.excel_generator = ExcelReportGenerator()
        self._metrics: BacktestMetrics = None
        self.report_dir = Path("reports", datetime.now().strftime("%Y_%m_%d__%H_%M_%S"))

    def _equity_at_or_nearest(self, target_dt: datetime) -> float:
        """Return equity at target_dt; if no exact match, use nearest timestamp.

        Handles tz-aware/naive mismatches gracefully. Returns 0.0 if curve empty.
        """
        df = self.broker.equity_curve
        if (
            df is None
            or df.empty
            or "date" not in df.columns
            or "equity" not in df.columns
        ):
            return 0.0

        ts = pd.to_datetime(df["date"], errors="coerce")
        # Normalize to tz-naive for comparison
        tzinfo = getattr(ts.dt, "tz", None)
        if tzinfo is not None:
            ts = ts.dt.tz_convert(None)

        tgt = pd.to_datetime(target_dt, errors="coerce")
        if isinstance(tgt, pd.Timestamp) and tgt.tz is not None:
            tgt = tgt.tz_convert(None)

        # Drop any rows where date failed to parse
        valid = ts.notna()
        if not valid.any():
            return 0.0
        ts_valid = ts[valid]
        eq_valid = df.loc[valid, "equity"].reset_index(drop=True)
        # Find nearest by absolute time difference
        try:
            idx = (ts_valid - tgt).abs().argmin()
            val = float(eq_valid.iloc[int(idx)])
            return val
        except (ValueError, TypeError, AttributeError, IndexError):
            # Fallback: last known equity
            try:
                return float(df["equity"].iloc[-1])
            except (ValueError, TypeError, AttributeError, IndexError):
                return 0.0

    @property
    def metrics(self) -> BacktestMetrics:
        if self._metrics is None:
            self._metrics = self.calculate_metrics()
        return self._metrics

    @property
    def cagr(self) -> float:
        """
        Calculate the Compound Annual Growth Rate (CAGR).
        """
        if self.broker.initial_cash <= 0 or self.broker.total_time.days <= 0:
            return 0.0
        ratio = self.final_equity / self.broker.initial_cash
        if ratio <= 0:
            return -1.0  # or 0.0, depending on your preference
        return float(ratio ** (365 / self.broker.total_time.days) - 1)

    def get_risk_free_rate(self) -> float:
        try:
            irx = yf.Ticker("^IRX")
            hist = irx.history(period="5d")
            if not hist.empty:
                latest_yield = hist["Close"].iloc[-1]  # percent
                return latest_yield / 100.0  # convert to decimal
        except (ValueError, KeyError, IndexError, AttributeError, OSError):
            return 0.0

    @property
    def sharpe_ratio(self) -> float:
        """
        Calculate the Sharpe Ratio.
        """
        # step 1: calculate per trade pair returns
        returns = [pair.return_pct for pair in self.broker.trade_pairs]

        # step 2: calculate risk-free rate for the duration of the trades
        rf_annual = self.get_risk_free_rate()
        if self.broker.total_time.days > 0:
            rf = rf_annual * (self.broker.total_time.days / 365)
        else:
            rf = 0.04

        # step 3: Sharpe ratio
        if returns:
            std_return = np.std(returns, ddof=1)

            sharpe = (self.cagr - rf) / std_return if std_return > 0 else 0.0
            return sharpe

        return 0.0

    @property
    def sortino_ratio(self) -> float:
        """
        Calculate the Sortino Ratio.
        """
        # step 1: calculate per trade pair returns
        returns = [pair.return_pct for pair in self.broker.trade_pairs]

        # step 2: calculate risk-free rate for the duration of the trades
        rf_annual = self.get_risk_free_rate()
        if self.broker.total_time.days > 0:
            rf = rf_annual * (self.broker.total_time.days / 365)
        else:
            rf = 0.04

        # step 3: Sortino ratio
        if returns:
            # downside deviation (only negative returns relative to 0)
            downside_returns = [r for r in returns if r < 0]

            if len(downside_returns) > 1:
                downside_std = np.std(downside_returns, ddof=1)
            elif len(downside_returns) == 1:
                downside_std = 0.0  # one negative trade, but no variation
            else:
                downside_std = 0.0  # no downside trades

            # if no downside risk, return large ratio
            if downside_std > 0:
                return (self.cagr - rf) / downside_std
            else:
                return "Not enough data"

        return 0.0

    @property
    def calmar_ratio(self) -> float:
        """
        Calculate the Calmar Ratio.
        """

        max_drawdown = self.max_drawdown[0]
        return self.cagr / abs(max_drawdown)

    @property
    def profit_factor(self) -> float:
        """
        Calculate the Profit Factor.
        """
        total_profit = sum(pair.pnl for pair in self.broker.trade_pairs if pair.pnl > 0)
        total_loss = sum(
            abs(pair.pnl) for pair in self.broker.trade_pairs if pair.pnl < 0
        )

        if total_loss > 0:
            return total_profit / total_loss
        else:
            return float("inf") if total_profit > 0 else 0.0

    @property
    def no_of_drawdowns(self) -> int:
        """
        Count the number of drawdowns (peak-to-trough periods).
        Optionally apply a threshold (e.g., 0.05 = only count drawdowns > 5%).
        """
        if self.broker.equity_curve.empty:
            return 0

        equity = self.broker.equity_curve["equity"].values
        running_max = np.maximum.accumulate(equity)
        drawdowns = (equity - running_max) / running_max

        in_drawdown = False
        num_drawdowns = 0

        for dd in drawdowns:
            if dd < -0.0:  # we are in a drawdown
                if not in_drawdown:
                    num_drawdowns += 1  # new drawdown starts
                    in_drawdown = True
            else:  # recovered
                in_drawdown = False

        return num_drawdowns

    def drawdowns(self, version: int = 1) -> pd.DataFrame:
        """
        Record a drawdown every time equity drops after a growth phase.
        """
        if version not in (1, 2):
            raise ValueError("version must be 1 or 2")

        ec = self.broker.equity_curve
        if (
            ec is None
            or ec.empty
            or "equity" not in ec.columns
            or "date" not in ec.columns
        ):
            return pd.DataFrame()

        ec_copy = ec.copy()
        ec_copy["peak"] = False
        ec_copy["trough"] = False
        is_growing = True

        if version == 1:
            for i in range(1, len(ec_copy)):
                if is_growing:
                    if ec_copy.loc[i, "equity"] < ec_copy.loc[i - 1, "equity"]:
                        is_growing = False
                        ec_copy.loc[i - 1, "peak"] = True
                elif ec_copy.loc[i, "equity"] >= ec_copy.loc[i - 1, "equity"]:
                    is_growing = True
                    ec_copy.loc[i - 1, "trough"] = True

        if version == 2:
            previous_peak_idx = 0

            for i in range(1, len(ec_copy)):
                if (
                    is_growing
                    and ec_copy.loc[i, "equity"] < ec_copy.loc[i - 1, "equity"]
                ):
                    is_growing = False

                    if (
                        ec_copy.loc[i - 1, "equity"]
                        > ec_copy.loc[previous_peak_idx, "equity"]
                    ):
                        previous_peak_idx = i - 1
                        ec_copy.loc[i - 1, "peak"] = True

                elif (
                    not is_growing
                    and ec_copy.loc[i, "equity"] >= ec_copy.loc[i - 1, "equity"]
                ):
                    is_growing = True

            # Mark troughs as the minimum equity point between each pair of consecutive peaks
            peak_indices = ec_copy.index[ec_copy["peak"]].to_list()

            for prev_idx, next_idx in zip(peak_indices[:-1], peak_indices[1:]):
                trough_idx = ec_copy.loc[prev_idx:next_idx, "equity"].idxmin()
                ec_copy.loc[trough_idx, "trough"] = True

        peaks = ec_copy[ec_copy["peak"]]
        troughs = ec_copy[ec_copy["trough"]]

        drawdowns = pd.DataFrame(
            columns=[
                "start_date",
                "end_date",
                "start_equity",
                "end_equity",
                "drawdown_pct",
            ]
        )

        for i in range(len(peaks)):
            peak = peaks.iloc[i]
            subsequent_troughs = troughs[troughs.index > peak.name]
            if not subsequent_troughs.empty:
                trough = subsequent_troughs.iloc[0]
                drawdown_pct = (peak["equity"] - trough["equity"]) / peak["equity"]
                new_row = pd.DataFrame(
                    {
                        "start_date": [peak["date"]],
                        "end_date": [trough["date"]],
                        "start_equity": [peak["equity"]],
                        "end_equity": [trough["equity"]],
                        "drawdown_pct": [drawdown_pct],
                    }
                )

                if drawdowns.empty:
                    drawdowns = new_row
                else:
                    drawdowns = pd.concat([drawdowns, new_row], ignore_index=True)

        return drawdowns

    @property
    def max_drawdown(
        self,
    ) -> Tuple[float, float, float, Optional[datetime], Optional[datetime]]:
        """
        Calculate the maximum drawdown as a percentage, and return peak and trough equity.
        """
        drawdowns = self.drawdowns(version=2)

        if drawdowns.empty:
            return 0.0, 0.0, 0.0, None, None

        max_dd_row = drawdowns.loc[drawdowns["drawdown_pct"].idxmax()]
        max_dd = max_dd_row["drawdown_pct"]
        peak_equity = max_dd_row["start_equity"]
        trough_equity = max_dd_row["end_equity"]
        start_date = max_dd_row["start_date"]
        end_date = max_dd_row["end_date"]

        return max_dd, peak_equity, trough_equity, start_date, end_date

    @property
    def median_drawdown(self) -> float:
        """
        Calculate the median drawdown as a percentage.
        """
        drawdowns = self.drawdowns()
        if drawdowns.empty:
            return 0.0

        median_dd = np.median(drawdowns["drawdown_pct"])
        return median_dd

    @property
    def average_drawdown(self) -> float:
        """
        Calculate the average drawdown as a percentage.
        """
        drawdowns = self.drawdowns()
        if drawdowns.empty:
            return 0.0

        avg_dd = np.mean(drawdowns["drawdown_pct"])
        return avg_dd

    @property
    def winrate(self) -> float:
        """
        Calculate the win rate as a percentage.
        """
        if not self.broker.trade_pairs:
            return 0.0

        wins = sum(1 for pair in self.broker.trade_pairs if pair.return_pct > 0)
        total = len(self.broker.trade_pairs)

        return wins / total if total > 0 else 0.0

    @property
    def portfolio_return_pct(self) -> float:
        """
        Calculate the total portfolio return percentage.
        """
        final_equity = self.final_equity
        if self.broker.initial_cash > 0:
            return (final_equity - self.broker.initial_cash) / self.broker.initial_cash
        else:
            return 0.0

    @property
    def final_equity(self) -> float:
        """
        Calculate the final equity.
        """
        final_equity = (
            self.broker.equity_curve["equity"].iloc[-1]
            if not self.broker.equity_curve.empty
            else 0.0
        )
        return final_equity

    @property
    def total_trades(self) -> int:
        """
        Calculate the total number of closed trades.
        """
        if not self.broker.trade_pairs:
            return 0

        total = len(self.broker.trade_pairs)

        return total

    @property
    def total_wins(self) -> int:
        """
        Calculate the total number of winning trades.
        """
        if not self.broker.trade_pairs:
            return 0

        wins = sum(1 for pair in self.broker.trade_pairs if pair.return_pct > 0)

        return wins

    @property
    def total_losses(self) -> int:
        """
        Calculate the total number of losing trades.
        """
        if not self.broker.trade_pairs:
            return 0

        losses = sum(1 for pair in self.broker.trade_pairs if pair.return_pct < 0)

        return losses

    @property
    def average_profit(self) -> float:
        """
        Calculate the average profit per winning trade.
        """
        if not self.broker.trade_pairs:
            return 0.0

        profits = [
            pair.return_pct for pair in self.broker.trade_pairs if pair.return_pct > 0
        ]
        return np.mean(profits) if profits else 0.0

    @property
    def average_loss(self) -> float:
        """
        Calculate the average loss per losing trade.
        """
        if not self.broker.trade_pairs:
            return 0.0

        losses = [
            pair.return_pct for pair in self.broker.trade_pairs if pair.return_pct < 0
        ]
        return np.mean(losses) if losses else 0.0

    @property
    def expected_value(self) -> float:
        """
        Calculate the expected value of the strategy.
        """
        if not self.broker.trade_pairs:
            return 0.0

        win_rate = self.winrate / 100
        average_win = self.average_profit / 100
        average_loss = self.average_loss / 100

        # Calculate expected value using the formula:
        # EV = (Win Rate * Average Win) - (Loss Rate * Average Loss)
        return (win_rate * average_win) - ((1 - win_rate) * average_loss) * 100

    @property
    def risk_reward_ratio(self) -> float:
        """
        Calculate the risk-reward ratio of the strategy.
        """
        average_profit = self.average_profit
        average_loss = self.average_loss
        if average_loss == 0:
            return "Unavailable"
        return average_profit / abs(average_loss)

    def _build_benchmark(self) -> List[Dict[str, Union[str, float]]]:
        """Compute benchmark growth for selected tickers plus risk-free and strategy.

        Returns a list of dicts: [{ticker: str, growth: float|None}, ...]
        - Includes '^GSPC', 'QQQ', 'TQQQ' and any symbols seen in broker.last_prices.
        - Growth computed as (close_end - open_start) / open_start over backtest window.
        - If a ticker has no price at the start_date but has data later (e.g. VXX),
        use the earliest available Open as the starting price.
        - Adds 'Risk-Free' using get_risk_free_rate and period years.
        - Inserts 'Strategy' as first element using portfolio_return_pct.
        """
        tickers = ["^GSPC", "QQQ", "TQQQ"]
        for ticker in self.broker.last_prices.keys():
            if ticker not in tickers and ticker != "VXX":
                tickers.append(ticker)

        # Download the stock data for the window
        data = yf.download(
            tickers,
            start=self.broker.start_date,
            end=self.broker.end_date,
            progress=False,
            auto_adjust=False,
        )

        results: List[Dict[str, Union[str, float]]] = []
        for ticker in tickers:
            try:
                open_series = data["Open"][ticker]
                close_series = data["Close"][ticker]
            except (KeyError, TypeError):
                # ticker not present in downloaded data
                results.append({"ticker": ticker, "growth": None})
                continue

            # If the entire series is missing or all NaNs, treat as no data
            if open_series.isnull().all() or close_series.isnull().all():
                results.append({"ticker": ticker, "growth": None})
                continue

            # Use the first non-NaN open as the starting price (handles tickers that started later)
            open_valid = open_series.dropna()
            close_valid = close_series.dropna()

            if open_valid.empty or close_valid.empty:
                results.append({"ticker": ticker, "growth": None})
                continue

            open_start = open_valid.iloc[0]
            close_end = close_valid.iloc[-1]

            # protect against division by zero
            if open_start == 0 or np.isnan(open_start) or np.isnan(close_end):
                growth = None
            else:
                growth = (close_end - open_start) / open_start

            # Optional: debug print when we used a later start than requested
            try:
                requested_first_index = open_series.index[0]
                actual_first_index = open_valid.index[0]
                if actual_first_index > requested_first_index:
                    print(
                        f"{ticker}: no price at requested start ({requested_first_index.date()}), "
                        f"using earliest available price on {actual_first_index.date()}"
                    )
            except Exception:
                # ignore index/printing issues
                pass

            results.append({"ticker": ticker, "growth": growth})

        # Rename the first ticker to S&P 500 if present
        if results:
            results[0]["ticker"] = "S&P 500"

        # Risk-free growth over the period
        risk_free_rate = self.get_risk_free_rate()
        start_date = pd.to_datetime(self.broker.start_date)
        end_date = pd.to_datetime(self.broker.end_date)
        period_years = max((end_date - start_date).days / 365.25, 0)
        rf_growth = (1 + risk_free_rate) ** period_years - 1
        results.append({"ticker": "Risk-Free", "growth": rf_growth})

        # Strategy growth
        simulated_returns = self.portfolio_return_pct
        results.insert(0, {"ticker": "Strategy", "growth": simulated_returns})

        # print all
        for res in results:
            print(f"Benchmark: {res['ticker']}, Growth: {res['growth']}")

        return results

    def calculate_metrics(self) -> BacktestMetrics:
        max_drawdown = self.max_drawdown
        benchmark = self._build_benchmark()
        simulated_returns = self.portfolio_return_pct

        return BacktestMetrics(
            strategy_name=self.strategy.name,
            start_date=self.broker.start_date,
            end_date=self.broker.end_date,
            total_time=self.broker.total_time,
            initial_cash=self.broker.initial_cash,
            final_cash=self.broker.available_cash,
            final_equity=self.final_equity,
            total_fees=self.broker.total_fees,
            equity_curve=self.broker.equity_curve,
            cagr=self.cagr,
            no_of_drawdowns=self.no_of_drawdowns,
            max_drawdown=max_drawdown[0],
            max_drawdown_start=max_drawdown[3],
            max_drawdown_end=max_drawdown[4],
            peak_equity=(
                self.broker.equity_curve["equity"].max()
                if not self.broker.equity_curve.empty
                else 0.0
            ),
            trough_equity=(
                self.broker.equity_curve["equity"].min()
                if not self.broker.equity_curve.empty
                else 0.0
            ),
            median_drawdown=self.median_drawdown,
            average_drawdown=self.average_drawdown,
            win_rate=self.winrate,
            portfolio_return_pct=simulated_returns,
            total_trades=self.total_trades,
            total_wins=self.total_wins,
            total_losses=self.total_losses,
            average_profit=self.average_profit,
            average_loss=self.average_loss,
            expected_value=self.expected_value,
            risk_reward_ratio=self.risk_reward_ratio,
            risk_free_rate=self.get_risk_free_rate(),
            sharpe_ratio=self.sharpe_ratio,
            sortino_ratio=self.sortino_ratio,
            calmar_ratio=self.calmar_ratio,
            profit_factor=self.profit_factor,
            benchmark=benchmark,
            leftover_positions=self.broker.positions,
        )

    def print_full_report(self):
        print("=== Backtest Report ===")
        print(f"Start Date: {self.metrics.start_date.isoformat()}")
        print(f"End Date: {self.metrics.end_date.isoformat()}")
        print(f"Total Time (days): {self.metrics.total_time.days}")
        print(f"Initial Cash: {self.metrics.initial_cash}")
        print(f"Final Cash: {self.metrics.final_cash}")
        print(f"Final Equity: {self.metrics.final_equity}")
        print(f"Total Fees: {self.metrics.total_fees}")
        print("=== Statistics ===")
        print(f"Total Return %: {self.metrics.portfolio_return_pct * 100:.2f}%")
        print(f"CAGR: {self.metrics.cagr:.2%}")
        print(f"Sharpe Ratio: {self.metrics.sharpe_ratio:.2f}")
        print(
            f"Max Drawdown: {self.metrics.max_drawdown:.2%} (from {self.metrics.max_drawdown_start} to {self.metrics.max_drawdown_end})"
        )
        print(f"Peak Equity: {self.metrics.peak_equity:.2f}")
        print(f"Trough Equity: {self.metrics.trough_equity:.2f}")
        print(f"Average Drawdown: {self.metrics.average_drawdown:.2%}")
        print(f"Win Rate: {self.metrics.win_rate * 100:.2f}%")
        print("=== Final Positions ===")
        for position in self.broker.positions.values():
            print(
                f"Symbol: {position.symbol}, Quantity: {position.quantity}, Avg Price: {position.avg_price}"
            )

    def save_equity_curve(self):
        """Save the equity curve to a CSV file and generate a non-GUI plot."""
        if self.broker.equity_curve.empty:
            print("No equity curve data available to save.")
            return

        # save equity curve data
        self.broker.equity_curve.to_csv(
            self.report_dir / "equity_curve.csv", index=False
        )

        # generate equity curve plot
        plt.figure(figsize=(12, 6))
        plt.plot(
            self.broker.equity_curve["date"],
            self.broker.equity_curve["equity"],
            label="Equity Curve",
        )
        plt.title("Backtest Equity Curve")
        plt.xlabel("Date")
        plt.ylabel("Equity")
        plt.legend()
        plt.grid()
        plt.savefig(self.report_dir / "equity_curve.png")
        plt.close()

        print(f"Equity curve saved to {self.report_dir}")

    def save_config(self):
        """Save the backtest configuration to a file."""
        config = read_backtest_config()
        config_path = self.report_dir / "backtest_config.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
        print(f"Backtest configuration saved to {config_path}")

    def create_folder(self):
        """Create the report directory if it doesn't exist."""
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(self):
        """Generate a report for the backtest."""
        path = self.excel_generator.generate_report(
            self.broker,
            self.metrics,
            self.broker.filled_orders,
            output_dir=self.report_dir,
        )
        print(f"Excel report written to {path}")

    def save_strategy(self):
        """Save strategy python file."""
        strategy_file = os.path.abspath(sys.modules[self.strategy.__module__].__file__)
        strategy_dest = self.report_dir / "strategy.py"
        shutil.copy(strategy_file, strategy_dest)
        print(f"Strategy code saved to {strategy_dest}")
