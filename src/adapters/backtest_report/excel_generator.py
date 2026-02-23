from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Iterable, Optional

import pandas as pd
import xlsxwriter  # relies on installed dependency

from src.domain.models.order import Order
from src.adapters.backtest_broker import BacktestBroker
from src.adapters.backtest_report.graph_generator import GraphGenerator

if TYPE_CHECKING:
    from src.adapters.backtest_report.backtest_report import BacktestMetrics


class ExcelReportGenerator:
    def _comments_to_bullets_for_date(self, comments, day_dt: datetime) -> str:
        """Return a bullet-list string of comments for the given calendar day.

        Supports:
        - pandas.DataFrame with a datetime column (date/timestamp) and a text column (comment/comments/text/note)
        - dict[datetime|date|str -> str|List[str]] (legacy)
        - anything else -> ""
        """

        # Helper to bullet-join a list of strings
        def _join(items):
            items = [str(x).strip() for x in items if x is not None and str(x).strip()]
            return ("• " + "\n• ".join(items)) if items else ""

        # DataFrame path
        if isinstance(comments, pd.DataFrame):
            df = comments
            cols = {c.lower(): c for c in df.columns}
            time_col = (
                cols.get("timestamp")
                or cols.get("date")
                or cols.get("datetime")
                or cols.get("time")
            )
            text_col = (
                cols.get("comment")
                or cols.get("comments")
                or cols.get("text")
                or cols.get("note")
            )
            if not time_col or not text_col:
                return ""
            ts = pd.to_datetime(df[time_col], errors="coerce")
            tzinfo = getattr(ts.dt, "tz", None)
            if tzinfo is not None:
                ts = ts.dt.tz_convert(None)
            mask = ts.dt.date == day_dt.date()
            vals = df.loc[mask, text_col]
            # Flatten possible list-like entries
            items = []
            for v in vals.tolist():
                if isinstance(v, (list, tuple)):
                    items.extend(list(v))
                else:
                    items.append(v)
            return _join(items)

        # Dict path (legacy)
        if isinstance(comments, dict):
            items = []
            # optional pandas for parsing keys
            for k, v in comments.items():
                k_date = None
                if hasattr(k, "date"):
                    try:
                        k_date = k.date()
                    except (AttributeError, ValueError, TypeError):
                        k_date = None
                if k_date is None:
                    try:
                        if pd is not None:
                            k_date = pd.to_datetime(k, errors="coerce").date()
                    except (ValueError, TypeError):
                        k_date = None
                if k_date == day_dt.date():
                    if isinstance(v, (list, tuple)):
                        items.extend(v)
                    elif v is not None:
                        items.append(v)
            return _join(items)

        return ""

    def _prepare_daily_equity(self, metrics: "BacktestMetrics") -> pd.DataFrame:
        """Return one row per day with start/end equity and daily return.

        Assumes metrics.equity_curve can have intraday rows. We collapse to daily
        using the LAST equity value of each calendar day as that day's close.
        Start equity for a day is prior day's closing equity (first day = its own close).
        daily_return_pct is computed off those closes.
        """
        curve = metrics.equity_curve.copy()
        if "date" not in curve.columns:
            raise ValueError("Equity curve must contain 'date' column")

        # Normalize to tz-naive UTC then drop tz for Excel; ensure sorted
        curve["date"] = pd.to_datetime(curve["date"], utc=True).dt.tz_convert(None)
        curve = curve.sort_values("date").reset_index(drop=True)

        # Group by calendar day and take last equity as the daily close
        curve["_day"] = curve["date"].dt.date
        daily_close = (
            curve.groupby("_day")["equity"]
            .last()
            .reset_index()
            .rename(columns={"_day": "date", "equity": "equity"})
        )

        # Convert date (python date) back to midnight datetime for consistent handling downstream
        daily_close["date"] = pd.to_datetime(daily_close["date"])

        # Start equity is prior day's close; first day start == end
        daily_close["start_equity"] = (
            daily_close["equity"].shift(1).fillna(daily_close["equity"])
        )
        daily_close["daily_return_pct"] = (
            daily_close["equity"] / daily_close["start_equity"] - 1.0
        ) * 100.0

        return daily_close[["date", "equity", "start_equity", "daily_return_pct"]]

    def _orders_by_day(self, orders: List[Order]) -> Dict[date, List[Order]]:
        by_day: Dict[date, List[Order]] = {}
        for o in orders:
            # prefer fill timestamp (completion) else placed_at
            ts = o.filled_at or o.placed_at
            if ts is None:
                continue
            # ensure naive datetime for Excel
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.replace(tzinfo=None)
            d = ts.date()
            by_day.setdefault(d, []).append(o)
        return by_day

    def _generate_equity_comparison_charts(
        self,
        broker: BacktestBroker,
        output_dir: Path,
        tickers: Iterable[str] = ("SPY", "QQQ", "TQQQ"),
        timestamp: Optional[str] = None,
    ) -> List[str]:
        """
        Generate comparison charts using GraphGenerator and return image paths.
        Creates two images: absolute dollars and normalized (start=1.0).
        Returns empty list if generation fails (e.g., no network).
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        gg = GraphGenerator(broker)

        paths: List[str] = []

        abs_png = out_dir / f"equity_compare_abs_{ts}.png"
        p1 = gg.plot_equity_comparison(
            tickers=tickers,
            title="Equity Curve (Absolute $)",
            outfile=abs_png.as_posix(),
            normalize=False,
        )
        if p1:
            paths.append(p1)

        norm_png = out_dir / f"equity_compare_norm_{ts}.png"
        p2 = gg.plot_equity_comparison(
            tickers=tickers,
            title="Equity Curve (Log Normalized)",
            outfile=norm_png.as_posix(),
            normalize=True,
        )
        if p2:
            paths.append(p2)

        return paths

    def _insert_charts_on_contents(
        self,
        ws: xlsxwriter.Worksheet,
        chart_paths: Optional[List[str]],
        start_col: int = 6,  # place images starting at column G (0-based index)
        start_row: int = 0,
        x_scale: float = 0.9,
        y_scale: float = 0.9,
        row_spacing: int = 28,
    ) -> None:
        """
        Insert chart images on the right side of the Contents sheet.
        start_col=6 -> column G. Adjust if you need more or less space for links.
        """
        if not chart_paths:
            return
        # Optional: give some width to the image area
        ws.set_column(start_col, start_col + 4, 18)

        r = start_row
        for p in chart_paths:
            try:
                ws.insert_image(
                    r, start_col, p, {"x_scale": x_scale, "y_scale": y_scale}
                )
                r += row_spacing
            except (FileNotFoundError, xlsxwriter.exceptions.XlsxWriterException):
                # Ignore missing files or insertion issues
                continue

    def _write_summary_sheet(
        self,
        wb: xlsxwriter.Workbook,
        daily: pd.DataFrame,
        orders_by_day: Dict[date, List[Order]],
        broker: BacktestBroker,  # expects broker.comments to be Dict[datetime, Union[str, List[str]]]
    ):
        ws = wb.add_worksheet("Summary")
        header_fmt = wb.add_format({"bold": True})
        pct_fmt = wb.add_format({"num_format": "0.00%"})
        num_fmt = wb.add_format({"num_format": "0.00"})
        comment_fmt = wb.add_format({"text_wrap": True, "valign": "top"})

        headers = [
            "Date",
            "Day",
            "Start Equity",
            "End Equity",
            "Daily Return %",
            "Orders",
            "Comments",
        ]
        # Navigation row (row 0) single backlink
        ws.write_url(0, 0, "internal:'Contents'!A1", string="Contents")
        # comments are handled per-day using _comments_to_bullets_for_date
        # Header row (row 1)
        for col, h in enumerate(headers):
            ws.write(1, col, h, header_fmt)

        # --- Extend daily with comment-only days (no equity data) ---
        original_daily_dates = (
            set(daily["date"].dt.date.tolist()) if not daily.empty else set()
        )
        comment_src = broker.comments
        comment_dates: set[date] = set()
        if comment_src is not None:
            dfc = comment_src
            cols = {c.lower(): c for c in dfc.columns}
            time_col = (
                cols.get("timestamp")
                or cols.get("date")
                or cols.get("datetime")
                or cols.get("time")
            )
            if time_col:
                ts = pd.to_datetime(dfc[time_col], errors="coerce")
                tzinfo = getattr(ts.dt, "tz", None)
                if tzinfo is not None:
                    ts = ts.dt.tz_convert(None)
                comment_dates = {t.date() for t in ts.dropna().tolist()}

        missing_comment_dates = sorted(comment_dates - original_daily_dates)
        if missing_comment_dates:
            add_rows = pd.DataFrame(
                {
                    "date": [pd.to_datetime(d) for d in missing_comment_dates],
                    "equity": [pd.NA] * len(missing_comment_dates),
                    "start_equity": [pd.NA] * len(missing_comment_dates),
                    "daily_return_pct": [pd.NA] * len(missing_comment_dates),
                }
            )
            daily = (
                pd.concat([daily, add_rows], ignore_index=True)
                .sort_values("date")
                .reset_index(drop=True)
            )

        # Iterate combined daily (including comment-only days)
        for row_idx, row in enumerate(daily.itertuples(index=False), start=2):
            d: datetime = row.date
            # Only hyperlink if we have an orders sheet (i.e., orders exist for that day)
            if d.date() in orders_by_day:
                sheet_name = d.strftime("%Y-%m-%d")
                link = f"internal:'{sheet_name}'!A1"
                ws.write_url(row_idx, 0, link, string=d.strftime("%Y-%m-%d"))
            else:
                ws.write(row_idx, 0, d.strftime("%Y-%m-%d"))
            ws.write(row_idx, 1, d.strftime("%a"))

            # Numeric columns (may be NA for comment-only days)
            if pd.notna(row.start_equity):
                ws.write_number(row_idx, 2, float(row.start_equity), num_fmt)
            else:
                ws.write(row_idx, 2, "-")
            if pd.notna(row.equity):
                ws.write_number(row_idx, 3, float(row.equity), num_fmt)
            else:
                ws.write(row_idx, 3, "-")
            if pd.notna(row.daily_return_pct):
                ws.write_number(
                    row_idx, 4, float(row.daily_return_pct) / 100.0, pct_fmt
                )
            else:
                ws.write(row_idx, 4, "-")
            ws.write_number(row_idx, 5, len(orders_by_day.get(d.date(), [])))

            # Comments: aggregate for the day
            comment_text = self._comments_to_bullets_for_date(comment_src, d)
            ws.write(row_idx, 6, comment_text, comment_fmt)
            if comment_text:
                lines = comment_text.count("\n") + 1
                ws.set_row(row_idx, min(15 * lines, 200))

        ws.autofilter(1, 0, len(daily) + 1, len(headers) - 1)
        ws.freeze_panes(2, 0)
        ws.set_column(0, 0, 12)
        ws.set_column(1, 1, 6)
        ws.set_column(2, 4, 14)
        ws.set_column(5, 5, 8)
        ws.set_column(6, 6, 50)

    def _write_orders_sheet(
        self, wb: xlsxwriter.Workbook, day: date, orders: List[Order]
    ):
        name = day.strftime("%Y-%m-%d")
        ws = wb.add_worksheet(name[:31])  # Excel sheet name limit
        header_fmt = wb.add_format({"bold": True})
        num_fmt = wb.add_format({"num_format": "0.0000"})
        money_fmt = wb.add_format({"num_format": "0.00"})
        time_fmt = wb.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})

        headers = [
            "Order ID",
            "Symbol",
            "Side",
            "Type",
            "Quantity",
            "Filled Qty",
            "Avg Fill Price",
            "Fees",
            "Placed At",
            "Filled At",
            "Status",
        ]
        # Single backlink to contents
        ws.write_url(0, 0, "internal:'Contents'!A1", string="Contents")
        # Write headers starting row 1
        for c, h in enumerate(headers):
            ws.write(1, c, h, header_fmt)

        row_index = 2
        for o in orders:
            ws.write(row_index, 0, o.order_id)
            ws.write(row_index, 1, o.symbol)
            ws.write(row_index, 2, o.side.value)
            ws.write(row_index, 3, o.order_type.value)
            ws.write_number(row_index, 4, o.quantity, num_fmt)
            ws.write_number(row_index, 5, o.filled_quantity, num_fmt)
            ws.write_number(
                row_index,
                6,
                o.avg_fill_price if o.filled_quantity else 0.0,
                money_fmt,
            )
            ws.write_number(row_index, 7, o.fees, money_fmt)
            if o.placed_at:
                pa = (
                    o.placed_at.replace(tzinfo=None)
                    if o.placed_at.tzinfo
                    else o.placed_at
                )
                ws.write_datetime(row_index, 8, pa, time_fmt)
            if o.filled_at:
                fa = (
                    o.filled_at.replace(tzinfo=None)
                    if o.filled_at.tzinfo
                    else o.filled_at
                )
                ws.write_datetime(row_index, 9, fa, time_fmt)
            ws.write(row_index, 10, o.status.value)
            row_index += 1

        last_row = row_index - 1 if orders else 1
        ws.autofilter(1, 0, last_row, len(headers) - 1)
        ws.freeze_panes(2, 0)
        ws.set_column(0, 0, 14)
        ws.set_column(1, 1, 10)
        ws.set_column(2, 3, 6)
        ws.set_column(4, 7, 12)
        ws.set_column(8, 9, 22)
        ws.set_column(10, 10, 10)

    # -----------------
    # Metrics sheet
    # -----------------
    def _write_metrics_sheet(
        self,
        wb: xlsxwriter.Workbook,
        metrics: "BacktestMetrics",
        ws: xlsxwriter.Worksheet = None,
        broker: BacktestBroker = None,
    ):
        header_fmt = wb.add_format({"bold": True, "align": "left"})
        pct_fmt = wb.add_format({"num_format": "0.00%", "align": "left"})
        num_fmt = wb.add_format({"num_format": "0.00", "align": "left"})
        int_fmt = wb.add_format({"num_format": "0", "align": "left"})
        date_fmt = wb.add_format({"num_format": "yyyy-mm-dd", "align": "left"})

        ws.set_column(0, 1, 25)

        # Header row (row 1)
        r = 7
        ws.merge_range(r, 0, r, 1, "General", header_fmt)

        rows = [
            ("Strategy Name", metrics.strategy_name, None),
            ("Start Date", metrics.start_date, date_fmt),
            ("End Date", metrics.end_date, date_fmt),
            ("Total Days", metrics.total_time.days, int_fmt),
        ]

        r += 1
        for label, value, fmt in rows:
            ws.write(r, 0, label)
            if value is None:
                ws.write(r, 1, "-")
            else:
                if fmt is not None and isinstance(value, (int, float)):
                    ws.write_number(r, 1, value, fmt)
                elif fmt is not None and isinstance(value, datetime):
                    ws.write_datetime(r, 1, value, fmt)
                else:
                    ws.write(r, 1, value)
            r += 1

        r += 1
        # Performance section
        ws.merge_range(r, 0, r, 1, "Performance", header_fmt)

        rows = [
            ("Starting Capital", metrics.initial_cash, num_fmt),
            ("Final Cash", metrics.final_cash, num_fmt),
            ("Equity End", metrics.final_equity, num_fmt),
            ("Equity Peak", metrics.peak_equity, num_fmt),
            ("Equity Trough", metrics.trough_equity, num_fmt),
            ("Simtulated Returns", metrics.portfolio_return_pct, pct_fmt),
            ("Total Fees", metrics.total_fees, num_fmt),
            ("CAGR", metrics.cagr, pct_fmt),
        ]

        r += 1
        for label, value, fmt in rows:
            ws.write(r, 0, label)
            if value is None:
                ws.write(r, 1, "-")
            else:
                if fmt is not None and isinstance(value, (int, float)):
                    ws.write_number(r, 1, value, fmt)
                elif fmt is not None and isinstance(value, datetime):
                    ws.write_datetime(r, 1, value, fmt)
                else:
                    ws.write(r, 1, value)
            r += 1

        if metrics.leftover_positions:
            r += 1
            ws.merge_range(r, 0, r, 1, "Leftover Positions", header_fmt)
            r += 1

            for position in metrics.leftover_positions.values():
                qty = position.quantity
                if qty.is_integer():
                    qty_str = f"{int(qty)}"
                else:
                    qty_str = f"{qty}"
                ws.write(
                    r,
                    0,
                    f"{position.symbol} x {qty_str} @ {broker.last_prices[position.symbol]:.2f}",
                    None,
                )
                ws.write(
                    r,
                    1,
                    f"{position.quantity * broker.last_prices[position.symbol]:.2f} filled @ {position.avg_price:.2f}",
                    None,
                )
                r += 1

        # Trading section
        r = 0
        ws.set_column(3, 4, 22)
        ws.set_column(4, 4, 15)
        ws.merge_range(0, 3, 0, 4, "Trading Technicals", header_fmt)
        performance_rows = [
            ("No of Trades", metrics.total_trades, int_fmt),
            ("No of Winning Trades", metrics.total_wins, int_fmt),
            ("No of Losing Trades", metrics.total_losses, int_fmt),
            ("Average Profit", metrics.average_profit, pct_fmt),
            ("Average Loss", metrics.average_loss, pct_fmt),
            ("Win Rate", metrics.win_rate, pct_fmt),
            ("Expected Value", metrics.expected_value, pct_fmt),
            ("Risk-Reward Ratio", metrics.risk_reward_ratio, num_fmt),
            ("Risk-Free Rate", metrics.risk_free_rate, pct_fmt),
            ("Sharpe Ratio", metrics.sharpe_ratio, num_fmt),
            ("Sortino Ratio", metrics.sortino_ratio, num_fmt),
            ("Calmar Ratio", metrics.calmar_ratio, num_fmt),
            ("Profit Factor", metrics.profit_factor, num_fmt),
        ]

        r += 1
        for label, value, fmt in performance_rows:
            ws.write(r, 3, label)
            if value is None:
                ws.write(r, 4, "-")
            else:
                if fmt is not None and isinstance(value, (int, float)):
                    ws.write_number(r, 4, value, fmt)
                else:
                    ws.write(r, 4, value)
            r += 1

        # Risk section
        r += 1
        ws.merge_range(r, 3, r, 4, "Risk", header_fmt)
        risk_rows = [
            ("No of Drawdowns", metrics.no_of_drawdowns, int_fmt),
            ("Average Drawdown", abs(metrics.average_drawdown), pct_fmt),
            ("Median Drawdown", abs(metrics.average_drawdown), pct_fmt),
            ("Max Drawdown", abs(metrics.max_drawdown), pct_fmt),
            (
                "Max Drawdown Start",
                metrics.max_drawdown_start,
                date_fmt if metrics.max_drawdown_start else None,
            ),
            (
                "Max Drawdown End",
                metrics.max_drawdown_end,
                date_fmt if metrics.max_drawdown_end else None,
            ),
        ]

        r += 1
        for label, value, fmt in risk_rows:
            ws.write(r, 3, label)
            if value is None:
                ws.write(r, 4, "-")
            else:
                if fmt is not None and isinstance(value, (int, float)):
                    ws.write_number(r, 4, value, fmt)
                elif fmt is not None and isinstance(value, datetime):
                    ws.write_datetime(r, 4, value, fmt)
                else:
                    ws.write(r, 4, value)
            r += 1

        # Benchmark section
        r += 1
        ws.merge_range(r, 3, r, 4, "Benchmark", header_fmt)
        benchmark_rows = [
            (g["ticker"], g["growth"], pct_fmt) for g in metrics.benchmark
        ]

        r += 1
        for label, value, fmt in benchmark_rows:
            ws.write(r, 3, label)
            if value is None:
                ws.write(r, 4, "Not available")
            else:
                if fmt is not None and isinstance(value, (int, float)):
                    ws.write_number(r, 4, value, fmt)
                else:
                    ws.write(r, 4, value)
            r += 1

    def _write_heatmaps(
        self, wb: xlsxwriter.Workbook, metrics: "BacktestMetrics", mode: str = "both"
    ) -> List[str]:
        """Create percentage return heatmap sheet(s).

        mode:
                    'daily'   -> DailyHeatmap only (rows = date, columns = day-of-week)
                    'monthly' -> MonthlyHeatmap only (rows = year, columns = Jan..Dec)
                    'both'    -> both sheets
        """
        mode = mode.lower()
        if mode not in {"daily", "monthly", "both"}:
            raise ValueError("heatmap mode must be 'daily', 'monthly', or 'both'")

        # Use prepared daily equity (one row per day) for consistent daily returns
        daily = self._prepare_daily_equity(metrics)
        if daily.empty:
            return []
        created: List[str] = []

        # DAILY (WEEKLY-ROW) HEATMAP ----------------------------------
        if mode in {"daily", "both"}:
            try:
                ws_d = wb.add_worksheet("DailyHeatmap")
            except xlsxwriter.exceptions.DuplicateWorksheetName:
                ws_d = wb.add_worksheet("DailyHeatmap1")
            created.append(ws_d.get_name())
            header_fmt = wb.add_format({"bold": True})
            pct_fmt = wb.add_format({"num_format": "0.00%"})
            ws_d.write_url(0, 0, "internal:'Contents'!A1", string="Contents")
            dow = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            ws_d.write(1, 0, "Week (Mon-Fri)", header_fmt)
            for ci, day_name in enumerate(dow, start=1):
                ws_d.write(1, ci, day_name, header_fmt)

            # Determine unique week starts (Monday) present
            daily["week_start"] = daily["date"].map(
                lambda d: d - timedelta(days=d.weekday())
            )
            week_starts = sorted(daily["week_start"].unique())
            week_row_map = {}
            current_row = 2
            for ws_val in week_starts:
                week_end = ws_val + timedelta(days=4)
                label = f"{ws_val:%Y-%m-%d} - {week_end:%Y-%m-%d}"
                ws_d.write(current_row, 0, label)
                # prefill placeholders for all weekdays
                for c in range(1, len(dow) + 1):
                    ws_d.write(current_row, c, "no data")
                week_row_map[ws_val] = current_row
                current_row += 1

            # Fill in daily returns in appropriate week row/weekday column
            min_val = daily["daily_return_pct"].min() / 100.0 if not daily.empty else 0
            max_val = daily["daily_return_pct"].max() / 100.0 if not daily.empty else 0
            for rec in daily.itertuples(index=False):
                dt = rec.date
                ws_val = dt - timedelta(days=dt.weekday())
                day_name = dt.strftime("%A")
                if day_name in dow:
                    row_idx = week_row_map.get(ws_val)
                    if row_idx is not None:
                        col = dow.index(day_name) + 1
                        ws_d.write_number(
                            row_idx, col, float(rec.daily_return_pct) / 100.0, pct_fmt
                        )

            last_row = current_row - 1
            if last_row >= 2:
                # Banded colors: <0 = light green->green, >0 = light red->red, ==0 = white.
                # This avoids the near-white issue around 0 from 3-color scales.
                bins = 5
                # Daily min/max in fraction terms (already computed above as pct/100.0)
                pos_max = max(max_val, 0)  # most positive (>=0)
                neg_min = min(min_val, 0)  # most negative (<=0)

                # Define color bands (light -> dark)
                red_bands = ["#ffcccc", "#ff9999", "#ff7777", "#ff5555", "#ff3333"]
                green_bands = ["#ccffcc", "#99ff99", "#66ff66", "#33ff33", "#00ff00"]

                # Formats
                white_fmt = wb.add_format({"bg_color": "#FFFFFF"})
                red_fmts = [wb.add_format({"bg_color": c}) for c in red_bands]
                green_fmts = [wb.add_format({"bg_color": c}) for c in green_bands]

                # Positive bands: (0, pos_max] split into 'bins' equal-width bands.
                if pos_max > 0:
                    step = pos_max / bins
                    for i in range(1, bins + 1):
                        low = (i - 1) * step
                        high = i * step
                        # exclude exact 0 from first band by nudging low up a tiny epsilon
                        if i == 1:
                            low = max(low, 1e-12)
                        ws_d.conditional_format(
                            2,
                            1,
                            last_row,
                            len(dow),
                            {
                                "type": "cell",
                                "criteria": "between",
                                "minimum": low,
                                "maximum": high,
                                "format": green_fmts[i - 1],
                            },
                        )

                # Negative bands: [neg_min, 0) split into 'bins' equal-width bands.
                if neg_min < 0:
                    rng = abs(neg_min)
                    step = rng / bins
                    for i in range(1, bins + 1):
                        # bands go from -i*step up to -(i-1)*step
                        low = -i * step
                        high = -(i - 1) * step
                        # exclude exact 0 from last band by nudging high down a tiny epsilon
                        if i == 1:
                            high = min(high, -1e-12)
                        ws_d.conditional_format(
                            2,
                            1,
                            last_row,
                            len(dow),
                            {
                                "type": "cell",
                                "criteria": "between",
                                "minimum": low,
                                "maximum": high,
                                "format": red_fmts[i - 1],
                            },
                        )

                # Exactly zero -> white (add last so it has highest priority)
                ws_d.conditional_format(
                    2,
                    1,
                    last_row,
                    len(dow),
                    {
                        "type": "cell",
                        "criteria": "==",
                        "value": 0,
                        "format": white_fmt,
                    },
                )

            ws_d.freeze_panes(2, 1)
            ws_d.set_column(0, 0, 21)
            ws_d.set_column(1, len(dow), 10)

        # MONTHLY HEATMAP -----------------------------------------------
        if mode in {"monthly", "both"}:
            daily_sorted = daily.sort_values("date")
            daily_sorted["year"] = daily_sorted["date"].dt.year
            daily_sorted["month"] = daily_sorted["date"].dt.month
            # Compute annual (calendar year) returns for later column
            year_returns = {}
            for y, grp_year in daily_sorted.groupby("year"):
                first_eq_year = (
                    grp_year.iloc[0].start_equity
                    if "start_equity" in grp_year.columns
                    else grp_year.iloc[0].equity
                )
                last_eq_year = grp_year.iloc[-1].equity
                if first_eq_year == 0:
                    y_ret = 0.0
                else:
                    y_ret = last_eq_year / first_eq_year - 1.0
                year_returns[y] = y_ret
            month_rows = []
            for (y, m), grp in daily_sorted.groupby(["year", "month"]):
                first_eq = (
                    grp.iloc[0].start_equity
                    if "start_equity" in grp.columns
                    else grp.iloc[0].equity
                )
                last_eq = grp.iloc[-1].equity
                if first_eq == 0:
                    ret = 0.0
                else:
                    ret = last_eq / first_eq - 1.0
                month_rows.append({"year": y, "month": m, "return": ret})
            if month_rows:
                mdf = pd.DataFrame(month_rows)
                mdf["month_name"] = mdf["month"].apply(
                    lambda x: calendar.month_abbr[int(x)]
                )
                pivot = mdf.pivot(
                    index="year", columns="month_name", values="return"
                ).sort_index()
                ordered = [calendar.month_abbr[i] for i in range(1, 13)]
                for col in ordered:
                    if col not in pivot.columns:
                        pivot[col] = pd.NA
                pivot = pivot[ordered]
                try:
                    ws_m = wb.add_worksheet("MonthlyHeatmap")
                except xlsxwriter.exceptions.DuplicateWorksheetName:
                    ws_m = wb.add_worksheet("MonthlyHeatmap1")
                created.append(ws_m.get_name())
                header_fmt = wb.add_format({"bold": True})
                pct_fmt = wb.add_format({"num_format": "0.00%"})
                ws_m.write_url(0, 0, "internal:'Contents'!A1", string="Contents")
                ws_m.write(1, 0, "Year", header_fmt)
                for ci, col in enumerate(ordered, start=1):
                    ws_m.write(1, ci, col, header_fmt)
                annual_col = len(ordered) + 1  # after 12 months
                ws_m.write(1, annual_col, "Year Return %", header_fmt)
                for r_i, (year_val, row_s) in enumerate(pivot.iterrows(), start=2):
                    ws_m.write(r_i, 0, int(year_val))
                    # prefill monthly cells + annual column with placeholders
                    for ci, col in enumerate(ordered, start=1):
                        ws_m.write(r_i, ci, "no data")
                    ws_m.write(r_i, annual_col, "no data")
                    # overwrite where we have actual monthly data
                    for ci, col in enumerate(ordered, start=1):
                        val = row_s[col]
                        if pd.notna(val):
                            ws_m.write_number(r_i, ci, float(val), pct_fmt)
                    # annual return
                    if int(year_val) in year_returns:
                        ws_m.write_number(
                            r_i, annual_col, float(year_returns[int(year_val)]), pct_fmt
                        )

                last_r = 1 + pivot.shape[0]
                # Monthly cells (Jan-Dec): same banded scheme
                bins = 5
                month_pos_max = (
                    float(mdf["return"][mdf["return"] > 0].max()) * 1.0000001
                    if not mdf.empty and (mdf["return"] > 0).any()
                    else 0.0
                )
                month_neg_min = (
                    float(mdf["return"][mdf["return"] < 0].min()) * 1.0000001
                    if not mdf.empty and (mdf["return"] < 0).any()
                    else 0.0
                )

                white_fmt = wb.add_format({"bg_color": "#FFFFFF"})
                red_bands = ["#ffcccc", "#ff9999", "#ff7777", "#ff5555", "#ff3333"]
                green_bands = ["#ccffcc", "#99ff99", "#66ff66", "#33ff33", "#00ff00"]
                red_fmts = [wb.add_format({"bg_color": c}) for c in red_bands]
                green_fmts = [wb.add_format({"bg_color": c}) for c in green_bands]

                # Apply to Jan-Dec block
                if month_pos_max > 0:
                    step = month_pos_max / bins
                    for i in range(1, bins + 1):
                        low = (i - 1) * step
                        high = i * step
                        if i == 1:
                            low = max(low, 1e-12)
                        ws_m.conditional_format(
                            2,
                            1,
                            last_r,
                            12,
                            {
                                "type": "cell",
                                "criteria": "between",
                                "minimum": low,
                                "maximum": high,
                                "format": green_fmts[i - 1],
                            },
                        )
                if month_neg_min < 0:
                    rng = abs(month_neg_min)
                    step = rng / bins
                    for i in range(1, bins + 1):
                        low = -i * step
                        high = -(i - 1) * step
                        if i == 1:
                            high = min(high, -1e-12)
                        ws_m.conditional_format(
                            2,
                            1,
                            last_r,
                            12,
                            {
                                "type": "cell",
                                "criteria": "between",
                                "minimum": low,
                                "maximum": high,
                                "format": red_fmts[i - 1],
                            },
                        )
                ws_m.conditional_format(
                    2,
                    1,
                    last_r,
                    12,
                    {"type": "cell", "criteria": "==", "value": 0, "format": white_fmt},
                )

                # Annual return column banded too
                if year_returns:
                    ann_pos_max = max((v for v in year_returns.values() if v > 0), default=0.0)

                    # White for exactly zero FIRST
                    ws_m.conditional_format(
                        2, annual_col, last_r, annual_col,
                        {"type": "cell", "criteria": "==", "value": 0, "format": white_fmt}
                    )

                    # Green bands for positives (unchanged)
                    if ann_pos_max > 0:
                        step = ann_pos_max / bins
                        for i in range(1, bins + 1):
                            low = (i - 1) * step
                            high = i * step
                            if i == 1:
                                low = max(low, 1e-12)
                            ws_m.conditional_format(
                                2, annual_col, last_r, annual_col,
                                {"type": "cell", "criteria": "between", "minimum": low,
                                "maximum": high, "format": green_fmts[i - 1]},
                            )

                    # **Replace** your negative-band loop with a single "< 0" rule
                    ws_m.conditional_format(
                        2, annual_col, last_r, annual_col,
                        {"type": "cell", "criteria": "<", "value": 0, "format": red_fmts[0]}
                    )

                ws_m.freeze_panes(2, 1)
                ws_m.set_column(0, 0, 8)
                ws_m.set_column(1, 12, 10)
                ws_m.set_column(annual_col, annual_col, 14)

        return created

    def _write_contents_sheet(self, wb: xlsxwriter.Workbook, sheets: List[str]):
        """Create a 'Contents' sheet linking to provided sheet names (excludes per-date order sheets)."""
        try:
            ws = wb.add_worksheet("Contents")
        except xlsxwriter.exceptions.DuplicateWorksheetName:
            ws = wb.add_worksheet("Contents1")

        title_fmt = wb.add_format({"bold": True, "font_size": 14})
        header_fmt = wb.add_format({"bold": True, "align": "left"})
        link_fmt = wb.add_format({"underline": 1})
        ws.write(0, 0, "Report Contents", title_fmt)
        ws.write(2, 0, "Sheet", header_fmt)
        row = 3
        for name in sheets:
            safe = name.replace("'", "''")
            ws.write_url(row, 0, f"internal:'{safe}'!A1", link_fmt, string=name)
            row += 1
        ws.set_column(0, 0, 30)
        ws.freeze_panes(3, 0)

    def generate_trade_pairs(self, wb: xlsxwriter.Workbook, broker: BacktestBroker):
        trade_data = []

        for pair in broker.trade_pairs:
            trade_data.append(
                {
                    "symbol": pair.symbol,
                    "timestamp": pair.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "side": pair.side.value,
                    "total_entry_cost": pair.entry_avg_price,
                    "total_exit_amount": pair.exit_avg_price,
                    "pnl": pair.pnl,
                    "quantity": pair.quantity,
                    "return_pct": pair.return_pct,
                }
            )

        # Create worksheet
        try:
            ws = wb.add_worksheet("Trade Pairs")
        except xlsxwriter.exceptions.DuplicateWorksheetName:
            ws = wb.add_worksheet("Trade Pairs1")

        # Define formats
        header_fmt = wb.add_format({"bold": True, "bg_color": "#EFEFEF", "border": 1})
        money_fmt = wb.add_format({"num_format": "$#,##0.00"})
        pct_fmt = wb.add_format({"num_format": "0.00%"})
        text_fmt = wb.add_format({"text_wrap": True})
        text_mid = wb.add_format({"align": "center"})
        int_fmt = wb.add_format({"num_format": "#,##0", "align": "left"})
        float_fmt = wb.add_format({"num_format": "#,##0.####"})

        # Write headers
        headers = [
            "Symbol",
            "Timestamp",
            "Qty",
            "Side",
            "Entry Avg",
            "Exit Avg",
            "PnL",
            "Return %",
        ]
        for col, header in enumerate(headers):
            ws.write(0, col, header, header_fmt)

        # Write rows
        for row, trade in enumerate(trade_data, start=1):
            ws.write(row, 0, trade["symbol"], text_fmt)
            ws.write(
                row, 1, str(trade["timestamp"]), text_fmt
            )  # convert to string for safety
            val = trade["quantity"]
            if float(val).is_integer():
                ws.write_number(row, 2, int(val), int_fmt)
            else:
                ws.write_number(row, 2, val, float_fmt)
            if trade["side"] == "BUY":
                ws.write(row, 3, "Long", text_mid)
            else:
                ws.write(row, 3, "Short", text_mid)
            ws.write_number(row, 4, trade["total_entry_cost"], money_fmt)
            ws.write_number(row, 5, trade["total_exit_amount"], money_fmt)
            ws.write_number(row, 6, trade["pnl"], money_fmt)
            ws.write_number(row, 7, trade["return_pct"], pct_fmt)

        # Auto-fit columns
        ws.set_column(0, 0, 12)  # Symbol
        ws.set_column(1, 1, 20)  # Timestamp
        ws.set_column(2, 2, 6)  # Quantity
        ws.set_column(3, 3, 8)  # Side
        ws.set_column(4, 7, 12)  # Others
        ws.freeze_panes(1, 0)

    def generate_report(
        self,
        broker: BacktestBroker,
        metrics: "BacktestMetrics",
        orders: List[Order],
        output_dir: str = "reports",
        heatmap_mode: str = "both",
    ) -> Path:
        if metrics.equity_curve.empty:
            raise ValueError("Equity curve is empty; cannot build report.")

        file_dir = Path(output_dir)
        file_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = file_dir / f"backtest_report_{timestamp}.xlsx"

        daily = self._prepare_daily_equity(metrics)
        orders_by_day = self._orders_by_day(orders)

        # Build comparison charts up front
        chart_paths = self._generate_equity_comparison_charts(
            broker=broker,
            output_dir=file_dir,
            tickers=("SPY", "QQQ", "TQQQ"),
            timestamp=timestamp,
        )

        with xlsxwriter.Workbook(file_path, {"remove_timezone": True}) as wb:
            # Create Contents first so it's the first tab
            try:
                contents_ws = wb.add_worksheet("Contents")
            except xlsxwriter.exceptions.DuplicateWorksheetName:
                contents_ws = wb.add_worksheet("Contents1")

            title_fmt = wb.add_format({"bold": True, "font_size": 14})
            contents_header_fmt = wb.add_format({"bold": True, "bg_color": "#EFEFEF"})
            link_fmt = wb.add_format({"font_color": "blue", "underline": 1})

            # Left side header and column headers
            contents_ws.write(0, 0, "Report Contents", title_fmt)
            contents_ws.write(2, 0, "Sheet", contents_header_fmt)

            # Build non-date sheets
            non_date: List[str] = []
            self._write_metrics_sheet(wb, metrics, contents_ws, broker)
            self._write_summary_sheet(
                wb, daily, orders_by_day, broker
            )  # CHANGED: pass broker
            non_date.append("Summary")
            self.generate_trade_pairs(wb, broker)
            heatmap_sheets = self._write_heatmaps(wb, metrics, mode=heatmap_mode)
            non_date.extend(heatmap_sheets)

            # Populate contents links on the left
            row = 3
            for name in non_date:
                safe = name.replace("'", "''")
                contents_ws.write_url(
                    row, 0, f"internal:'{safe}'!A1", link_fmt, string=name
                )
                row += 1

            # Insert charts on the right side (column G by default)
            self._insert_charts_on_contents(
                contents_ws,
                chart_paths,
                start_col=6,
                start_row=0,
                x_scale=0.9,
                y_scale=0.9,
            )

            # Append daily order sheets last
            for day_key in sorted(orders_by_day.keys()):
                self._write_orders_sheet(wb, day_key, orders_by_day[day_key])

        return file_path
