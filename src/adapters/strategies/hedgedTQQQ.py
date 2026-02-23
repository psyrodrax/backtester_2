from typing import Optional
from src.domain import events, commands
import yfinance as yf
from datetime import datetime, timedelta
from src.domain import commands, events
from src.domain.models import OrderSide, OrderType
from src.domain.ports import AbstractBroker, EventBusAdapter, Strategy
from enum import IntEnum


class Season(IntEnum):
    Regular = 1
    Crisis = 2
    Bear = 3


class HedgedTQQQ(EventBusAdapter, Strategy):
    def __init__(
        self,
        broker: AbstractBroker,
    ):
        super().__init__(broker=broker)
        self.initial = True
        self.bought_in = False
        self.ytd_vix = 0.0
        self.ytd_vvix = 0.0
        self.days = 0
        self.in_trade = False
        self.season: Season = Season.Regular
        self.safe_check_count = 0

    @property
    def name(self):
        return "Hedged TQQQ"

    def on_start(self, cmd: commands.StartStrategyCommand):
        self.bought_in = False
        print("\nTemplate strategy started.")

    def on_end(self, cmd: commands.EndStrategyCommand):
        print("\nTemplate strategy ended.")

    def on_day_start(self, event: events.DayStarted):
        vix = self.get_open(event.date, "VIX")
        vvix = self.get_open(event.date, "VVIX")
        self.broker.add_comment(event.date, f"VIX: {vix:.2f}, VVIX: {vvix:.2f}, TQQQ: {self.broker.last_prices['TQQQ']:.2f}")

        self.days += 1
        if not self.in_trade:
            if self.season == Season.Crisis and self.days > 60 and vix > 40:
                self.season = Season.Bear
                self.trade(is_buy=True, ticker="SVXY", leverage=1)
                self.broker.add_comment(event.date, "Entered Bear Season")
            elif vvix > 160:
                self.season = Season.Crisis
                self.trade(is_buy=True, ticker="TQQQ")
                self.days = 0
                self.broker.add_comment(event.date, "Entered Crisis Season")
            elif vvix < 90 and vix < 16:
                self.season = Season.Regular
                self.trade(is_buy=True, ticker="TQQQ")
                self.days = 0
                self.broker.add_comment(event.date, "Entered Regular Season")
        else:
            match (self.season):
                case Season.Regular:
                    if vvix > 110:
                        self.trade(is_buy=False, ticker="TQQQ")
                        self.broker.add_comment(event.date, "Exited Regular Season")
                case Season.Crisis:
                    if vvix < 90 or self.days > 60:
                        self.trade(is_buy=False, ticker="TQQQ")
                        self.broker.add_comment(event.date, "Exited Crisis Season")
                case Season.Bear:
                    if vvix < 90 and vix < 16:
                        self.trade(is_buy=False, ticker="SVXY")
                        self.broker.add_comment(event.date, "Exited Bear Season")

        self.ytd_vix = vix
        self.ytd_vvix = vvix

    def on_day_end(self, event: events.DayEnded):
        pass

    def on_tick_changed(
        self, event: events.Event
    ) -> Optional[commands.PlaceOrderCommand]:
        pass

    def on_order_changed(self, event: events.Event):
        pass

    def trade(self, is_buy: bool, ticker: str = "QQQ", leverage: float = 1):
        import math

        quantity = 0
        if is_buy:
            funds = self.broker.available_cash
            quantity = math.floor(
                int(funds / self.broker.last_prices[ticker]) * leverage
            )
            self.in_trade = True
        else:
            quantity = self.broker.positions[ticker].quantity
            self.in_trade = False

        action_cmd = commands.PlaceOrderCommand(
            symbol=ticker,
            side=OrderSide.BUY if is_buy else OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=quantity,
        )
        self.event_bus.handle(action_cmd)

    def short_vxx(self, is_open: bool, ticker: str = "VXX"):
        quantity = 0
        if is_open:
            funds = self.broker.available_cash
            quantity = int(funds / self.broker.last_prices[ticker])
            self.in_trade = True
        else:
            quantity = abs(self.broker.positions[ticker].quantity)
            self.in_trade = False

        action_cmd = commands.PlaceOrderCommand(
            symbol=ticker,
            side=OrderSide.SELL if is_open else OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=quantity,
        )
        self.event_bus.handle(action_cmd)

    def get_open(self, datetime_started_utc: datetime, ticker: str = "VIX") -> float:
        import contextlib, sys, io

        with contextlib.redirect_stderr(io.StringIO()):
            vix = yf.Ticker(f"^{ticker}")
            date_str = datetime_started_utc.strftime("%Y-%m-%d")
            next_date_str = (datetime_started_utc + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            hist = vix.history(start=date_str, end=next_date_str, interval="1d")
        if hist.empty:
            if ticker == "VIX":
                return self.ytd_vix
            if ticker == "VVIX":
                return self.ytd_vvix
        return float(hist.iloc[0]["Open"])
