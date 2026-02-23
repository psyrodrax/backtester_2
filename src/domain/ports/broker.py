import abc
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

from src.domain.models.position import Position
from src.domain.models.trade_pair import TradePair
from src.domain.models.order import Order
from src.domain import commands, events


class AbstractBroker(abc.ABC):
    """
    Port (abstract interface) for trading brokers.
    """

    def __init__(self, gst: float = 0.09):
        self.gst = gst
        self._comments: pd.DataFrame = pd.DataFrame(columns=["timestamp", "comment"])

    @abc.abstractmethod
    def handle_place_order(self, cmd: commands.PlaceOrderCommand):  # pragma: no cover
        """Place an order; may emit OrderPlaced (and optionally immediately executed) events.
        Should either return a list of events OR publish them via event_bus if attached.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def on_day_start(self, event: events.DayStarted):  # pragma: no cover
        """Process start of trading day."""
        raise NotImplementedError

    @abc.abstractmethod
    def on_day_end(self, event: events.DayEnded):  # pragma: no cover
        """Process end of trading day."""
        raise NotImplementedError

    @abc.abstractmethod
    def on_quote(self, quote: events.QuoteReceived):  # pragma: no cover
        """Process incoming quote for potential order execution. Should emit OrderExecuted events."""
        raise NotImplementedError

    @abc.abstractmethod
    def calculate_fees(self, order: Order) -> float:
        """Calculate fees for a given order."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def initial_cash(self) -> float:
        """Retrieve the initial cash balance."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def available_cash(self) -> float:
        """Retrieve the available cash balance."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def total_fees(self) -> float:
        """Retrieve the total fees incurred."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def positions(self) -> Dict[str, Position]:
        """Retrieve the current open positions."""
        raise NotImplementedError
    
    @property
    @abc.abstractmethod
    def get_equity_value(self) -> float:
        """Retrieve the current equity value of positions."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def trade_pairs(self) -> List[TradePair]:
        """Retrieve the current trade pairs."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def start_date(self) -> datetime:
        """Retrieve the start date of the backtest."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def end_date(self) -> datetime:
        """Retrieve the end date of the backtest."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def total_time(self) -> timedelta:
        """Retrieve the total time duration of the backtest."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def equity_curve(self) -> pd.DataFrame:
        """Retrieve the equity curve of the backtest."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def filled_orders(self) -> List[Order]:
        """Retrieve the list of filled orders."""
        raise NotImplementedError
    
    @property
    @abc.abstractmethod
    def last_prices(self) -> Dict[str, float]:
        """Retrieve the last prices for symbols."""
        raise NotImplementedError

    @property
    def comments(self) -> pd.DataFrame:
        """Retrieve the comments as a DataFrame with columns ['timestamp', 'comment']."""
        return self._comments

    def add_comment(self, timestamp: datetime, comment: str):
        """Add a comment for a specific timestamp (normalized to day start)."""
        timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        new_row = pd.DataFrame([{"timestamp": timestamp, "comment": comment}])
        self._comments = pd.concat([self._comments, new_row], ignore_index=True)

    #     # Commission
    #     commission = self._clamp(0.005 * num_shares, 0.99, 0.005 * trade_value)

    #     # Platform
    #     platform_fee = self._clamp(0.005 * num_shares, 1, 0.005 * trade_value)

    #     # SEC Fee (sell only)
    #     sec_fee = 0
    #     if not is_buy:
    #         sec_fee = self._clamp(0.000008 * trade_value, 0.01)

    #     # Settlement Fee
    #     settlement_fee = self._clamp(0.003 * num_shares, max_val=0.07 * trade_value)

    #     # Trading Activity Fee (sell only)
    #     ta_fee = 0
    #     if not is_buy:
    #         ta_fee = self._clamp(0.000166 * num_shares, 0.01, 8.3)

    #     total_fee_value = (
    #         commission + platform_fee + sec_fee + settlement_fee + ta_fee
    #     ) * (1 + self.gst)

    #     return total_fee_value
