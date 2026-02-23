# pylint: disable=missing-docstring,too-few-public-methods

from dataclasses import dataclass
from datetime import datetime

from src.domain.models.order import OrderSide, Order


class Event:
    pass


@dataclass(frozen=True)
class DayStarted(Event):
    date: datetime


@dataclass(frozen=True)
class DayEnded(Event):
    date: datetime


@dataclass(frozen=True)
class CandleReceived(Event):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class QuoteReceived(Event):
    symbol: str
    timestamp: datetime
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float


@dataclass(frozen=True)
class TradeReceived(Event):
    symbol: str
    timestamp: datetime
    price: float
    size: float


@dataclass(frozen=True)
class SignalGenerated(Event):
    symbol: str
    action: str
    generated_at: datetime


@dataclass(frozen=True)
class OrderPlaced(Event):
    symbol: str
    order_id: str
    side: OrderSide
    quantity: float
    price: float
    placed_at: datetime


@dataclass(frozen=True)
class OrderFilled(Event):
    order: Order


@dataclass(frozen=True)
class StopLossTriggered(Event):
    order: Order
    triggered_at: datetime
