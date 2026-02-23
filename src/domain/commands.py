# pylint: disable=missing-docstring,too-few-public-methods
from dataclasses import dataclass
from datetime import datetime

from src.domain.models.order import OrderType, OrderSide


class Command:
    pass


@dataclass(frozen=True)
class GenerateSignalCommand(Command):
    symbol: str
    action: str
    generated_at: datetime


@dataclass(frozen=True)
class PlaceOrderCommand(Command):
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    price: float = None         # required for LIMIT orders
    stop_loss: float = None    # optional stop loss price
    take_profit: float = None  # optional take profit price


@dataclass(frozen=True)
class CancelOrderCommand(Command):
    symbol: str
    order_id: str

@dataclass(frozen=True)
class StartStrategyCommand(Command):
    timestamp: datetime
    
@dataclass(frozen=True)
class EndStrategyCommand(Command):
    timestamp: datetime