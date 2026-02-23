from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from typing import List, Optional


class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    FILLED_PARTIALLY = "FILLED_PART"
    FILLED_ALL = "FILLED_ALL"
    CANCELLED_PARTIALLY = "CANCELLED_PART"
    CANCELLED_ALL = "CANCELLED_ALL"
    FAILED = "FAILED"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class OrderFill:
    price: float
    quantity: float
    timestamp: datetime


@dataclass
class Order:
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    limit_price: Optional[float] = None
    placed_at: datetime = None

    status: OrderStatus = OrderStatus.SUBMITTED
    filled_quantity: float = 0.0
    fills: list[OrderFill] = field(default_factory=list)
    _fees: float = 0.0
    _filled_at: Optional[datetime] = None
    stop_loss: Optional[float] = None
    stop_triggered: bool = False

    def __init__(
        self,
        order_id: str,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: float,
        limit_price: Optional[float] = None,
        placed_at: datetime = None,
    ):
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.quantity = quantity
        self.limit_price = limit_price
        self.placed_at = placed_at
        self.status = OrderStatus.SUBMITTED
        self.filled_quantity = 0.0
        self.fills: List[OrderFill] = []

    def shallow_copy(self) -> "Order":
        order:Order = Order(
            order_id=self.order_id,
            symbol=self.symbol,
            side=self.side,
            order_type=self.order_type,
            quantity=self.quantity,
            limit_price=self.limit_price,
            placed_at=self.placed_at,
        )
        order.status = self.status
        order.filled_quantity = self.filled_quantity
        order.fills = self.fills.copy()
        order._fees = self._fees
        order._filled_at = self._filled_at
        order.stop_loss = self.stop_loss
        order.stop_triggered = self.stop_triggered
        return order

    def apply_fill(self, fill: OrderFill):
        if self.status in [OrderStatus.FILLED_ALL, OrderStatus.CANCELLED_ALL]:
            raise ValueError("Cannot apply fill to a filled or cancelled order.")

        if fill.quantity <= 0:
            raise ValueError("Invalid fill quantity.")

        if self.filled_quantity + fill.quantity > self.quantity:
            raise ValueError("Fill quantity exceeds remaining order quantity.")

        self.filled_quantity += fill.quantity
        self.fills.append(fill)

        if self.filled_quantity == self.quantity:
            self.status = OrderStatus.FILLED_ALL
            self._filled_at = fill.timestamp

        if self.filled_quantity > 0 and self.status not in [
            OrderStatus.FILLED_ALL,
            OrderStatus.FILLED_PARTIALLY,
        ]:
            self.status = OrderStatus.FILLED_PARTIALLY

    @property
    def filled_at(self) -> Optional[datetime]:
        return self._filled_at

    @property
    def fees(self) -> float:
        return self._fees

    @fees.setter
    def fees(self, fees: float):
        if fees < 0:
            raise ValueError("Fees cannot be negative.")
        self._fees = fees

    @property
    def filled_cost(self) -> float:
        return sum(fill.price * fill.quantity for fill in self.fills)

    @property
    def avg_fill_price(self) -> float:
        """Average fill price of the order excluding fees."""
        if not self.fills:
            return 0.0
        return self.filled_cost / self.filled_quantity

    @property
    def avg_price(self) -> float:
        """Average price of the order including fees."""
        if not self.fills:
            return 0.0
        return (self.filled_cost + self.fees) / self.filled_quantity

    def check_stop_loss(self, bid_price: float, ask_price: float) -> bool:
        if self.stop_loss is None or self.stop_triggered:
            return False
        if self.side == OrderSide.BUY and bid_price <= self.stop_loss:
            self.stop_triggered = True
            return True
        if self.side == OrderSide.SELL and ask_price >= self.stop_loss:
            self.stop_triggered = True
            return True
        return False
