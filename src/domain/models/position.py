from dataclasses import dataclass

from src.domain.models.order import OrderSide, Order


@dataclass
class Position:
    symbol: str
    quantity: float
    avg_price: float

    def __init__(self, symbol: str, quantity: float, avg_price: float):
        self.symbol = symbol
        self.quantity = quantity
        self.avg_price = avg_price

    @property
    def total_cost(self) -> float:
        return abs(self.quantity) * self.avg_price

    def add_filled_order(self, order: Order):
        """Add a filled order to the position, supporting both long and short."""
        if order.side == OrderSide.BUY:
            self.add(order.filled_quantity, order.avg_fill_price, order.fees)
        elif order.side == OrderSide.SELL:
            self.subtract(order.filled_quantity, order.avg_fill_price, order.fees)

    def add(self, quantity: float, price: float, fee: float = 0.0):
        """Buy shares/contracts, updating avg_price for long or covering short."""
        if quantity <= 0:
            raise ValueError("Quantity to add must be positive.")

        if self.quantity >= 0:
            new_quantity = self.quantity + quantity
            total_cost = self.total_cost + quantity * price + fee
            self.avg_price = total_cost / new_quantity
            self.quantity = new_quantity
            return

        new_quantity = self.quantity + quantity
        if new_quantity < 0:
            self.quantity = new_quantity
            return
        if new_quantity == 0:
            self.quantity = 0
            self.avg_price = 0.0
            return

        self.quantity = new_quantity
        self.avg_price = (new_quantity * price + fee) / new_quantity

    def subtract(self, quantity: float, price: float, fee: float = 0.0):
        """Sell shares/contracts, updating avg_price for shorts or reducing longs."""
        if quantity <= 0:
            raise ValueError("Quantity to subtract must be positive.")

        new_quantity = self.quantity - quantity

        if self.quantity > 0 and new_quantity > 0:
            proportion_remaining = new_quantity / self.quantity
            remaining_cost = self.total_cost * proportion_remaining + fee
            self.avg_price = remaining_cost / new_quantity
            self.quantity = new_quantity
            return

        if self.quantity > 0 and new_quantity == 0:
            self.quantity = 0
            self.avg_price = 0.0
            return

        if self.quantity > 0 and new_quantity < 0:
            short_quantity = -new_quantity
            self.quantity = -short_quantity
            self.avg_price = (short_quantity * price + fee) / short_quantity
            return

        if new_quantity == 0:
            self.quantity = 0
            self.avg_price = 0.0
            return

        if new_quantity < 0:
            total_cost = self.total_cost + quantity * price + fee
            self.avg_price = total_cost / (-new_quantity)
            self.quantity = new_quantity
            return

        self.quantity = new_quantity
        self.avg_price = (new_quantity * price + fee) / new_quantity
