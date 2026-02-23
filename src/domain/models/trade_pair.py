from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from src.domain.models.order import Order, OrderSide, OrderStatus
from dataclasses import dataclass, field

    
@dataclass
class TradePair:
    symbol: str
    quantity: float
    side: OrderSide
    entry_avg_price: float
    exit_avg_price: float
    timestamp: datetime
    pnl: float
    return_pct: float

    def __init__(self, symbol: str, quantity: float, side: OrderSide, entry_avg_price: float, exit_avg_price: float, timestamp: datetime) -> None:
        self.symbol = symbol
        self.quantity = quantity
        self.side = side
        self.entry_avg_price = entry_avg_price
        self.exit_avg_price = exit_avg_price
        self.timestamp = timestamp
        if self.side == OrderSide.BUY:
            self.pnl = self.exit_avg_price - self.entry_avg_price
        else:
            self.pnl = self.entry_avg_price - self.exit_avg_price
        self.return_pct = self.pnl / self.entry_avg_price if self.entry_avg_price != 0 else 0.0

