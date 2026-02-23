# src/adapters/backtest_broker.py
from datetime import datetime, timedelta
from typing import Dict, List
from uuid import uuid4

import pandas as pd

from src.domain import commands, events
from src.domain.models.order import Order, OrderFill, OrderSide, OrderStatus, OrderType
from src.domain.models.position import Position
from src.domain.models.trade_pair import TradePair
from src.domain.ports import AbstractBroker, EventBusAdapter


class BacktestBroker(EventBusAdapter, AbstractBroker):
    def __init__(self, initial_cash: float = 100_000):
        super().__init__()
        # Each element: {"cmd": PlaceOrderCommand, "order_id": str, "placed_at": datetime}
        self._open_orders: List[Order] = []  # pending orders (market + limit)
        self._last_price: Dict[str, float] = {}  # last mid prices per symbol

        self._initial_cash = initial_cash
        self._start_date: datetime = None
        self._end_date: datetime = None

        self._cash = initial_cash
        self._total_fees = 0.0
        self._trade_pairs: List[TradePair] = []
        self._positions: Dict[str, Position] = {}  # current positions
        self._equity_curve: Dict[datetime, float] = {}
        self._filled_orders: List[Order] = []
        self._current_timestamp = None

    # === Message Handlers ===
    def handle_place_order(self, cmd: commands.PlaceOrderCommand):
        """Place an order and queue it for execution.

        MARKET: Executed on the *next* QuoteReceived strictly after placement
                ( BUY filled at next quote ask, SELL at next quote bid ).
        LIMIT:  Executed when quote satisfies limit condition.
        """
        order_id = uuid4().hex

        new_order = Order(
            order_id=order_id,
            symbol=cmd.symbol,
            side=cmd.side,
            quantity=cmd.quantity,
            limit_price=cmd.price,
            order_type=cmd.order_type,
            placed_at=self._current_timestamp,
        )
        # Store full context so we can enforce next-tick logic (compare timestamps)
        self._open_orders.append(new_order)

    def on_day_start(self, event: events.DayStarted):
        """Handle the start of a trading day."""
        if self._start_date is None:
            self._start_date = event.date
            self._equity_curve[event.date] = self._cash

    def on_day_end(self, event: events.DayEnded):
        """Handle the end of a trading day."""
        self._end_date = event.date
        self._equity_curve[event.date] = self.total_assets

    def on_quote(self, quote: events.QuoteReceived):
        """Process a QuoteReceived event and attempt fills for queued orders."""
        self._current_timestamp = quote.timestamp
        
        # Update last mid price reference
        mid = (quote.bid_price + quote.ask_price) / 2.0
        self._last_price[quote.symbol] = mid

        remaining = []
        for order in self._open_orders:
            if order.symbol != quote.symbol:
                remaining.append(order)
                continue

            if self._should_execute(order, quote):
                fill_price = 0.0

                if order.order_type == OrderType.MARKET:
                    fill_price = (
                        quote.ask_price
                        if order.side == OrderSide.BUY
                        else quote.bid_price
                    )
                elif order.order_type == OrderType.LIMIT:
                    fill_price = order.limit_price

                order.apply_fill(
                    OrderFill(
                        price=fill_price,
                        quantity=order.quantity,
                        timestamp=quote.timestamp,
                    )
                )

                fees = self.calculate_fees(order)
                order.fees = fees

                order_filled_event = events.OrderFilled(order=order)
                self.event_bus.handle(order_filled_event)

            else:
                remaining.append(order)

        self._open_orders = remaining

    def handle_order_filled(self, event: events.OrderFilled):
        order: Order = event.order

        pos = self.positions.get(order.symbol)
        if pos:
            pos_qty = pos.quantity
            recorded_quantity = 0.0
            order_side = None

            if pos_qty > 0 and order.side == OrderSide.SELL:
                recorded_quantity = min(order.filled_quantity, pos_qty)
                order_side = OrderSide.BUY

            elif pos_qty < 0 and order.side == OrderSide.BUY:
                recorded_quantity = min(order.filled_quantity, abs(pos_qty))
                order_side = OrderSide.SELL

            if recorded_quantity > 0:
                tradepair = TradePair(
                    symbol=order.symbol,
                    quantity=recorded_quantity,
                    side=order_side,
                    entry_avg_price=pos.avg_price,
                    exit_avg_price=order.avg_fill_price,
                    timestamp=order.filled_at,
                )
                self.trade_pairs.append(tradepair)

        self.update_position(order)
        self.update_cash_and_fees(order)
        self._filled_orders.append(order)

    def update_position(self, order: Order):
        """
        Update the position for a given symbol.
        If quantity is positive, it represents a buy.
        If quantity is negative, it represents a sell.
        """
        if order.symbol not in self.positions:
            self.positions[order.symbol] = Position(order.symbol, 0, 0)

        pos = self.positions[order.symbol]
        pos.add_filled_order(order)

        if pos.quantity == 0:
            del self.positions[order.symbol]

    def update_cash_and_fees(self, order: Order):
        """
        Update the cash balance and total fees based on the order.
        """
        if order.side == OrderSide.BUY:
            self._cash -= order.filled_cost
        elif order.side == OrderSide.SELL:
            self._cash += order.filled_cost

        self._cash -= order.fees
        self._total_fees += order.fees

    # === Execution Management ===
    def _should_execute(self, order: Order, quote: events.QuoteReceived):
        if order.order_type == OrderType.MARKET:
            # Only after a strictly later quote timestamp (next tick)
            return quote.timestamp > order.placed_at

        # LIMIT logic
        limit_price = order.limit_price
        if limit_price is None:
            return False
        if order.side == OrderSide.BUY and quote.ask_price <= limit_price:
            return True
        if order.side == OrderSide.SELL and quote.bid_price >= limit_price:
            return True
        return False

    # === Price Management ===
    def _get_market_price(self, symbol, _side):
        return self._last_price.get(symbol)

    def calculate_fees(self, order: Order) -> float:
        if order.status not in [
            OrderStatus.FILLED_ALL,
            OrderStatus.CANCELLED_PARTIALLY,
        ]:
            raise ValueError("Invalid order status for fee calculation.")

        txn_amount = order.filled_cost
        fees = {}

        # Platform Fee
        if order.quantity < 1:
            platform_fee = min(0.0099 * txn_amount, 0.99)
        else:
            platform_fee = 0.99
        gst_fee = platform_fee * self.gst
        fees["Platform + GST"] = platform_fee + gst_fee

        # SEC Fee (sell only)
        if order.side == "SELL":
            sec_fee = max(0.0000278 * txn_amount, 0.01)
            fees["SEC Fee"] = sec_fee

        # Settlement Fee
        settlement_fee = min(0.003 * order.quantity, 0.01 * txn_amount)
        fees["Settlement Fee"] = settlement_fee

        # TAF (sell only)
        if order.side == "SELL":
            taf_fee = min(max(0.000166 * order.quantity, 0.01), 8.30)
            fees["TAF"] = taf_fee

        # CAT Fee
        cat_fee = 0.0000265 * order.quantity
        fees["CAT Fee"] = cat_fee

        # Total
        total_fees = sum(fees.values())

        return total_fees

    # === Account Management ===
    @property
    def available_cash(self) -> float:
        return self._cash

    @property
    def initial_cash(self) -> float:
        return self._initial_cash
    
    @property
    def total_assets(self) -> float:
        return self.available_cash + sum(
            pos.quantity * self.last_prices[pos.symbol] for pos in self.positions.values()
        )

    @property
    def total_fees(self) -> float:
        return self._total_fees

    @property
    def positions(self) -> Dict[str, Position]:
        return self._positions
    
    @property
    def get_equity_value(self) -> float:
        return sum(
            pos.quantity * self.last_prices[pos.symbol] for pos in self.positions.values()
        ) + self.available_cash

    @property
    def trade_pairs(self) -> List[TradePair]:
        return self._trade_pairs

    @property
    def start_date(self) -> datetime:
        return self._start_date

    @property
    def end_date(self) -> datetime:
        return self._end_date

    @property
    def total_time(self) -> timedelta:
        if self._start_date and self._end_date:
            delta = self._end_date - self._start_date

            # Ensure minimum of 1 day
            if delta < timedelta(days=1):
                return timedelta(days=1)

            return delta
        return None

    @property
    def equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(
            list(self._equity_curve.items()), columns=["date", "equity"]
        )

    @property
    def filled_orders(self) -> List[Order]:
        return self._filled_orders

    @property
    def last_prices(self) -> Dict[str, float]:
        return self._last_price
