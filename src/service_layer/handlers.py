# src/service_layer/handlers.py
from src.domain.ports import AbstractBroker, Strategy
from src.domain import events, commands


def handle_candle_received(event: events.CandleReceived, strategy: Strategy):
    strategy.on_tick_changed(event)


def handle_quote_received(
    event: events.QuoteReceived, strategy: Strategy, broker: AbstractBroker
):
    broker.on_quote(event)
    strategy.on_tick_changed(event)


def handle_day_start(event: events.Event, strategy: Strategy, broker: AbstractBroker):
    strategy.on_day_start(event)
    broker.on_day_start(event)


def handle_day_end(event: events.Event, strategy: Strategy, broker: AbstractBroker):
    strategy.on_day_end(event)
    broker.on_day_end(event)


def handle_order_filled(event: events.OrderFilled, strategy: Strategy, broker: AbstractBroker):
    broker.handle_order_filled(event)
    strategy.on_order_changed(event)
    


def handle_stop_loss_triggered(event: events.StopLossTriggered, strategy: Strategy, broker: AbstractBroker):
    strategy.on_order_changed(event)


# --- Command Handlers ---


def handle_place_order(cmd: commands.PlaceOrderCommand, broker: AbstractBroker):
    """Delegate PlaceOrder command to broker adapter."""
    broker.handle_place_order(cmd)


def handle_start_strategy(cmd: commands.StartStrategyCommand, strategy: Strategy):
    strategy.on_start(cmd)


def handle_end_strategy(cmd: commands.EndStrategyCommand, strategy: Strategy):
    strategy.on_end(cmd)


# --- Event Handlers Map ---

EVENT_HANDLERS = {
    events.CandleReceived: [handle_candle_received],
    events.QuoteReceived: [handle_quote_received],
    events.DayStarted: [handle_day_start],
    events.DayEnded: [handle_day_end],
    events.OrderFilled: [handle_order_filled],
    events.StopLossTriggered: [handle_stop_loss_triggered],
}

# --- Command Handlers Map ---

COMMAND_HANDLERS = {
    commands.PlaceOrderCommand: handle_place_order,
    commands.StartStrategyCommand: handle_start_strategy,
    commands.EndStrategyCommand: handle_end_strategy,
}
