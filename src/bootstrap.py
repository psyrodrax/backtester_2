# src/bootstrap.py
import inspect
from typing import Callable
from src.adapters.backtest_report.backtest_report import PerformanceReport
from src.service_layer import handlers, messagebus
from src.domain.ports import MarketDataFeed, AbstractBroker, Strategy


def bootstrap(
    market_adapter: MarketDataFeed,
    strategy: Strategy,
    broker: AbstractBroker,
    report: PerformanceReport,
    publish: Callable = print,  # replace with Redis, Kafka, etc.
) -> messagebus.MessageBus:
    """
    Wires together handlers with injected dependencies and returns the MessageBus.
    """

    dependencies = {
        "market_adapter": market_adapter,
        "broker": broker,
        "strategy": strategy,
        "publish": publish,
        "report": report
    }

    injected_event_handlers = {
        event_type: [
            inject_dependencies(handler, dependencies)
            for handler in event_handlers
        ]
        for event_type, event_handlers in handlers.EVENT_HANDLERS.items()
    }

    injected_command_handlers = {
        command_type: inject_dependencies(handler, dependencies)
        for command_type, handler in handlers.COMMAND_HANDLERS.items()
    }

    return messagebus.MessageBus(
        event_handlers=injected_event_handlers,
        command_handlers=injected_command_handlers,
    )


def inject_dependencies(handler, dependencies):
    params = inspect.signature(handler).parameters
    deps = {
        name: dependency
        for name, dependency in dependencies.items()
        if name in params
    }
    return lambda message: handler(message, **deps)
