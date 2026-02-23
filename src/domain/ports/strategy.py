import abc
from datetime import timedelta
from typing import Dict, Optional
from src.domain import commands, events
from src.domain.ports.broker import AbstractBroker


class Strategy(abc.ABC):
    def __init__(self, broker: AbstractBroker, **kwargs):
        self.broker = broker
        super().__init__(**kwargs)

    @property
    def name(self):
        return self.__class__.__name__

    # -------------------
    # Lifecycle hooks
    # -------------------
    @abc.abstractmethod
    def on_start(self, cmd: commands.StartStrategyCommand): ...
    @abc.abstractmethod
    def on_end(self, cmd: commands.EndStrategyCommand): ...
    @abc.abstractmethod
    def on_day_start(self, event: events.DayStarted): ...
    @abc.abstractmethod
    def on_day_end(self, event: events.DayEnded): ...
    @abc.abstractmethod
    def on_tick_changed(self, event: events.QuoteReceived): ...
    @abc.abstractmethod
    def on_order_changed(self, event: events.Event): ...
