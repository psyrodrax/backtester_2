from typing import List, Sequence

from alpaca.data.live import StockDataStream
from alpaca.data.models import Bar, Quote, Trade

from src.domain.ports import EventBusAdapter, MarketDataFeed, SubscribeType
from src.domain import events


class AlpacaMarketAdapter(EventBusAdapter, MarketDataFeed):
    def __init__(self, api_key: str, secret_key: str, symbols: List[str] = None, types: Sequence[SubscribeType] = ("QUOTES",)):
        super().__init__()
        self.stream = StockDataStream(api_key=api_key, secret_key=secret_key)
        self.symbols = symbols or []
        self.types = types

    def connect(self):
        self.stream.run()

        for subscribe_type in self.types:
            if subscribe_type == "CANDLES":
                self.stream.subscribe_bars(self._on_candle, self.symbols)
            elif subscribe_type == "QUOTES":
                self.stream.subscribe_quotes(self._on_quote, self.symbols)
            elif subscribe_type == "TRADES":
                self.stream.subscribe_trades(self._on_trade, self.symbols)

    def _on_candle(self, candle: Bar):
        event = events.CandleReceived(
            symbol=candle.symbol,
            timestamp=candle.timestamp,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
        )
        if self.event_bus:
            self.event_bus.handle(event)

    def _on_quote(self, quote: Quote):
        event = events.QuoteReceived(
            symbol=quote.symbol,
            timestamp=quote.timestamp,
            bid_price=quote.bid_price,
            ask_price=quote.ask_price,
            bid_size=quote.bid_size,
            ask_size=quote.ask_size,
        )
        if self.event_bus:
            self.event_bus.handle(event)

    def _on_trade(self, trade: Trade):
        event = events.TradeReceived(
            symbol=trade.symbol,
            timestamp=trade.timestamp,
            price=trade.price,
            size=trade.size,
        )
        if self.event_bus:
            self.event_bus.handle(event)

    def close(self):
        self.stream.stop()
