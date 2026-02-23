from typing import List, Literal, Sequence
import abc

# Allowed data stream types
SubscribeType = Literal["BARS", "QUOTES", "TRADES"]


class MarketDataFeed(abc.ABC):
    """
    Port (abstract interface) for market data sources.
    """

    @abc.abstractmethod
    def connect(
        self,
        symbols: List[str],
        types: Sequence[SubscribeType] = ("BARS", "QUOTES", "TRADES"),
    ):
        """Subscribe to market data for the given symbols.

        types must be a sequence containing only 'BARS', 'QUOTES', or 'TRADES'.
        Implementations may ignore unsupported entries but should not accept
        arbitrary strings.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        """Gracefully close connection."""
        raise NotImplementedError
