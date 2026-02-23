from datetime import datetime

from src.config import get_backtest_symbols, get_alpaca_key_and_secret
from src.adapters.backtest_report.backtest_report import PerformanceReport
from src.bootstrap import bootstrap
from src.adapters.alpaca_market import AlpacaMarketAdapter
from src.adapters.backtest_broker import BacktestBroker
from src.adapters.strategies.moving_average import MovingAverageStrategy
from src.adapters.strategies.rsi_strategy import RSIStrategy


if __name__ == "__main__":
    alpaca_config = get_alpaca_key_and_secret()
    market_adapter = AlpacaMarketAdapter(
        api_key=alpaca_config["ALPACA_API_KEY"],
        secret_key=alpaca_config["ALPACA_API_SECRET"],
    )
    broker_adapter = BacktestBroker()
    strategy = RSIStrategy(broker=broker_adapter, timeframe="5m")
    report = PerformanceReport(broker=broker_adapter)

    bus = bootstrap(market_adapter, strategy, broker_adapter, report)

    market_adapter.setup_event_bus(bus)
    broker_adapter.setup_event_bus(bus)
    strategy.setup_event_bus(bus)

    market_adapter.connect()

    report.print_full_report()
    report.save_equity_curve()
    report.generate_report()
