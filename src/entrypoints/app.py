from datetime import datetime

from src.config import get_backtest_dates, get_backtest_symbols
from src.adapters.backtest_report.backtest_report import PerformanceReport
from src.bootstrap import bootstrap
from src.adapters.backtest_market import FindatapyBacktestAdapter
from src.adapters.backtest_market_yfin import YfinBacktestAdapter
from src.adapters.backtest_broker import BacktestBroker
from src.adapters.strategies.test import Test
from src.adapters.strategies.hedgedTQQQ import HedgedTQQQ


if __name__ == "__main__":
    start_date, end_date = get_backtest_dates()
    market_adapter = YfinBacktestAdapter(
        symbols=get_backtest_symbols(), start_date=start_date, end_date=end_date, speed=0, show_progress=True
    )  # fast replay
    broker_adapter = BacktestBroker()
    strategy = HedgedTQQQ(broker=broker_adapter)
    report = PerformanceReport(strategy=strategy, broker=broker_adapter)

    bus = bootstrap(market_adapter, strategy, broker_adapter, report)

    market_adapter.setup_event_bus(bus)
    broker_adapter.setup_event_bus(bus)
    strategy.setup_event_bus(bus)

    market_adapter.connect()

    report.print_full_report()
    report.create_folder()
    report.save_equity_curve()
    report.generate_report()
    report.save_config()
    report.save_strategy()
