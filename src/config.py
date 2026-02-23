from datetime import datetime
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from typing import Tuple, List


load_dotenv(override=True)


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "config.json"
BACKTEST_CONFIG_PATH = ROOT_DIR / "config" / "backtest_config.json"


def read_config():
    """Read the configuration file and return the parsed JSON."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def read_backtest_config():
    """Read the backtest configuration and return the parsed JSON."""
    with open(BACKTEST_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        return config


def get_alpaca_endpoint():
    """Retrieve Alpaca API endpoint from config.json."""
    config = read_config()
    alpaca_config = config.get("alpaca", {})
    return alpaca_config.get("ALPACA_ENDPOINT")


def get_alpaca_key_and_secret():
    """Retrieve Alpaca API key and secret from config.json."""
    config = read_config()
    alpaca_config = config.get("alpaca", {})
    return {
        "ALPACA_API_KEY": alpaca_config.get("ALPACA_API_KEY"),
        "ALPACA_API_SECRET": alpaca_config.get("ALPACA_API_SECRET"),
    }


def get_telegram_token():
    """Retrieve Telegram bot token from config.json."""
    config = read_config()
    telegram_config = config.get("telegram", {})
    return telegram_config.get("token")


def get_telegram_ids():
    """Retrieve Telegram chat IDs from config.json."""
    config = read_config()
    telegram_config = config.get("telegram", {})
    return telegram_config.get("ids", {})


def get_fred_api_key():
    """Retrieve FRED API key from .env file."""
    return os.getenv("FRED_API_KEY")


def get_backtest_dates() -> Tuple[datetime, datetime]:
    """Retrieve backtest start and end dates from config.json."""
    config = read_backtest_config()
    return (
        datetime.fromisoformat(config.get("start_date")),
        datetime.fromisoformat(config.get("end_date")),
    )

def get_backtest_symbols() -> List[str]:
    """Retrieve backtest symbols from config.json."""
    config = read_backtest_config()
    return config.get("symbols", [])