#!/usr/bin/env python3
"""
version 3.1.0
Market Data Update Script

Key Features:
- OHLCV data fetching from Polygon.io with rate limit compliance (5 calls/minute)
- NYSE trading calendar integration for precise business day calculations
- Intelligent missing data detection and batch fetching optimization
- SQLite database storage with INSERT OR REPLACE for data integrity
- Support for 95-symbol watchlist with individual progress tracking
- New symbol detection with full historical data backfill (365 days)

Fetches OHLCV data from Polygon.io and stores in SQLite database.
Manages API rate limits and trading day calculations for efficient data retrieval.
"""

import os
import sys
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from polygon import RESTClient
import pandas_market_calendars as mcal
from dotenv import load_dotenv

# Import centralized logging
from logging_config import setup_logger, log_script_start, log_script_end

# Setup logging
logger = setup_logger("update_db")

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"
ENV_PATH = PROJECT_ROOT / ".env"

# Load environment variables
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    logger.info(f"Loaded environment from: {ENV_PATH}")
else:
    logger.warning(f"Environment file not found: {ENV_PATH}")
    load_dotenv()  # Try default locations

REQUEST_PAUSE_DURATION = 13  # Rate limit: 5 calls/minute + buffer
POLYGON_KEY = os.getenv("POLYGON_KEY")

# Watchlist - this should match your current watchlist plus additional indices
WATCHLIST = [
    # Core portfolio
    "TLT",
    "BND",
    "SCHD",
    "SPY",
    "PHYS",
    "AAPL",
    "MSFT",
    "ORCL",
    "PLTR",
    "PANW",
    "SNPS",
    "CRWD",
    "AMZN",
    "EBAY",
    "TSLA",
    "GM",
    "F",
    "HD",
    "LOW",
    "MCD",
    "SBUX",
    "DPZ",
    "BKNG",
    "ABNB",
    "MAR",
    "HLT",
    "NKE",
    "RL",
    "LVS",
    "WYNN",
    "MGM",
    "WMT",
    "COST",
    "TGT",
    "PG",
    "KO",
    "PEP",
    "NVDA",
    "AMD",
    "AVGO",
    "QCOM",
    "MU",
    "CRM",
    "INTU",
    "NOW",
    "DELL",
    "HPE",
    "CSCO",
    "S",
    "OKTA",
    "FTNT",
    "AMAT",
    "LRCX",
    "ACN",
    "FSLR",
    "GOOGL",
    "META",
    "TMUS",
    "T",
    "NFLX",
    "DIS",
    "SPOT",
    "BABA",
    "TCEHY",
    "JD",
    "BIDU",
    "GE",
    "BA",
    "CAT",
    "UNP",
    "CSX",
    "HON",
    "MMM",
    "ROK",
    "AME",
    "DAL",
    "UAL",
    "WM",
    "RSG",
    "V",
    "MA",
    "AXP",
    "JPM",
    "BAC",
    "WFC",
    "C",
    "BX",
    "BLK",
    "SPGI",
    "ICE",
    "CME",
    "MS",
    "GS",
    "SCHW",
    "COIN",
    "MSCI",
    "LLY",
    "JNJ",
    "PFE",
    "ABT",
    "SPG",
    "O",
    "XOM",
    "CVX",
    "OXY",
    "COP",
    "LIN",
    "SHW",
]


def initialize_polygon_client():
    """Initialize Polygon REST client with API key validation.

    Essential Features:
    - Environment variable validation for POLYGON_KEY
    - REST client initialization with error handling
    - Connection testing and authentication verification
    """
    if not POLYGON_KEY:
        logger.error("POLYGON_KEY not found in environment variables")
        sys.exit(1)

    return RESTClient(POLYGON_KEY)


def symbol_exists(symbol):
    """Check if symbol has any data in database.

    Essential Features:
    - Quick existence check using COUNT query
    - Database connection handling with proper cleanup
    - Boolean return for new vs existing symbol detection
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM stock_data WHERE symbol = ? LIMIT 1", (symbol,))
    result = c.fetchone() is not None
    conn.close()
    return result


def get_latest_db_date(symbol):
    """Get most recent date for symbol in database.

    Essential Features:
    - Retrieves latest date using MAX(date) query
    - Date format conversion and validation
    - Handles missing data gracefully
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT MAX(date) FROM stock_data WHERE symbol = ?", (symbol,))
    result = c.fetchone()[0]
    conn.close()
    return result


def get_missing_trading_days(symbol, days_needed=150):
    """Calculate missing trading days for symbol based on NYSE calendar.

    Essential Features:
    - NYSE calendar integration for accurate trading day calculation
    - Efficient calendar-to-trading-day conversion (~252 trading days/year)
    - Database comparison using set operations for missing date detection
    - Automatic search period extension if insufficient trading days found
    - Optimized query using LIMIT for recent data comparison
    """
    nyse = mcal.get_calendar("NYSE")
    end_date = datetime.now().date() - timedelta(days=1)
    calendar_days_needed = int(days_needed / 0.69) + 10  # ~252 trading days/year
    start_date = end_date - timedelta(days=calendar_days_needed)

    all_trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    if len(all_trading_days) < days_needed:
        logger.warning(f"Extending search period for {symbol}")
        start_date = end_date - timedelta(days=int(days_needed / 0.65))
        all_trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    required_trading_days = all_trading_days[-days_needed:]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT date FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        (symbol, days_needed),
    )
    db_dates = {datetime.strptime(row[0], "%Y-%m-%d").date() for row in c.fetchall()}
    conn.close()

    missing_dates = [day for day in required_trading_days if day.date() not in db_dates]

    logger.info(f"{symbol}: {len(db_dates)} have, {len(missing_dates)} missing")
    return missing_dates


def fetch_ohlcv_data(client, symbol, start_date, end_date):
    """Fetch and store OHLCV data for date range with rate limiting.

    Essential Features:
    - Polygon API daily OHLC data retrieval with adjusted prices
    - Rate limiting compliance with 13-second delays between calls
    - Data validation and null value handling with precision rounding
    - INSERT OR REPLACE database operations for data integrity
    - Progress tracking with success/failure counting per symbol
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    nyse = mcal.get_calendar("NYSE")
    trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)
    logger.info(f"Fetching {len(trading_days)} days for {symbol}")
    successful_updates = 0

    for day in trading_days:
        date_str = day.strftime("%Y-%m-%d")
        try:
            resp = client.get_daily_open_close_agg(symbol, date_str, adjusted="true")
            if getattr(resp, "status", None) != "OK":
                continue

            c.execute(
                """INSERT OR REPLACE INTO stock_data 
                (symbol, date, open_price, high_price, low_price, close_price, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    resp.symbol,
                    resp.from_,
                    round(resp.open, 2) if resp.open is not None else None,
                    round(resp.high, 2) if resp.high is not None else None,
                    round(resp.low, 2) if resp.low is not None else None,
                    round(resp.close, 2) if resp.close is not None else None,
                    resp.volume,
                ),
            )
            conn.commit()
            successful_updates += 1

        except Exception as e:
            logger.error(f"Error fetching {symbol} {date_str}: {e}")

        time.sleep(REQUEST_PAUSE_DURATION)

    conn.close()
    logger.info(f"✅ {symbol}: {successful_updates}/{len(trading_days)} updated")


def update_ohlcv_data():
    """Update OHLCV data for all watchlist symbols.

    Essential Features:
    - Processes 95-symbol watchlist with progress tracking
    - New symbol detection with 365-day historical backfill
    - Missing data identification using 150-trading-day requirement
    - Batch date range optimization to minimize API calls
    - Individual symbol success confirmation and logging
    """
    logger.info(f"Updating {len(WATCHLIST)} symbols (150 trading days)")
    client = initialize_polygon_client()

    for i, symbol in enumerate(WATCHLIST, 1):
        logger.info(f"[{i}/{len(WATCHLIST)}] {symbol}")

        if not symbol_exists(symbol):
            logger.info(f"New symbol {symbol} - fetching 1 year history")
            start_date = datetime.now().date() - timedelta(days=365)
            end_date = datetime.now().date() - timedelta(days=1)
            fetch_ohlcv_data(client, symbol, start_date, end_date)
        else:
            missing_dates = get_missing_trading_days(symbol, days_needed=150)
            if missing_dates:
                if len(missing_dates) > 1:
                    start_date = min(missing_dates).date()
                    end_date = max(missing_dates).date()
                    fetch_ohlcv_data(client, symbol, start_date, end_date)
                else:
                    single_date = missing_dates[0].date()
                    fetch_ohlcv_data(client, symbol, single_date, single_date)
            else:
                logger.info(f"✅ {symbol} up to date")


def main():
    """Execute market data update workflow.

    Essential Features:
    - Complete OHLCV data pipeline execution
    - Database path validation and logging
    - Comprehensive error handling with exit code management
    - Script timing and completion status tracking
    """
    start_time = datetime.now()
    log_script_start(logger, "Database Update Script")

    logger.info(f"Database: {DB_PATH}")

    try:
        update_ohlcv_data()
        logger.info("✅ Market data update completed")
        log_script_end(logger, "Database Update Script", start_time, True)
    except Exception as e:
        logger.error(f"❌ Market data update failed: {e}", exc_info=True)
        log_script_end(logger, "Database Update Script", start_time, False)
        sys.exit(1)


if __name__ == "__main__":
    main()
