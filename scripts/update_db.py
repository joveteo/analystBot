#!/usr/bin/env python3
"""
version 3.0.0
Market Data Update Script
Fetches OHLCV data from Polygon.io and updates into database
Handles free tier rate limits (5 calls/minute)
"""

import os
import sys
import time
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from polygon import RESTClient
import pandas_market_calendars as mcal
from dotenv import load_dotenv

# Setup simple console logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"
ENV_PATH = PROJECT_ROOT / ".env"

# Load environment variables
load_dotenv(ENV_PATH)

# Configuration
REQUEST_PAUSE_DURATION = 13  # seconds (5 calls/minute = 12s + buffer)
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

# Configuration - OHLCV only, no technical indicators
# Technical indicators removed for simplified system


def initialize_polygon_client():
    """Initialize Polygon REST client"""
    if not POLYGON_KEY:
        logger.error("POLYGON_KEY not found in environment variables")
        sys.exit(1)

    return RESTClient(POLYGON_KEY)


def symbol_exists(symbol):
    """Check if symbol exists in database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM stock_data WHERE symbol = ? LIMIT 1", (symbol,))
    result = c.fetchone() is not None
    conn.close()
    return result


def get_latest_db_date(symbol):
    """Get the latest date for a symbol in the database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT MAX(date) FROM stock_data WHERE symbol = ?", (symbol,))
    result = c.fetchone()[0]
    conn.close()
    return result


def get_missing_trading_days(symbol, days_needed=150):
    """Get list of missing trading days for a symbol - PRECISE & EFFICIENT"""
    nyse = mcal.get_calendar("NYSE")
    today = datetime.now().date()
    end_date = today - timedelta(days=1)  # Yesterday

    # Calculate precise calendar days needed for trading days
    # ~252 trading days per year = ~0.69 trading days per calendar day
    # To get 150 trading days: need ~217 calendar days (150 ÷ 0.69)
    calendar_days_needed = int(days_needed / 0.69) + 10  # Small safety buffer
    start_date = end_date - timedelta(days=calendar_days_needed)

    # Get all trading days in the period
    all_trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    # Ensure we have enough trading days (safety check)
    if len(all_trading_days) < days_needed:
        logger.warning(
            f"Only found {len(all_trading_days)} trading days, extending search..."
        )
        start_date = end_date - timedelta(
            days=int(days_needed / 0.65)
        )  # More aggressive
        all_trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    required_trading_days = all_trading_days[-days_needed:]  # Last N trading days

    # Get last N records from database (simple & fast)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT date FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        (symbol, days_needed),
    )
    db_dates = {datetime.strptime(row[0], "%Y-%m-%d").date() for row in c.fetchall()}
    conn.close()

    # Find missing dates
    missing_dates = [day for day in required_trading_days if day.date() not in db_dates]

    logger.info(
        f"{symbol}: Required {len(required_trading_days)} days, have {len(db_dates)} days, missing {len(missing_dates)} days"
    )
    return missing_dates


def fetch_ohlcv_data(client, symbol, start_date, end_date):
    """Fetch OHLCV data from Polygon for a date range"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    nyse = mcal.get_calendar("NYSE")
    trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    logger.info(
        f"📊 Fetching {len(trading_days)} trading days for {symbol} ({start_date} to {end_date})"
    )
    successful_updates = 0

    for day in trading_days:
        date_str = day.strftime("%Y-%m-%d")
        try:
            resp = client.get_daily_open_close_agg(symbol, date_str, adjusted="true")
            if getattr(resp, "status", None) != "OK":
                logger.debug(f"No data for {symbol} on {date_str}")
                continue

            c.execute(
                """
                INSERT OR REPLACE INTO stock_data 
                (symbol, date, open_price, high_price, low_price, close_price, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
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
            logger.debug(f"✅ Updated OHLCV for {symbol} on {date_str}")

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol} on {date_str}: {e}")

        # Rate limiting
        time.sleep(REQUEST_PAUSE_DURATION)

    conn.close()
    logger.info(
        f"✅ Completed {symbol}: {successful_updates}/{len(trading_days)} days updated"
    )


def update_ohlcv_data():
    """Update OHLCV data for all symbols"""
    logger.info("Starting OHLCV data update...")
    logger.info(f"Target: Last 150 trading days for {len(WATCHLIST)} symbols")
    client = initialize_polygon_client()

    for i, symbol in enumerate(WATCHLIST, 1):
        logger.info(f"[{i}/{len(WATCHLIST)}] Processing OHLCV for {symbol}...")

        if not symbol_exists(symbol):
            logger.info(f"🆕 New symbol {symbol}, fetching full history...")
            start_date = datetime.now().date() - timedelta(days=365)  # 1 year
            end_date = datetime.now().date() - timedelta(days=1)
            fetch_ohlcv_data(client, symbol, start_date, end_date)
        else:
            # Check for missing recent data
            missing_dates = get_missing_trading_days(
                symbol, days_needed=150
            )  # Last 150 trading days
            if missing_dates:
                logger.info(f"Fetching {len(missing_dates)} missing days for {symbol}")
                # Fetch in batches to minimize API calls
                if len(missing_dates) > 1:
                    start_date = min(missing_dates).date()
                    end_date = max(missing_dates).date()
                    fetch_ohlcv_data(client, symbol, start_date, end_date)
                else:
                    # Single date
                    fetch_ohlcv_data(
                        client, symbol, missing_dates[0].date(), missing_dates[0].date()
                    )
            else:
                logger.info(f"✅ OHLCV data for {symbol} is up to date")


def main():
    """Main function - OHLCV data only"""
    logger.info("Starting OHLCV market data update...")
    logger.info(f"Database path: {DB_PATH}")
    logger.info(f"Processing {len(WATCHLIST)} symbols")

    start_time = datetime.now()

    try:
        # Update OHLCV data only
        update_ohlcv_data()

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info(f"🎉 OHLCV data update completed in {duration}")
        logger.info(f"📈 System ready for BTD/STR analysis on {len(WATCHLIST)} symbols")
        logger.info("✅ Database optimized for multi-timeframe calculations")

    except Exception as e:
        logger.error(f"❌ Error during OHLCV data update: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
