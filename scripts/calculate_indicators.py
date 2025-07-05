#!/usr/bin/env python3
"""
version 3.0.0
Indicators Calculator Script
Calculates indicators from OHLCV data in database
Currently only BTD and STR multiple periods are calculated
"""

import os
import sys
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, List
import pandas as pd

# Setup simple console logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"


def get_stock_data(symbol: str, days: int = 150) -> pd.DataFrame:
    """Get stock data from database"""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT date, open_price, high_price, low_price, close_price, volume
        FROM stock_data 
        WHERE symbol = ? 
        ORDER BY date DESC 
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(symbol, days))
    conn.close()

    if df.empty:
        logger.warning(f"No data found for {symbol}")
        return pd.DataFrame()

    # Convert date column and sort
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df


def calculate_btd_str(df: pd.DataFrame, period: int) -> Tuple[float, float]:
    """Calculate BTD and STR for a given period - matches original logic"""
    if len(df) < period + 1:  # Need period + 1 for current day + period lookback
        return None, None

    # Get current day data (most recent)
    current_day = df.iloc[-1]
    high_price = current_day["high_price"]
    low_price = current_day["low_price"]

    # Get past 'period' days (excluding current day)
    past_data = df.iloc[-(period + 1) : -1]  # Last period days, excluding current

    # Find lowest and highest close prices in the past period
    lowest_close = past_data["close_price"].min()
    highest_close = past_data["close_price"].max()

    # Calculate BTD: (current_high - lowest_close_past) / lowest_close_past * 100
    if lowest_close > 0:
        btd = ((high_price - lowest_close) / lowest_close) * 100
    else:
        btd = None

    # Calculate STR: (current_low - highest_close_past) / highest_close_past * 100
    if highest_close > 0:
        str_value = ((low_price - highest_close) / highest_close) * 100
    else:
        str_value = None

    return (
        round(btd, 2) if btd is not None else None,
        round(str_value, 2) if str_value is not None else None,
    )


def calculate_and_update_btd(symbol: str):
    """Calculate BTD indicators for 22, 66, 132 periods and update database"""
    logger.info(f"Calculating BTD indicators for {symbol}...")

    # Get enough data for 132-period calculation + 1 current day
    df = get_stock_data(symbol, days=140)  # Buffer for weekends/holidays
    if df.empty or len(df) < 23:  # Need at least 23 days for BTD-22
        logger.warning(f"Insufficient data for BTD calculation: {symbol}")
        return

    # Calculate BTD for all timeframes
    btd_22, _ = calculate_btd_str(df, 22)
    btd_66, _ = calculate_btd_str(df, 66)
    btd_132, _ = calculate_btd_str(df, 132)

    # Update database
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    update_fields = {}
    if btd_22 is not None:
        update_fields["btd_22"] = btd_22
    if btd_66 is not None:
        update_fields["btd_66"] = btd_66
    if btd_132 is not None:
        update_fields["btd_132"] = btd_132

    if update_fields:
        columns = list(update_fields.keys())
        values = list(update_fields.values())

        set_clause = ", ".join([f"{col} = ?" for col in columns])
        query = f"""
            UPDATE stock_data SET {set_clause}
            WHERE symbol = ? AND date = (SELECT MAX(date) FROM stock_data WHERE symbol = ?)
        """

        c.execute(query, values + [symbol, symbol])
        conn.commit()
        logger.info(f"Updated BTD indicators for {symbol}: {update_fields}")

    conn.close()


def calculate_and_update_str(symbol: str):
    """Calculate STR indicators for 22, 66, 132 periods and update database"""
    logger.info(f"Calculating STR indicators for {symbol}...")

    # Get enough data for 132-period calculation + 1 current day
    df = get_stock_data(symbol, days=140)  # Buffer for weekends/holidays
    if df.empty or len(df) < 23:  # Need at least 23 days for STR-22
        logger.warning(f"Insufficient data for STR calculation: {symbol}")
        return

    # Calculate STR for all timeframes
    _, str_22 = calculate_btd_str(df, 22)
    _, str_66 = calculate_btd_str(df, 66)
    _, str_132 = calculate_btd_str(df, 132)

    # Update database
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    update_fields = {}
    if str_22 is not None:
        update_fields["str_22"] = str_22
    if str_66 is not None:
        update_fields["str_66"] = str_66
    if str_132 is not None:
        update_fields["str_132"] = str_132

    if update_fields:
        columns = list(update_fields.keys())
        values = list(update_fields.values())

        set_clause = ", ".join([f"{col} = ?" for col in columns])
        query = f"""
            UPDATE stock_data SET {set_clause}
            WHERE symbol = ? AND date = (SELECT MAX(date) FROM stock_data WHERE symbol = ?)
        """

        c.execute(query, values + [symbol, symbol])
        conn.commit()
        logger.info(f"Updated STR indicators for {symbol}: {update_fields}")

    conn.close()


def update_custom_indicators_for_symbol(symbol: str):
    """Update custom indicators for a single symbol - BTD and STR only"""
    logger.info(f"Processing custom indicators for {symbol}...")

    # Calculate and update BTD indicators
    calculate_and_update_btd(symbol)

    # Calculate and update STR indicators
    calculate_and_update_str(symbol)


def get_all_symbols() -> List[str]:
    """Get all unique symbols from database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT symbol FROM stock_data")
    symbols = [row[0] for row in c.fetchall()]
    conn.close()
    return symbols


def main():
    """Main function to calculate all custom indicators"""
    logger.info("Starting custom indicators calculation...")
    logger.info(f"Database path: {DB_PATH}")

    start_time = datetime.now()

    try:
        # Get all symbols from database
        symbols = get_all_symbols()

        if not symbols:
            logger.warning("No symbols found in database")
            return

        logger.info(f"Processing {len(symbols)} symbols...")

        # Process each symbol
        for symbol in symbols:
            try:
                update_custom_indicators_for_symbol(symbol)
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info(f"Custom indicators calculation completed in {duration}")
        logger.info(f"Successfully processed {len(symbols)} symbols")

    except Exception as e:
        logger.error(f"Error during custom indicators calculation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
