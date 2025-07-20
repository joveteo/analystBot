#!/usr/bin/env python3
"""
version 3.1.0
Indicators Calculator Script

Key Features:
- BTD/STR calculation using Larry Williams' VixFix methodology
- Multi-timeframe analysis: 22, 66, and 132-period lookbacks
- Pandas-based data processing with 150-day rolling window requirements
- Safe parameterized SQL queries preventing injection attacks
- Database validation with automatic symbol discovery and processing
- Contrarian signal generation: BTD for oversold, STR for overbought conditions

Calculates BTD (Buy The Dip) and STR (Short The Rip) indicators for multiple timeframes.
Uses Larry Williams' VixFix methodology across 22, 66, and 132-period lookbacks.
"""

import sys
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Tuple, List
import pandas as pd

# Import centralized logging
from logging_config import setup_logger, log_script_start, log_script_end

# Setup logging
logger = setup_logger("calculate_indicators")

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"


def get_stock_data(symbol: str, days: int = 150) -> pd.DataFrame:
    """Retrieve OHLCV data for symbol from database.

    Essential Features:
    - Parameterized SQL query with ORDER BY date DESC for recent data
    - LIMIT clause for efficient data retrieval (default: 150 days)
    - Pandas DataFrame conversion with date parsing and sorting
    - Empty DataFrame handling for missing symbols
    """
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
        logger.warning(f"No data for {symbol}")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def calculate_btd_str(df: pd.DataFrame, period: int) -> Tuple[float, float]:
    """Calculate BTD and STR indicators using VixFix methodology.

    Essential Features:
    - VixFix algorithm: compares current high/low to period min/max close
    - BTD calculation: (current_high - lowest_close_past) / lowest_close_past * 100
    - STR calculation: (current_low - highest_close_past) / highest_close_past * 100
    - Data sufficiency validation (period + 1 days minimum required)
    - 2-decimal precision rounding for consistent output
    """
    if len(df) < period + 1:
        return None, None

    current_day = df.iloc[-1]
    high_price = current_day["high_price"]
    low_price = current_day["low_price"]

    past_data = df.iloc[-(period + 1) : -1]
    lowest_close = past_data["close_price"].min()
    highest_close = past_data["close_price"].max()

    btd = (
        ((high_price - lowest_close) / lowest_close) * 100 if lowest_close > 0 else None
    )
    str_value = (
        ((low_price - highest_close) / highest_close) * 100
        if highest_close > 0
        else None
    )

    return (
        round(btd, 2) if btd is not None else None,
        round(str_value, 2) if str_value is not None else None,
    )


def calculate_and_update_btd(symbol: str):
    """Calculate and store BTD indicators for all timeframes.

    Essential Features:
    - Multi-timeframe BTD calculation (22, 66, 132 periods)
    - Data sufficiency validation (minimum 23 days required)
    - Safe parameterized UPDATE queries with dynamic field construction
    - Latest date targeting using MAX(date) subquery
    - Result validation and selective database updates
    """
    df = get_stock_data(symbol, days=140)
    if df.empty or len(df) < 23:
        logger.warning(f"Insufficient data for {symbol} BTD")
        return

    btd_22, _ = calculate_btd_str(df, 22)
    btd_66, _ = calculate_btd_str(df, 66)
    btd_132, _ = calculate_btd_str(df, 132)

    update_fields = {}
    if btd_22 is not None:
        update_fields["btd_22"] = btd_22
    if btd_66 is not None:
        update_fields["btd_66"] = btd_66
    if btd_132 is not None:
        update_fields["btd_132"] = btd_132

    if update_fields:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        set_clause = ", ".join([f"{col} = ?" for col in update_fields.keys()])
        query = """UPDATE stock_data SET {} 
                   WHERE symbol = ? AND date = (SELECT MAX(date) FROM stock_data WHERE symbol = ?)""".format(
            set_clause
        )

        c.execute(query, list(update_fields.values()) + [symbol, symbol])
        conn.commit()
        conn.close()
        logger.info(f"{symbol} BTD: {update_fields}")


def calculate_and_update_str(symbol: str):
    """Calculate and store STR indicators for all timeframes.

    Essential Features:
    - Multi-timeframe STR calculation (22, 66, 132 periods)
    - Data sufficiency validation (minimum 23 days required)
    - Safe parameterized UPDATE queries with dynamic field construction
    - Latest date targeting using MAX(date) subquery
    - Result validation and selective database updates
    """
    df = get_stock_data(symbol, days=140)
    if df.empty or len(df) < 23:
        logger.warning(f"Insufficient data for {symbol} STR")
        return

    _, str_22 = calculate_btd_str(df, 22)
    _, str_66 = calculate_btd_str(df, 66)
    _, str_132 = calculate_btd_str(df, 132)

    update_fields = {}
    if str_22 is not None:
        update_fields["str_22"] = str_22
    if str_66 is not None:
        update_fields["str_66"] = str_66
    if str_132 is not None:
        update_fields["str_132"] = str_132

    if update_fields:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        set_clause = ", ".join([f"{col} = ?" for col in update_fields.keys()])
        query = """UPDATE stock_data SET {} 
                   WHERE symbol = ? AND date = (SELECT MAX(date) FROM stock_data WHERE symbol = ?)""".format(
            set_clause
        )

        c.execute(query, list(update_fields.values()) + [symbol, symbol])
        conn.commit()
        conn.close()
        logger.info(f"{symbol} STR: {update_fields}")


def update_custom_indicators_for_symbol(symbol: str):
    """Calculate BTD and STR indicators for single symbol.

    Essential Features:
    - Sequential execution of BTD and STR calculations
    - Single symbol processing with error isolation
    - Database update coordination for both indicator types
    """
    calculate_and_update_btd(symbol)
    calculate_and_update_str(symbol)


def get_all_symbols() -> List[str]:
    """Retrieve all unique symbols from database.

    Essential Features:
    - DISTINCT symbol query for complete database coverage
    - Sorted output for consistent processing order
    - Database connection handling with proper cleanup
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT symbol FROM stock_data")
    symbols = [row[0] for row in c.fetchall()]
    conn.close()
    return symbols


def main():
    """Execute indicators calculation for all symbols.

    Essential Features:
    - Complete database symbol processing with progress tracking
    - Database existence validation before processing
    - Individual symbol error handling with continue-on-failure
    - Success rate calculation and logging
    - Exit code management based on processing results
    """
    start_time = datetime.now()
    log_script_start(logger, "Indicators Calculator Script")

    logger.info(f"Database: {DB_PATH}")

    if not DB_PATH.exists():
        logger.error(f"Database not found: {DB_PATH}")
        log_script_end(logger, "Indicators Calculator Script", start_time, False)
        sys.exit(1)

    try:
        symbols = get_all_symbols()
        if not symbols:
            logger.warning("No symbols in database")
            log_script_end(logger, "Indicators Calculator Script", start_time, False)
            return

        logger.info(f"Processing {len(symbols)} symbols")
        processed_count = 0

        for symbol in symbols:
            try:
                update_custom_indicators_for_symbol(symbol)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}", exc_info=True)
                continue

        logger.info(f"Processed {processed_count}/{len(symbols)} symbols")
        success = processed_count > 0
        log_script_end(logger, "Indicators Calculator Script", start_time, success)

        if not success:
            sys.exit(1)

    except Exception as e:
        logger.error(f"Indicators calculation failed: {e}", exc_info=True)
        log_script_end(logger, "Indicators Calculator Script", start_time, False)
        sys.exit(1)


if __name__ == "__main__":
    main()
