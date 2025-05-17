import yfinance as yf
import sqlite3
import time
import logging
from datetime import datetime, timedelta
from typing import List, Tuple
from .config import LIVE_DB_PATH, TEST_DB_PATH

logging.basicConfig(level=logging.INFO)

def get_db_connection(live: bool = True) -> sqlite3.Connection:
    """Get a database connection to the live or test database."""
    db_path = LIVE_DB_PATH if live else TEST_DB_PATH
    return sqlite3.connect(db_path)

def ticker_exists(conn: sqlite3.Connection, ticker: str) -> bool:
    """Check if a ticker exists in the database."""
    c = conn.cursor()
    c.execute("SELECT 1 FROM stock_data WHERE ticker = ? LIMIT 1", (ticker,))
    return c.fetchone() is not None

def fetch_and_store_data(conn: sqlite3.Connection, ticker: str) -> None:
    """Fetch and store OHLC and PE ratio data for a ticker."""
    c = conn.cursor()
    if ticker_exists(conn, ticker):
        logging.info(f"{ticker} exists in database, using stored values.")
        return
    try:
        stock = yf.Ticker(ticker)
        logging.info(f"Fetching 30 days of data for {ticker}")
        hist = stock.history(period="30d")
        info = stock.info
        pe_ratio = info.get("trailingPE")
        pe_ratio = round(pe_ratio, 2) if isinstance(pe_ratio, (int, float)) else None
        if hist.empty:
            logging.warning(f"No new data from Yahoo Finance available for {ticker}.")
            return
        for date, row in hist.iterrows():
            c.execute(
                """
                INSERT OR REPLACE INTO stock_data (ticker, date, open_price, high_price, low_price, close_price, pe_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    date.strftime("%Y-%m-%d"),
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    pe_ratio,
                ),
            )
        conn.commit()
        logging.info(f"Updated {ticker} data into database.")
        time.sleep(5)  # To avoid rate limits
    except Exception as e:
        logging.error(f"Error fetching {ticker}: {e}")

def update_latest_data(conn: sqlite3.Connection) -> None:
    """Update the latest OHLC and PE ratio for all tickers in the database."""
    c = conn.cursor()
    c.execute("SELECT DISTINCT ticker FROM stock_data")
    tickers = [row[0] for row in c.fetchall()]
    today = datetime.now().date()
    for ticker in tickers:
        try:
            c.execute("SELECT MAX(date) FROM stock_data WHERE ticker = ?", (ticker,))
            result = c.fetchone()[0]
            if result:
                last_date = datetime.strptime(result, "%Y-%m-%d").date()
                start_date = last_date + timedelta(days=1)
            else:
                start_date = today - timedelta(days=30)
            if start_date >= today:
                logging.info(f"{ticker} is already up to date.")
                continue
            logging.info(f"Fetching {ticker} data from {start_date} to {today}")
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=today + timedelta(days=1))
            if hist.empty:
                logging.warning(f"No new data available for {ticker}.")
                continue
            info = stock.info
            pe_ratio = info.get("trailingPE")
            pe_ratio = round(pe_ratio, 2) if isinstance(pe_ratio, (int, float)) else None
            for date, row in hist.iterrows():
                c.execute(
                    """
                    INSERT OR REPLACE INTO stock_data (ticker, date, open_price, high_price, low_price, close_price, pe_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker,
                        date.strftime("%Y-%m-%d"),
                        row["Open"],
                        row["High"],
                        row["Low"],
                        row["Close"],
                        pe_ratio,
                    ),
                )
            conn.commit()
            logging.info(f"{ticker} updated from {start_date} to {today}.")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error updating {ticker}: {e}") 