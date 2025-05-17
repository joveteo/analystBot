import yfinance as yf
import sqlite3
import time
from datetime import datetime, timedelta
from .config import LIVE_DB_PATH, TEST_DB_PATH


def get_db_connection(live=True):
    db_path = LIVE_DB_PATH if live else TEST_DB_PATH
    return sqlite3.connect(db_path)


def ticker_exists(conn, ticker):
    c = conn.cursor()
    c.execute("SELECT 1 FROM stock_data WHERE ticker = ? LIMIT 1", (ticker,))
    return c.fetchone() is not None


def fetch_and_store_data(conn, ticker):
    c = conn.cursor()
    if ticker_exists(conn, ticker):
        print(f"{ticker} exists in database, using stored values.")
        return
    try:
        stock = yf.Ticker(ticker)
        print(f"Fetching 30 days of data for {ticker}")
        hist = stock.history(period="30d")
        info = stock.info
        pe_ratio = info.get("trailingPE")
        pe_ratio = round(pe_ratio, 2) if isinstance(pe_ratio, (int, float)) else None
        if hist.empty:
            print(f"No new data from Yahoo Finance available for {ticker}.")
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
        print(f"Updated {ticker} data into database.")
        time.sleep(5)  # To avoid rate limits
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")


def update_latest_data(conn):
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
                print(f"{ticker} is already up to date.")
                continue
            print(f"Fetching {ticker} data from {start_date} to {today}")
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=today + timedelta(days=1))
            if hist.empty:
                print(f"No new data available for {ticker}.")
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
            print(f"{ticker} updated from {start_date} to {today}.")
            time.sleep(5)
        except Exception as e:
            print(f"Error updating {ticker}: {e}") 