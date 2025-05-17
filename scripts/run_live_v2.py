import os
import yfinance as yf
import asyncio
import sqlite3
import time
from datetime import datetime, timedelta
from telegram import Bot
from dotenv import load_dotenv
from tabulate import tabulate

# Watchlist dictionary for tickers
WATCHLIST = [
    "VIX",
    "TLT",
    "BND",
    "SUI20947-USD",
    "BTC-USD",
    "SGD=X",
    "D05.SI",
    "O39.SI",
    "C6L.SI",
    "Z74.SI",
    "SPY",
    "PHYS",
    "AAPL",
    "NVDA",
    "MSFT",
    "AMZN",
    "META",
    "AVGO",
    "GOOGL",
    "TSLA",
    "GOOG",
    "BRK-B",
    "JPM",
    "LLY",
    "V",
    "UNH",
    "COST",
    "XOM",
    "MA",
    "WMT",
    "NFLX",
    "HD",
    "PG",
    "JNJ",
    "ABBV",
    "CRM",
    "BAC",
    "ORCL",
    "KO",
    "WFC",
    "CVX",
    "CSCO",
    "ACN",
    "PLTR",
    "IBM",
    "PM",
    "GE",
    "ABT",
    "MCD",
    "LIN",
    "MRK",
    "ISRG",
    "TMO",
    "GS",
    "ADBE",
    "NOW",
    "DIS",
    "PEP",
    "QCOM",
    "T",
    "AMD",
    "VZ",
    "AXP",
    "MS",
    "CAT",
    "SPGI",
    "RTX",
    "UBER",
    "BKNG",
    "TXN",
    "INTU",
    "AMGN",
    "BSX",
    "C",
    "UNP",
    "PGR",
    "AMAT",
    "PFE",
    "NEE",
    "LOW",
    "BLK",
    "SCHW",
    "TJX",
    "BA",
    "HON",
    "CMCSA",
    "SYK",
    "DHR",
    "FI",
    "PANW",
    "GILD",
    "SBUX",
    "INTC",
]

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BTD_ID = os.getenv("BTD_ID")
STR_ID = os.getenv("STR_ID")
WATCHLIST_INDICATORS_ID = os.getenv("WATCHLIST_INDICATORS_ID")

# Database setup
db_path = "./data/live_stocks.db"
conn = sqlite3.connect(db_path)
c = conn.cursor()

# Initialize bot
bot = Bot(token=BOT_TOKEN)


# Function to update the latest OHLC and PE ratio for tickers in database
def update_latest_data():
    c.execute("SELECT DISTINCT ticker FROM stock_data")
    tickers = [row[0] for row in c.fetchall()]
    today = datetime.now().date()

    for ticker in tickers:
        try:
            # Get the latest date available in the DB for the ticker
            c.execute("SELECT MAX(date) FROM stock_data WHERE ticker = ?", (ticker,))
            result = c.fetchone()[0]

            if result:
                last_date = datetime.strptime(result, "%Y-%m-%d").date()
                start_date = last_date + timedelta(days=1)
            else:
                # Fallback if no data found (shouldn't happen for known tickers)
                start_date = today - timedelta(days=30)

            if start_date >= today:
                print(f"{ticker} is already up to date.")
                continue

            # Fetch missing data from yfinance
            print(f"Fetching {ticker} data from {start_date} to {today}")
            stock = yf.Ticker(ticker)
            hist = stock.history(
                start=start_date, end=today + timedelta(days=1)
            )  # include today
            if hist.empty:
                print(f"No new data available for {ticker}.")
                continue

            info = stock.info
            pe_ratio = info.get("trailingPE")
            pe_ratio = (
                round(pe_ratio, 2) if isinstance(pe_ratio, (int, float)) else None
            )

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
            time.sleep(30)  # Avoid rate limiting

        except Exception as e:
            print(f"Error updating {ticker}: {e}")


# Function to check if ticker exist in database
def ticker_exists(ticker):
    c.execute("SELECT 1 FROM stock_data WHERE ticker = ? LIMIT 1", (ticker,))
    return c.fetchone() is not None


# Function to fetch and store stock data efficiently
def fetch_and_store_data(ticker):

    if ticker_exists(ticker):
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
        time.sleep(30)  # To avoid rate limits
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")


# Function to calculate BTD22 and STR22 and update the database
def calculate_btd_str():
    c.execute("SELECT DISTINCT ticker FROM stock_data")
    tickers = [row[0] for row in c.fetchall()]

    for ticker in tickers:
	# edited to use 21 day because it is the avg number of days
        c.execute(
            """
            SELECT date, low_price, high_price, close_price FROM stock_data 
            WHERE ticker = ? ORDER BY date DESC LIMIT 21
        """,
            (ticker,),
        )
        results = c.fetchall()

        if len(results) != 21:
            continue  # Skip if not enough data

        lowest_close_22 = min(row[3] for row in results)
        highest_close_22 = max(row[3] for row in results)
        high_price = results[0][2]
        low_price = results[0][1]

        btd22 = round(((high_price - lowest_close_22) / lowest_close_22) * 100, 2)
        str22 = round(((low_price - highest_close_22) / highest_close_22) * 100, 2)

        c.execute(
            """
            UPDATE stock_data SET btd22 = ?, str22 = ?
            WHERE ticker = ? AND date = (SELECT MAX(date) FROM stock_data WHERE ticker = ?)
        """,
            (btd22, str22, ticker, ticker),
        )

    conn.commit()


# Function to generate watchlists async
def generate_watchlist(filter_col, threshold, comparator, title, emoji):
    query = f"SELECT ticker, pe_ratio, {filter_col} FROM stock_data WHERE {filter_col} {comparator} ? ORDER BY date DESC LIMIT 1"
    c.execute(query, (threshold,))
    results = c.fetchall()

    now = datetime.now().strftime("%Y-%m-%d")
    if not results:
        message = f"{emoji} ***{title} ({now})***\nNo available tickers."
    else:
        table = tabulate(
            results,
            headers=["Ticker", "PE Ratio", filter_col],
            tablefmt="plain",
            showindex=False,
        )
        message = f"{emoji} ***{title} ({now})***\n```\n{table}\n```"

    return message


async def send_watchlist():
    msg_btd = generate_watchlist("btd22", 1, "<", "BTD22 Watchlist", "📈")
    msg_str = generate_watchlist("str22", -1, ">", "STR22 Watchlist", "📉")

    await bot.send_message(
        chat_id=CHAT_ID, message_thread_id=BTD_ID, text=msg_btd, parse_mode="Markdown"
    )
    await bot.send_message(
	chat_id=CHAT_ID, message_thread_id=STR_ID, text=msg_str, parse_mode="Markdown"
    )


# Main function
async def main():
    # Call this function at the start of the script execution
    update_latest_data()
    for ticker in WATCHLIST:
        fetch_and_store_data(ticker)

    calculate_btd_str()
    await send_watchlist()


# Run the script
if __name__ == "__main__":
    asyncio.run(main())
