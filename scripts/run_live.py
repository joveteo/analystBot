import os
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta
from polygon import RESTClient
from telegram import Bot
from dotenv import load_dotenv
from tabulate import tabulate
import logging
import pandas_market_calendars as mcal

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- CONFIGURATION ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BTD_ID = os.getenv("BTD_ID")
STR_ID = os.getenv("STR_ID")
POLYGON_KEY = os.getenv("POLYGON_KEY")
DB_PATH = "./data/live_stocks.db"
WATCHLIST = [
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
    "BRK-B",
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
    "BSK",
    "SPG",
    "O",
    "XOM",
    "CVX",
    "OXY",
    "COP",
    "LIN",
    "SHW",
]

# --- DEFINATION: DATABASE PATH ---
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# --- DEFINATION: POLYGON REST CLIENT KEY ---
client = RESTClient(POLYGON_KEY)


# --- FUNCTION: CHECK IF SYMBOL EXISTS IN DB ---
def symbol_exists(symbol):
    c.execute("SELECT 1 FROM stock_data WHERE symbol = ? LIMIT 1", (symbol,))
    return c.fetchone() is not None


# --- FUNCTION: GET LATEST DATE FROM DB FOR A SYMBOL ---
def get_latest_db_date(symbol):
    c.execute("SELECT MAX(date) FROM stock_data WHERE symbol = ?", (symbol,))
    result = c.fetchone()[0]
    return result


# --- FUNCTION: GET LAST 22 DATES FROM DB ---
def get_db_dates(symbol, limit=22):
    c.execute(
        "SELECT date FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT ?",
        (symbol, limit),
    )
    return [row[0] for row in c.fetchall()]


# --- FUNCTION: FETCH OHLCV DATA FROM POLYGON ---
def fetch_data_from_polygon(symbol, start_date, end_date):
    nyse = mcal.get_calendar("NYSE")
    trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)
    for day in trading_days:
        date_str = day.strftime("%Y-%m-%d")
        try:
            resp = client.get_daily_open_close_agg(symbol, date_str, adjusted="true")
            if getattr(resp, "status", None) != "OK":
                logging.warning(f"No valid data for {symbol} on {date_str}: {resp}")
                continue

            c.execute(
                """
                INSERT OR REPLACE INTO stock_data (symbol, date, open_price, high_price, low_price, close_price, volume)
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
        except Exception as e:
            logging.error(f"Error fetching {symbol} on {date_str}: {e}")
        time.sleep(13)


# --- FUNCTION: CHECK FOR 22 DAYS OF DATA ---
def ensure_22_days_data(symbol):
    nyse = mcal.get_calendar("NYSE")
    today = datetime.now().date()
    end_date = today - timedelta(days=1)

    # Get all trading days up to the end date
    all_trading_days = nyse.valid_days(
        start_date=end_date - timedelta(days=60), end_date=end_date
    )

    # Select the last 22 trading days
    trading_days = all_trading_days[-22:]

    # Fetch dates from the database
    c.execute(
        "SELECT date FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT 22",
        (symbol,),
    )
    db_dates = {datetime.strptime(row[0], "%Y-%m-%d").date() for row in c.fetchall()}

    # Determine missing trading days
    missing_dates = [day for day in trading_days if day.date() not in db_dates]

    if missing_dates:
        print(
            f"Fetching missing data for {symbol}: {[d.strftime('%Y-%m-%d') for d in missing_dates]}"
        )
        for d in reversed(missing_dates):  # fetch oldest first
            fetch_data_from_polygon(symbol, d.date(), d.date())


# --- FUNCTION: BTD AND STR CALCULATION ---
def calculate_btd_str(symbol):
    c.execute(
        """
        SELECT date, low_price, high_price, close_price FROM stock_data 
        WHERE symbol = ? ORDER BY date DESC LIMIT 23
        """,
        (symbol,),
    )
    results = c.fetchall()
    if len(results) != 23:
        return None, None, None
    lowest_close_22 = min(row[3] for row in results[1:])  # Exclude the current day
    highest_close_22 = max(row[3] for row in results[1:])  # Exclude the current day
    high_price = results[0][2]
    low_price = results[0][1]
    last_price = results[0][3]  # Assuming the last price is the most recent close price
    btd_22 = round(((high_price - lowest_close_22) / lowest_close_22) * 100, 2)
    str_22 = round(((low_price - highest_close_22) / highest_close_22) * 100, 2)
    c.execute(
        """
        UPDATE stock_data SET btd_22 = ?, str_22 = ?
        WHERE symbol = ? AND date = (SELECT MAX(date) FROM stock_data WHERE symbol = ?)
        """,
        (btd_22, str_22, symbol, symbol),
    )
    conn.commit()
    return btd_22, str_22, last_price


# --- FUNCTION GENERATE WATHCLIST ---
# add filter < 1 and > -1
def generate_watchlist(symbols, column_name, title, emoji):
    rows = []
    for symbol in symbols:
        btd_22, str_22, last_price = calculate_btd_str(symbol)
        value = btd_22 if column_name == "btd_22" else str_22
        if value is not None:
            if (column_name == "btd_22" and value < 1) or (
                column_name == "str_22" and value > -1
            ):
                rows.append((symbol, last_price, value))
    now = datetime.now().strftime("%Y-%m-%d")
    if not rows:
        return f"{emoji} ***{title} ({now})***\nNo available symbols."
    table = tabulate(
        rows,
        headers=["Symbol", "Last", column_name.upper()],
        tablefmt="tsv",
        showindex=False,
        numalign="center",
        stralign="center",
        floatfmt=".2f",
    )
    return f"{emoji} ***{title} ({now})***\n```{table}\n```"


# --- FUNCTION: SEND TELEGRAM MESSAGE ---
async def send_watchlist(bot, symbols):
    btd_msg = generate_watchlist(symbols, "btd_22", "BTD Watchlist", "📈")
    str_msg = generate_watchlist(symbols, "str_22", "STR Watchlist", "📉")
    await bot.send_message(
        chat_id=CHAT_ID, message_thread_id=BTD_ID, text=btd_msg, parse_mode="Markdown"
    )
    await bot.send_message(
        chat_id=CHAT_ID, message_thread_id=STR_ID, text=str_msg, parse_mode="Markdown"
    )


# --- MAIN WORKFLOW ---
async def main():
    for symbol in WATCHLIST:
        if not symbol_exists(symbol):
            logging.info(f"Symbol {symbol} not found in DB. Fetching last 31 days data")
            fetch_data_from_polygon(
                symbol,
                datetime.now().date() - timedelta(days=30),
                datetime.now().date(),
            )
        ensure_22_days_data(symbol)

    bot = Bot(token=BOT_TOKEN)
    await send_watchlist(bot, WATCHLIST)


if __name__ == "__main__":
    asyncio.run(main())
