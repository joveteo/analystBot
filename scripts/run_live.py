import os
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta
from polygon import RESTClient
from telegram import Bot
from dotenv import load_dotenv
from tabulate import tabulate

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
def fetch_ohlc_from_polygon(symbol, start_date, end_date):
    days = (end_date - start_date).days + 1
    for n in range(days):
        day = start_date + timedelta(days=n)
        if day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            print(f"Skipping weekend for {symbol} on {day}")
            continue

        date_str = day.strftime("%Y-%m-%d")
        try:
            resp = client.get_daily_open_close_agg(symbol, date_str, adjusted="true")
            if getattr(resp, "status", None) != "OK":
                print(f"No valid data for {symbol} on {date_str}: {resp}")
                time.sleep(13)
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
            print(f"Error fetching {symbol} on {date_str}: {e}")
        time.sleep(13)


# --- FUNCTION: CHECK FOR 22 DAYS OF DATA ---
def ensure_22_days_data(symbol):
    today = datetime.now().date()
    end_date = today - timedelta(days=1)
    c.execute(
        "SELECT date FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT 22",
        (symbol,),
    )
    dates = [datetime.strptime(row[0], "%Y-%m-%d").date() for row in c.fetchall()]
    if len(dates) == 22 and dates[0] == end_date:
        return  # Already have 22 most recent days
    have_dates = set(dates)
    needed_dates = [end_date - timedelta(days=i) for i in range(22)]
    missing_dates = [d for d in needed_dates if d not in have_dates]
    if missing_dates:
        print(
            f"Fetching missing data for {symbol}: {[d.strftime('%Y-%m-%d') for d in missing_dates]}"
        )
        for d in reversed(missing_dates):  # fetch oldest first
            fetch_ohlc_from_polygon(symbol, d, d)


# --- FUNCTION: BTD AND STR CALCULATION ---
def calculate_btd_str(symbol):
    c.execute(
        """
        SELECT date, low_price, high_price, close_price FROM stock_data 
        WHERE symbol = ? ORDER BY date DESC LIMIT 22
        """,
        (symbol,),
    )
    results = c.fetchall()
    if len(results) != 22:
        return None, None
    lowest_close_22 = min(row[3] for row in results)
    highest_close_22 = max(row[3] for row in results)
    high_price = results[0][2]
    low_price = results[0][1]
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
    return btd_22, str_22


# --- FUNCTION: GENERATE BTD WATCHLIST ---
def generate_btd_watchlist(symbols):
    rows = []
    for symbol in symbols:
        btd_22, _ = calculate_btd_str(symbol)
        if btd_22 is not None:
            rows.append((symbol, btd_22))
    now = datetime.now().strftime("%Y-%m-%d")
    if not rows:
        return f"No available symbols for {now}."
    table = tabulate(
        rows, headers=["Symbol", "BTD_22"], tablefmt="plain", showindex=False
    )
    return f"📈 ***BTD Watchlist ({now})***\n```{table}\n```"


# --- FUNCTION: GENERATE STR WATCHLIST ---
def generate_str_watchlist(symbols):
    rows = []
    for symbol in symbols:
        _, str_22 = calculate_btd_str(symbol)
        if str_22 is not None:
            rows.append((symbol, str_22))
    now = datetime.now().strftime("%Y-%m-%d")
    if not rows:
        return f"No available symbols for {now}."
    table = tabulate(
        rows, headers=["Symbol", "STR_22"], tablefmt="plain", showindex=False
    )
    return f"📉 ***STR Watchlist ({now})***\n```{table}\n```"


# --- FUNCTION GENERATE WATHCLIST ---
def generate_watchlist(symbols, column_name, title, emoji):
    rows = []
    for symbol in symbols:
        btd_22, str_22 = calculate_btd_str(symbol)
        value = btd_22 if column_name == "btd_22" else str_22
        if value is not None:
            rows.append((symbol, value))
    now = datetime.now().strftime("%Y-%m-%d")
    if not rows:
        return f"{emoji} ***{title} ({now})***\nNo available symbols."
    table = tabulate(
        rows, headers=["Symbol", column_name.upper()], tablefmt="plain", showindex=False
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
            print(f"Symbol {symbol} not found in DB. Fetching last 31 days...")
            fetch_ohlc_from_polygon(
                symbol,
                datetime.now().date() - timedelta(days=30),
                datetime.now().date(),
            )
        ensure_22_days_data(symbol)
        time.sleep(1)

    bot = Bot(token=BOT_TOKEN)
    await send_watchlist(bot, WATCHLIST)


if __name__ == "__main__":
    asyncio.run(main())
