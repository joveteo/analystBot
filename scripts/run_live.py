import os
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta
from polygon import RESTClient
from telegram import Bot
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
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
    "ADP",
    "TMUS",
    "ETN",
    "DE",
    "MDT",
    "VRTX",
    "BX",
    "BMY",
    "ANET",
    "MMC",
    "PLD",
    "LRCX",
    "GEV",
    "ADI",
    "CRWD",
    "KLAC",
    "CB",
    "CEG",
    "INTC",
]


# Function to fetch stock data
def fetch_stock_data(tickers):
    data = []
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="30d")
            info = stock.info
            pe_ratio = info.get("trailingPE")
            pe_ratio = (
                f"{round(pe_ratio, 2):.2f}"
                if isinstance(pe_ratio, (int, float))
                else "N/A"
            )

            if hist.empty:
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
            error_message = f"Error fetching data for {ticker}: {e}\n"
            with open("log.txt", "a") as log_file:
                log_file.write(error_message)
            print(error_message)
        time.sleep(5)

    df = pd.DataFrame(
        data, columns=["Ticker", "BTD22", "STR22", "PE Ratio"]
    )
    return df


# Function to format and send messages
async def send_watchlist(title, description, df, filter_col, threshold, emoji):
    df_filtered = (
        df[df[filter_col] < threshold]
        if filter_col == "BTD22"
        else df[df[filter_col] > threshold]
    )

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
    df = fetch_stock_data(SNP500_TICKERS)
    await send_watchlist(
        "BTD22 Watchlist",
        "BTD22 under 0 signals price is relatively low compared to the past 22 days.",
        df,
        "BTD22",
        1,
        "📈",
    )
    await send_watchlist(
        "STR22 Watchlist",
        "STR22 above 0 signals price is relatively high compared to the past 22 days.",
        df,
        "STR22",
        0,
        "📉",
    )


# Run the script
if __name__ == "__main__":
    asyncio.run(main())
