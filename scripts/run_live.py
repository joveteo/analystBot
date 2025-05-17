import os
import yfinance as yf
import asyncio
import pandas as pd
import time
from datetime import datetime
from telegram import Bot
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Initialize bot
bot = Bot(token=BOT_TOKEN)

# Market List
SNP500_TICKERS = [
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

            close_prices = hist["Close"]
            high_prices = hist["High"]
            low_prices = hist["Low"]

            if len(close_prices) >= 23:
                lowest_close_22 = close_prices.iloc[-23:-1].min()
                highest_close_22 = close_prices.iloc[-23:-1].max()
                high_price = high_prices.iloc[-1]
                low_price = low_prices.iloc[-1]

                btd22 = round(
                    ((high_price - lowest_close_22) / lowest_close_22) * 100, 2
                )
                str22 = round(
                    ((low_price - highest_close_22) / highest_close_22) * 100, 2
                )

                data.append([ticker, btd22, str22, pe_ratio])
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

    if df_filtered.empty:
        message = f"{emoji} ***{title} ({now})***\nNo available tickers."
    else:
        table = tabulate(
            df_filtered[["Ticker", filter_col, "PE Ratio"]],
            headers=["Ticker", filter_col, "PE Ratio"],
            tablefmt="plain",
            showindex=False,
        )
        message = f"{emoji} ***{title} ({now})***\n{description}\n```\n{table}\n```"

    # Handle Telegram character limit (4096 chars)
    if len(message) > 4000:
        parts = [message[i : i + 4000] for i in range(0, len(message), 4000)]
        for part in parts:
            await bot.send_message(chat_id=CHAT_ID, text=part, parse_mode="Markdown")
    else:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")


# Main function
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
