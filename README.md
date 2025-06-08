# analystBot

A Python script that calculates if a stock is overvalued or undervalued based on the past 22 days of OHLC price. This value was derived from Larry Williams' VixFix, originally from useThinkScript. The source for this calculation can be found at https://www.ireallytrade.com/newsletters/VIXFix.pdf. 
Values crossing the 0 mark represent overextension, which may be a reversal signal. This signal works best with due diligence. If the overextension, underlying fundamentals, and economic circumstances do not justify the move, it might indicate a reversion to a fair value. 

## Features
- Fetches OHLC data using Polygon.io
- Stores data in a SQLite database for efficient retrieval
- Compiles values within specified parameters and sends alerts daily via Telegram (using crontab)
- Curated the watchlist with the top 2 to 3 companies of each industry in the US market

## Setup

1. **Ensure SQLite is installed**
2. **Clone the repository**
3. **Create and activate a virtual environment:**
   ```
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Mac/Linux:
   source venv/bin/activate
   ```
4. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```
5. **Create a `.env` file** in the project root:
   ```
   BOT_TOKEN=your_telegram_bot_token
   CHAT_ID=your_telegram_chat_id
   BTD_ID=your_telegram_chat_topic_id
   STR_ID=your_telegram_chat_topic_id
   TEST_CHAT_ID=...
   ```
6. **Create a `live_stocks.db` file** under data/:
   ```
   import sqlite3

   DB_PATH = "./data/live_stocks.db"


   def create_table():
      conn = sqlite3.connect(DB_PATH)
      c = conn.cursor()
      c.execute(
         """
         CREATE TABLE IF NOT EXISTS stock_data (
               symbol TEXT NOT NULL,
               date TEXT NOT NULL,
               open_price REAL,
               high_price REAL,
               low_price REAL,
               close_price REAL,
               volume INTEGER,
               btd_22 REAL,
               str_22 REAL,
               PRIMARY KEY (symbol, date)
         )
      """
      )
      conn.commit()
      conn.close()


   if __name__ == "__main__":
      create_table()
      print("Database and table created successfully.")

   ```
7. **Replace send_message with print if telegram is not connected:**
   ```
    # await bot.send_message(
    #     chat_id=CHAT_ID, message_thread_id=BTD_ID, text=btd_msg, parse_mode="Markdown"
    # )
    # await bot.send_message(
    #     chat_id=CHAT_ID, message_thread_id=STR_ID, text=str_msg, parse_mode="Markdown"
    # )
    print(btd_msg)
    print(str_msg)
   ```
8. **Run the bot:**
   ```
   python scripts/run_live.py
   ```

## Directory Structure
```
analystBot/
  data/
  scripts/
  .env
  .gitignore
  README.md
  requirements.txt
```
