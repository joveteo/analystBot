# analystBot
A bot that runs on python that pushes key financial information to users on Telegram so that they can take action.

## To-Do List
- New data available from polygon.io (crypto, currency trading pairs, maybe earnings tip?)
- Update messages with proper descriptions, follow-up actions and resources for users in the telegram group
- Fix issue with running scripts on Pi OS: cannot access the environment in the parent directory and the database in another data folder under the parent directory. had to hard code for now, but need a proper relative reference or maybe an output error
- Set up a git pull script that auto-pulls the origin main from GitHub daily at 7 am, before the script runs at 8 am (git_status runs at midnight daily)
- Add a version number to the telegram message -> tie with the bash script to auto pull from GitHub on pi os
- Update calculate_indicators to include more indicators using OHLCV calculation, and update indicators to DB (future proofing)
- Generate charts to telegram
- Backtest strategies

## Bot Features
- Multi timeframe buy and sell tip derived from Larry Williams' VixFix, originally from useThinkScript. The source for this calculation can be found at https://www.ireallytrade.com/newsletters/VIXFix.pdf.

## Technical Features
- Fetches data from Polygon.io
- Stores data in a SQLite database
- Compiles values within specified parameters and sends tips daily via Telegram
- Curated the watchlist with the top 2 to 3 companies of each industry in the US market
- Runs Rasberry Pi using Pi OS, running cron.services and Git

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
