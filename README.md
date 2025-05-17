# analystBot

A Python bot for fetching end-of-day OHLC prices, storing them, and calculating 'buy the dip' and 'sell the rip' signals. Integrates with Telegram for notifications.

## Features
- Fetches OHLC data using Yahoo Finance
- Stores data in SQLite databases
- Implements 'buy the dip' and 'sell the rip' strategies
- Sends alerts via Telegram
- Separate scripts for live and test environments

## Setup

1. **Clone the repository**
2. **Create and activate a virtual environment:**
   ```
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Mac/Linux:
   source venv/bin/activate
   ```
3. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```
4. **Create a `.env` file** in the project root with your Telegram and other secrets:
   ```
   BOT_TOKEN=your_telegram_bot_token
   CHAT_ID=your_telegram_chat_id
   BTD_ID=...
   STR_ID=...
   WATCHLIST_INDICATORS_ID=...
   TEST_CHAT_ID=...
   ```
5. **Run the bot:**
   ```
   python scripts/run_live.py   # For live/production
   python scripts/run_test.py   # For testing
   ```

## Directory Structure
```
analystBot/
├── analystbot/      # Core package
├── scripts/         # Entry points
├── tests/           # Test scripts/data
├── data/            # Databases
├── requirements.txt
├── .gitignore
├── README.md
└── .env             # Not committed
``` 