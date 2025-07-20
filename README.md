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
- - OHLCV data fetching from Polygon.io with NYSE calendar integration  
- SQLite database storage with intelligent missing data detection
- Telegram watchlist messaging with monospace table formatting
- Daily log rotation with automatic cleanup and cross-platform support
- Sequential script orchestration with comprehensive error handling

## Technical Features
- Fetches OHLCV data from Polygon.io with rate limit compliance (5 calls/minute)
- Stores data in SQLite database with INSERT OR REPLACE for data integrity
- Compiles BTD/STR indicators across 22, 66, and 132-period lookbacks
- Sends formatted watchlists daily via Telegram with topic-based routing
- Curated watchlist with 95 top companies across US market sectors
- Runs on Raspberry Pi using Pi OS with cron.services and Git integration

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
5. **Create a `.env` file** in the project root with all required variables:
   ```
   TELEGRAM_BOT_TOKEN=
   TELEGRAM_CHAT_ID=
   TELEGRAM_TEST_CHAT_ID=
   TELEGRAM_CHAT_BTD_ID=
   TELEGRAM_CHAT_STR_ID=
   TELEGRAM_CHAT_MARKET_INDICATORS_ID=
   POLYGON_KEY=
   FRED_API=
   ```
6. **Create database structure** by running:
   ```
   python -c "
   import sqlite3
   from pathlib import Path

   # Create data directory
   Path('./data').mkdir(exist_ok=True)
   
   # Create database and table
   conn = sqlite3.connect('./data/live_stocks.db')
   c = conn.cursor()
   c.execute('''
       CREATE TABLE IF NOT EXISTS stock_data (
           symbol TEXT NOT NULL,
           date TEXT NOT NULL,
           open_price REAL,
           high_price REAL,
           low_price REAL,
           close_price REAL,
           volume INTEGER,
           btd_22 REAL,
           btd_66 REAL,
           btd_132 REAL,
           str_22 REAL,
           str_66 REAL,
           str_132 REAL,
           PRIMARY KEY (symbol, date)
       )
   ''')
   conn.commit()
   conn.close()
   print('Database and table created successfully.')
   "
   ```
7. **Run the complete workflow:**
   ```
   python scripts/main_script.py
   ```

## Directory Structure
```
analystBot/
├── data/
│   └── live_stocks.db              # SQLite database for OHLCV and indicators
├── logs/                           # Daily log files (auto-created)
│   └── analystbot_YYYY-MM-DD.log  # Daily rotating log files
├── scripts/
│   ├── __init__.py
│   ├── logging_config.py           # Centralized logging with daily rotation
│   ├── main_script.py              # Orchestrator for complete workflow
│   ├── update_db.py                # OHLCV data fetching from Polygon.io
│   ├── calculate_indicators.py     # BTD/STR indicator calculations
│   └── send_telegram.py            # Telegram watchlist messaging
├── venv/                           # Virtual environment (created by user)
├── .env                            # Environment variables (created by user)
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt                # Python dependencies
```

## Script Execution Order
1. **main_script.py** - Orchestrates the complete workflow
   - Initializes logging and cleans old log files
   - Validates prerequisites (database, .env file, script dependencies)
   - Executes scripts sequentially with error handling

2. **update_db.py** - Market data collection
   - Fetches OHLCV data from Polygon.io for 95-symbol watchlist
   - Manages rate limits (5 calls/minute) and trading day calculations
   - Stores data in SQLite with missing data detection

3. **calculate_indicators.py** - Technical analysis
   - Calculates BTD/STR indicators using VixFix methodology
   - Processes multiple timeframes (22, 66, 132 periods)
   - Updates database with calculated indicator values

4. **send_telegram.py** - Messaging and alerts
   - Generates formatted BTD/STR watchlists
   - Sends monospace tables to Telegram with topic routing
   - Provides completion summaries and error notifications

## Environment Variables
| Variable | Purpose | Required |
|----------|---------|----------|
| `TELEGRAM_BOT_TOKEN` | Telegram bot authentication | Yes |
| `TELEGRAM_CHAT_ID` | Main chat/channel ID | Yes |
| `TELEGRAM_TEST_CHAT_ID` | Test environment chat ID | Optional |
| `TELEGRAM_CHAT_BTD_ID` | BTD watchlist topic ID | Yes |
| `TELEGRAM_CHAT_STR_ID` | STR watchlist topic ID | Yes |
| `TELEGRAM_CHAT_MARKET_INDICATORS_ID` | Market indicators topic ID | Optional |
| `POLYGON_KEY` | Polygon.io API key for market data | Yes |
| `FRED_API` | FRED API key for economic data | Optional |

## Cron Job Setup (Raspberry Pi)
Add to crontab for daily execution:
```
# Daily analyst bot execution at 8:00 AM
0 8 * * 1-5 cd /path/to/analystBot && /path/to/analystBot/venv/bin/python scripts/main_script.py

# Weekly log cleanup at midnight Sunday
0 0 * * 0 cd /path/to/analystBot && /path/to/analystBot/venv/bin/python -c "from scripts.logging_config import clean_old_logs; clean_old_logs()"
```

## Logging System
- **Daily log files**: `analystbot_YYYY-MM-DD.log` in `logs/` directory
- **Automatic cleanup**: Removes logs older than 30 days
- **Structured format**: Timestamp, script name, level, function, line number, message
- **Console + file output**: Real-time monitoring and persistent logging
- **Cross-platform**: Works on Windows, macOS, Linux, Raspberry Pi OS
