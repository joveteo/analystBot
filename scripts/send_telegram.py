#!/usr/bin/env python3
"""
version 3.1.0
Telegram Messaging Script

Key Features:
- BTD/STR watchlist generation with contrarian signal filtering (BTD_22 < 0, STR_22 > 0)
- Monospace table formatting with precise column alignment and spacing
- Topic-based message routing to separate BTD and STR Telegram channels
- Environment variable validation with detailed credential status logging
- Message length management with automatic chunking for Telegram limits
- Error notification system with automatic failure reporting to Telegram

Generates formatted BTD and STR watchlists from database indicators and sends to Telegram.
Creates clean, monospace tables showing symbol, price, and multi-timeframe signals.
"""

import os
import sys
import sqlite3
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
import pandas as pd

# Removed PrettyTable import - using manual formatting for better control

# Import centralized logging
from logging_config import setup_logger, log_script_start, log_script_end

# Setup logging
logger = setup_logger("send_telegram")

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"
ENV_PATH = PROJECT_ROOT / ".env"

# Load environment variables with debugging
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    logger.info(f"Loaded environment variables from: {ENV_PATH}")
else:
    logger.warning(f"Environment file not found: {ENV_PATH}")
    logger.info("Attempting to load from default locations...")
    load_dotenv()  # Try loading from current directory or default locations

# Telegram configuration with debugging
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TEST_CHAT_ID = os.getenv("TELEGRAM_TEST_CHAT_ID")
BTD_TOPIC_ID = os.getenv("TELEGRAM_CHAT_BTD_ID")
STR_TOPIC_ID = os.getenv("TELEGRAM_CHAT_STR_ID")
MARKET_INDICATORS_TOPIC_ID = os.getenv("TELEGRAM_CHAT_MARKET_INDICATORS_ID")

logger.info("Environment variables:")
logger.info(f"  BOT_TOKEN: {'✓' if TELEGRAM_BOT_TOKEN else '✗'}")
logger.info(f"  CHAT_ID: {'✓' if TELEGRAM_CHAT_ID else '✗'}")
logger.info(f"  TEST_CHAT_ID: {'✓' if TELEGRAM_TEST_CHAT_ID else '✗'}")
logger.info(f"  BTD_ID: {'✓' if BTD_TOPIC_ID else '✗'}")
logger.info(f"  STR_ID: {'✓' if STR_TOPIC_ID else '✗'}")
logger.info(f"  MARKET_INDICATORS_ID: {'✓' if MARKET_INDICATORS_TOPIC_ID else '✗'}")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("Missing Telegram credentials")
    logger.error("Required in .env file:")
    logger.error("  TELEGRAM_BOT_TOKEN=your_bot_token")
    logger.error("  TELEGRAM_CHAT_ID=your_chat_id")
    logger.error("  TELEGRAM_TEST_CHAT_ID=your_test_chat_id")
    logger.error("  TELEGRAM_CHAT_BTD_ID=your_btd_topic_id")
    logger.error("  TELEGRAM_CHAT_STR_ID=your_str_topic_id")
    logger.error("  TELEGRAM_CHAT_MARKET_INDICATORS_ID=your_market_indicators_topic_id")

MAX_MESSAGE_LENGTH = 4000


def send_telegram_message(
    message: str, topic_id: Optional[str] = None, parse_mode: str = "Markdown"
):
    """Send message to Telegram with optional topic threading.

    Essential Features:
    - Telegram Bot API integration with credential validation
    - Topic ID support for threaded channel messaging
    - HTTP error handling with detailed exception logging
    - Markdown parsing support for formatted messages
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Missing Telegram credentials")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}

    if topic_id:
        payload["message_thread_id"] = topic_id

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Message sent successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send message: {e}")
        return False


def split_long_message(message: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """Split message into chunks if exceeds Telegram limits.

    Essential Features:
    - 4000-character limit compliance with Telegram API restrictions
    - Line-based splitting to preserve table formatting integrity
    - Returns list of chunks for sequential message sending
    """
    if len(message) <= max_length:
        return [message]

    chunks = []
    current_chunk = ""
    lines = message.split("\n")

    for line in lines:
        if len(current_chunk) + len(line) + 1 <= max_length:
            current_chunk += line + "\n"
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


def get_btd_data() -> List[Dict]:
    """Retrieve BTD signals where BTD_22 < 0 (buy opportunities).

    Essential Features:
    - Contrarian signal filtering for oversold conditions (BTD_22 < 0)
    - Latest date data retrieval using MAX(date) subquery
    - Multi-timeframe BTD values (22, 66, 132) with current price
    - Ordered results by BTD_22 for priority ranking
    """
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT symbol, btd_22, btd_66, btd_132, close_price
        FROM stock_data s1
        WHERE s1.date = (SELECT MAX(date) FROM stock_data s2 WHERE s2.symbol = s1.symbol)
        AND btd_22 < 0
        AND btd_22 IS NOT NULL
        ORDER BY btd_22 ASC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df.to_dict("records") if not df.empty else []


def generate_btd_watchlist(btd_data: List[Dict]) -> str:
    """Format BTD data into clean monospace table.

    Essential Features:
    - Monospace table generation with precise column alignment
    - Right-justified number formatting for price and indicator values
    - Markdown code block wrapping for Telegram rendering
    - Header row with consistent spacing and column labels
    - Date stamp integration in title for reference tracking
    """
    now = datetime.now().strftime("%Y-%m-%d")

    if not btd_data:
        return f"📈 ***BTD Watchlist ({now})***\nNo signals."

    # Mobile-optimized header with left-aligned symbol column
    header = f"{'Symbol':<5} {'Last':>6} {'B22':>5} {'B66':>5} {'B132':>5}"
    lines = [header]

    for data in btd_data:
        symbol = data["symbol"][:5]  # Truncate long symbols for mobile
        last_price = data["close_price"] or 0
        btd_22 = data["btd_22"] or 0
        btd_66 = data["btd_66"] or 0
        btd_132 = data["btd_132"] or 0

        line = f"{symbol:<5} {last_price:>6.2f} {btd_22:>5.2f} {btd_66:>5.2f} {btd_132:>5.2f}"
        lines.append(line)

    formatted_table = "\n".join(lines)
    return f"📈 ***BTD Watchlist ({now})***\n```\n{formatted_table}\n```"


def send_btd_watchlist(btd_data: List[Dict]):
    """Send BTD watchlist to Telegram.

    Essential Features:
    - Console output for local monitoring and debugging
    - Topic-specific routing using BTD_TOPIC_ID for channel organization
    - Symbol count logging for processing confirmation
    """
    logger.info(f"Sending BTD watchlist ({len(btd_data)} symbols)")
    watchlist_message = generate_btd_watchlist(btd_data)
    print(watchlist_message)
    send_telegram_message(watchlist_message, topic_id=BTD_TOPIC_ID)


def get_str_data() -> List[Dict]:
    """Retrieve STR signals where STR_22 > 0 (short opportunities).

    Essential Features:
    - Contrarian signal filtering for overbought conditions (STR_22 > 0)
    - Latest date data retrieval using MAX(date) subquery
    - Multi-timeframe STR values (22, 66, 132) with current price
    - Ordered results by STR_22 for priority ranking
    """
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT symbol, str_22, str_66, str_132, close_price
        FROM stock_data s1
        WHERE s1.date = (SELECT MAX(date) FROM stock_data s2 WHERE s2.symbol = s1.symbol)
        AND str_22 > 0
        AND str_22 IS NOT NULL
        ORDER BY str_22 DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df.to_dict("records") if not df.empty else []


def generate_str_watchlist(str_data: List[Dict]) -> str:
    """Format STR data into clean monospace table.

    Essential Features:
    - Monospace table generation with precise column alignment
    - Right-justified number formatting for price and indicator values
    - Markdown code block wrapping for Telegram rendering
    - Header row with consistent spacing and column labels
    - Date stamp integration in title for reference tracking
    """
    now = datetime.now().strftime("%Y-%m-%d")

    if not str_data:
        return f"📉 ***STR Watchlist ({now})***\nNo signals."

    # Mobile-optimized header with left-aligned symbol column
    header = f"{'Symbol':<5} {'Last':>6} {'S22':>5} {'S66':>5} {'S132':>5}"
    lines = [header]

    for data in str_data:
        symbol = data["symbol"][:5]  # Truncate long symbols for mobile
        last_price = data["close_price"] or 0
        str_22 = data["str_22"] or 0
        str_66 = data["str_66"] or 0
        str_132 = data["str_132"] or 0

        line = f"{symbol:<5} {last_price:>6.2f} {str_22:>5.2f} {str_66:>5.2f} {str_132:>5.2f}"
        lines.append(line)

    formatted_table = "\n".join(lines)
    return f"📉 ***STR Watchlist ({now})***\n```\n{formatted_table}\n```"


def send_str_watchlist(str_data: List[Dict]):
    """Send STR watchlist to Telegram.

    Essential Features:
    - Console output for local monitoring and debugging
    - Topic-specific routing using STR_TOPIC_ID for channel organization
    - Symbol count logging for processing confirmation
    """
    logger.info(f"Sending STR watchlist ({len(str_data)} symbols)")
    watchlist_message = generate_str_watchlist(str_data)
    print(watchlist_message)
    send_telegram_message(watchlist_message, topic_id=STR_TOPIC_ID)


def main():
    """Execute Telegram messaging workflow.

    Essential Features:
    - Sequential BTD and STR watchlist generation and transmission
    - Comprehensive error handling with automatic error notification to Telegram
    - Script timing and completion status tracking
    - Exception logging with stack trace capture for debugging
    """
    start_time = datetime.now()
    log_script_start(logger, "Telegram Messaging Script")

    try:
        logger.info("Generating BTD watchlist")
        btd_data = get_btd_data()
        send_btd_watchlist(btd_data)

        logger.info("Generating STR watchlist")
        str_data = get_str_data()
        send_str_watchlist(str_data)

        logger.info("✅ Telegram messaging completed")
        log_script_end(logger, "Telegram Messaging Script", start_time, True)

    except Exception as e:
        logger.error(f"Telegram messaging failed: {e}", exc_info=True)

        try:
            error_message = f"🚨 **Messaging Error**\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n❌ {str(e)}"
            send_telegram_message(error_message)
        except:
            logger.error("Failed to send error notification")

        log_script_end(logger, "Telegram Messaging Script", start_time, False)
        sys.exit(1)


if __name__ == "__main__":
    main()
