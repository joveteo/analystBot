#!/usr/bin/env python3
"""
version 3.0.0
Telegram Messaging Script
Retrieves data from database, generate watchlists and sends to Telegram
Structure: 1 function to get data + 1 function to send data for each metric type
"""

import os
import sys
import sqlite3
import logging
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

# Setup simple console logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "live_stocks.db"
ENV_PATH = PROJECT_ROOT / ".env"

# Load environment variables
load_dotenv(ENV_PATH)

# Telegram configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BTD_TOPIC_ID = os.getenv("BTD_ID")  # Topic ID for BTD messages
STR_TOPIC_ID = os.getenv("STR_ID")  # Topic ID for STR messages

# Message formatting
MAX_MESSAGE_LENGTH = 4000  # Telegram limit is 4096


def send_telegram_message(
    message: str, topic_id: str = None, parse_mode: str = "Markdown"
):
    """Send a message to Telegram with optional topic ID"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not found in environment variables")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}

    # Add topic ID if provided
    if topic_id:
        payload["message_thread_id"] = topic_id

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")
        return False


def split_long_message(message: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """Split long messages into chunks"""
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


# Gets BTD data from database, generates watchlist message and sends to Telegram
def get_btd_data() -> List[Dict]:
    """Get BTD data from database where BTD_22 < 0 (contrarian signal)"""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT symbol, btd_22, btd_66, btd_132, close_price
        FROM stock_data s1
        WHERE s1.date = (SELECT MAX(date) FROM stock_data s2 WHERE s2.symbol = s1.symbol)
        AND btd_22 < 0
        AND btd_22 IS NOT NULL
        ORDER BY btd_22 ASC
    """

    # Import pandas here to avoid import issues
    import pandas as pd

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df.to_dict("records") if not df.empty else []


def generate_btd_watchlist(btd_data: List[Dict]) -> str:
    """Generate BTD watchlist message with multi-timeframe data"""
    now = datetime.now().strftime("%Y-%m-%d")

    if not btd_data:
        return f"📈 ***BTD Watchlist ({now})***\nNo available symbols."

    # Create table data with all timeframes
    rows = []
    for data in btd_data:
        symbol = data["symbol"]
        last_price = data["close_price"] or 0
        btd_22 = data["btd_22"] or 0
        btd_66 = data["btd_66"] or 0
        btd_132 = data["btd_132"] or 0
        rows.append(
            [
                symbol,
                f"{last_price:.2f}",
                f"{btd_22:.2f}",
                f"{btd_66:.2f}",
                f"{btd_132:.2f}",
            ]
        )

    # Format as simple table
    table_lines = ["Symbol\tLast\tBTD22\tBTD66\tBTD132"]
    for row in rows:
        table_lines.append(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")

    table = "\n".join(table_lines)

    return f"📈 ***BTD Watchlist ({now})***\n```\n{table}\n```"


def send_btd_watchlist(btd_data: List[Dict]):
    """Send BTD watchlist to Telegram"""
    logger.info(f"Sending BTD watchlist with {len(btd_data)} symbols...")

    watchlist_message = generate_btd_watchlist(btd_data)
    print(watchlist_message)
    send_telegram_message(watchlist_message, topic_id=BTD_TOPIC_ID)


# Gets STR data from database, generates watchlist message and sends to Telegram
def get_str_data() -> List[Dict]:
    """Get STR data from database where STR_22 > 0 (contrarian signal)"""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT symbol, str_22, str_66, str_132, close_price
        FROM stock_data s1
        WHERE s1.date = (SELECT MAX(date) FROM stock_data s2 WHERE s2.symbol = s1.symbol)
        AND str_22 > 0
        AND str_22 IS NOT NULL
        ORDER BY str_22 DESC
    """

    # Import pandas here to avoid import issues
    import pandas as pd

    df = pd.read_sql_query(query, conn)
    conn.close()

    return df.to_dict("records") if not df.empty else []


def generate_str_watchlist(str_data: List[Dict]) -> str:
    """Generate STR watchlist message with multi-timeframe data"""
    now = datetime.now().strftime("%Y-%m-%d")

    if not str_data:
        return f"📉 ***STR Watchlist ({now})***\nNo available symbols."

    # Create table data with all timeframes
    rows = []
    for data in str_data:
        symbol = data["symbol"]
        last_price = data["close_price"] or 0
        str_22 = data["str_22"] or 0
        str_66 = data["str_66"] or 0
        str_132 = data["str_132"] or 0
        rows.append(
            [
                symbol,
                f"{last_price:.2f}",
                f"{str_22:.2f}",
                f"{str_66:.2f}",
                f"{str_132:.2f}",
            ]
        )

    # Format as simple table
    table_lines = ["Symbol\tLast\tSTR22\tSTR66\tSTR132"]
    for row in rows:
        table_lines.append(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")

    table = "\n".join(table_lines)

    return f"📉 ***STR Watchlist ({now})***\n```\n{table}\n```"


def send_str_watchlist(str_data: List[Dict]):
    """Send STR watchlist to Telegram"""
    logger.info(f"Sending STR watchlist with {len(str_data)} symbols...")

    watchlist_message = generate_str_watchlist(str_data)
    print(watchlist_message)
    send_telegram_message(watchlist_message, topic_id=STR_TOPIC_ID)


def main():
    """Main function to send BTD and STR watchlists"""
    logger.info("Starting Telegram message sending...")

    start_time = datetime.now()

    try:
        # Send BTD watchlist
        logger.info("Sending BTD watchlist...")
        btd_data = get_btd_data()
        send_btd_watchlist(btd_data)

        # Send STR watchlist
        logger.info("Sending STR watchlist...")
        str_data = get_str_data()
        send_str_watchlist(str_data)

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info(f"Telegram messaging completed in {duration}")

    except Exception as e:
        logger.error(f"Error during Telegram messaging: {e}")
        # Send error message to Telegram
        error_message = f"🚨 **Error in Telegram Bot**\n"
        error_message += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        error_message += f"❌ Error: {str(e)}\n"
        error_message += f"🔧 Please check the logs for details"
        send_telegram_message(error_message)
        sys.exit(1)


if __name__ == "__main__":
    main()
