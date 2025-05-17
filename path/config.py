import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BTD_ID = os.getenv("BTD_ID")
STR_ID = os.getenv("STR_ID")
WATCHLIST_INDICATORS_ID = os.getenv("WATCHLIST_INDICATORS_ID")

# Database paths
LIVE_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'live_stocks.db')
TEST_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'test_stocks.db') 