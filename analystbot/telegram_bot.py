from telegram import Bot
from datetime import datetime
from tabulate import tabulate


def send_telegram_message(token, chat_id, text, thread_id=None):
    bot = Bot(token=token)
    if thread_id:
        bot.send_message(chat_id=chat_id, message_thread_id=thread_id, text=text, parse_mode="Markdown")
    else:
        bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")

def format_watchlist_message(results, filter_col, title, emoji):
    now = datetime.now().strftime("%Y-%m-%d")
    if not results:
        message = f"{emoji} ***{title} ({now})***\nNo available tickers."
    else:
        table = tabulate(
            results,
            headers=["Ticker", "PE Ratio", filter_col],
            tablefmt="plain",
            showindex=False,
        )
        message = f"{emoji} ***{title} ({now})***\n```
{table}\n```"
    return message 