import sqlite3


def create_database():
    conn = sqlite3.connect("./data/live_stocks.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            volume REAL,
            btd_22 REAL,
            str_22 REAL,
            PRIMARY KEY (symbol, date)
        )
    """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_database()
