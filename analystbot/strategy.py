def calculate_btd_str_from_prices(prices):
    """
    prices: list of tuples (date, low_price, high_price, close_price)
    Returns: (btd22, str22) or (None, None) if not enough data
    """
    if len(prices) < 21:
        return None, None
    lowest_close_22 = min(row[3] for row in prices)
    highest_close_22 = max(row[3] for row in prices)
    high_price = prices[0][2]
    low_price = prices[0][1]
    btd22 = round(((high_price - lowest_close_22) / lowest_close_22) * 100, 2)
    str22 = round(((low_price - highest_close_22) / highest_close_22) * 100, 2)
    return btd22, str22 