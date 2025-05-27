from polygon import RESTClient
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import random
from database import write_market, read_market
from functools import lru_cache
import pytz

load_dotenv(override=True)

polygon_api_key = os.getenv("POLYGON_API_KEY")
polygon_plan = os.getenv("POLYGON_PLAN")

is_paid_polygon = polygon_plan == "paid"
is_realtime_polygon = polygon_plan == "realtime"

def adjust_weekend_date(date: datetime.date) -> datetime.date:
    """Adjust weekend dates to the previous Friday.
    
    Args:
        date: The date to check and adjust
        
    Returns:
        The adjusted date (same date if not weekend, previous Friday if weekend)
    """
    if date.weekday() >= 5:  # Saturday (5) or Sunday (6)
        days_to_subtract = date.weekday() - 4  # 1 for Saturday, 2 for Sunday
        return date - timedelta(days=days_to_subtract)
    return date

def is_market_open() -> bool:
    client = RESTClient(polygon_api_key)
    market_status = client.get_market_status()
    return market_status.market == "open"

def get_all_share_prices_polygon_eod() -> dict[str, float]:
    client = RESTClient(polygon_api_key, trace=True)

    probe = client.get_previous_close_agg("SPY")[0]
    ny_tz = pytz.timezone('America/New_York')
    last_close = datetime.fromtimestamp(probe.timestamp/1000, tz=ny_tz).date()

    results = client.get_grouped_daily_aggs(last_close, adjusted=True, include_otc=False)
    return {result.ticker: result.close for result in results}

@lru_cache(maxsize=20)
def get_market_for_prior_date(today):
    market_data = read_market(today)
    if not market_data:
        market_data = get_all_share_prices_polygon_eod()
        write_market(today, market_data)
    return market_data

def get_share_price_polygon_eod(symbol) -> float:
    ny_tz = pytz.timezone('America/New_York')
    today = datetime.now(ny_tz).date()
    market_data = get_market_for_prior_date(today)
    return market_data.get(symbol, 0.0)

def get_share_price_polygon_min(symbol) -> float:
    client = RESTClient(polygon_api_key)
    result = client.get_snapshot_ticker("stocks", symbol)
    return result.min.close

def get_share_price_polygon(symbol) -> float:
    if is_paid_polygon:
        return get_share_price_polygon_min(symbol)
    else:
        return get_share_price_polygon_eod(symbol)

def get_share_price(symbol) -> float:
    if polygon_api_key:
        try:
            return get_share_price_polygon(symbol)
        except Exception as e:
            print(f"Was not able to use the polygon API due to {e}; using a random number")
    return float(random.randint(1, 100))