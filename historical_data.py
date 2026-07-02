"""
================================================================================
HISTORICAL DATA SEEDING UTILITIES
================================================================================

Handles initialization pipeline querying, fetching structural backlogs from
Fyers Intraday API servers for data priming.
"""

# ============================================================
# Imports
# ============================================================
from fyers_apiv3 import fyersModel
import pandas as pd
import datetime as dt
import pytz


# ============================================================
# Configuration
# ============================================================
symbol = 'NSE:RELIANCE-EQ'
timeZone = 'Asia/Kolkata'
resolution = "1"


# ============================================================
# Historical Data Fetching
# ============================================================
def fetch_historical_data(fyers: fyersModel.FyersModel) -> pd.DataFrame:
    """
    Before starting the live chart, we need to know what happened earlier 
    in the day so the chart doesn't start from a blank screen.
    """
    
    now = dt.datetime.now(pytz.timezone(timeZone))

    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    previous_time = start_of_day.timestamp()
    current_time = now.timestamp()

    nifty_data = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "0",
        "range_from": int(previous_time),
        "range_to": int(current_time),
        "cont_flag": "1"
    }

    # Ask Fyers for the data and convert it into an Excel-like table (DataFrame)
    response = fyers.history(data=nifty_data)
    historical_data = response['candles']
    df = pd.DataFrame(historical_data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    
    # Fix the time format so humans and computers can read it easily
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert(pytz.timezone(timeZone))
    df.set_index('date', inplace=True)
    return df