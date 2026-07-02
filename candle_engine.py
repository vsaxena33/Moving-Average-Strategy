# ============================================================
# Imports
# ============================================================
import pandas as pd
import numpy as np
from rolling_MA import rolling_ema


# ============================================================
# Configuration
# ============================================================
timeZone = 'Asia/Kolkata'


# ============================================================
# Update Logic for Live Data
# ============================================================
def update_live_data(data, message, last_total_volume):
    """
    Update the OHLCV dataframe using incoming websocket tick data.

    The websocket provides cumulative traded volume for the session.
    This function converts cumulative volume into incremental candle volume.

    Parameters
    ----------
    data : pandas.DataFrame
        Existing OHLCV dataframe indexed by timestamp.

    message : dict
        Incoming websocket tick message from Fyers.

    last_total_volume : int or None
        Previous cumulative traded volume received from websocket.

    Returns
    -------
    tuple
        Updated dataframe and latest cumulative traded volume.
    """

    # If the message is empty or broken, don't do anything
    if "symbol" not in message:
        return data, last_total_volume
    
    ltp = message.get('ltp')                            # LTP = Last Traded Price (The current market price)

    if ltp is None:
        return data, last_total_volume
    
    # Cumulative volume is the total shares traded since 9:15 AM. 
    # We want to find out how many were traded just in the last tick.
    total_vol = message.get('vol_traded_today')
    
    # Create a timestamp rounded to the current minute (e.g., 10:05:42 becomes 10:05:00)
    timestamp = pd.Timestamp.now(tz=timeZone).floor('1min')

    if total_vol is None:
        total_vol = last_total_volume if last_total_volume is not None else 0

    # The websocket gives TOTAL traded volume for the entire day.
    #
    # But each candle should only contain volume traded
    # during that specific minute.
    #
    # So:
    #
    # Incremental Volume = Current Total Volume - Previous Total Volume
    if last_total_volume is None:
        incremental_vol = 0
    else:
        incremental_vol = total_vol - last_total_volume

    if incremental_vol < 0:
        incremental_vol = 0

    # If the latest candle already belongs to the current minute,
    # we UPDATE the existing candle.
    #
    # Otherwise:
    # we CREATE a completely new candle.
    if len(data) > 0 and data.index[-1] == timestamp:
        # If we are still in the same minute, we update the existing candle
        data.iloc[-1, 3] = ltp                          # Close price continuously tracks latest traded price
        data.iloc[-1, 1] = max(data.iloc[-1, 1], ltp)   # Update High if price went higher
        data.iloc[-1, 2] = min(data.iloc[-1, 2], ltp)   # Update Low if price went lower
        data.iloc[-1, 4] += incremental_vol             # Add the new volume to the minute's total

        # Recalculating EMAs of Live Candle
        if len(data) >= 2:
            prev_ema_9  = data.iloc[-2, 5]   # last closed candle's ema_9
            prev_ema_15 = data.iloc[-2, 6]   # last closed candle's ema_15
            
            # EMA values are updated continuously during candle formation.
            #
            # This provides a live estimate of EMA movement before
            # candle close and allows the chart to display a more
            # realistic real-time trend representation.
            #
            # Trading decisions are still made only on closed candles.
            data.iloc[-1, 5] = rolling_ema(ltp, prev_ema_9, 9) if pd.notna(prev_ema_9) else np.nan
            data.iloc[-1, 6] = rolling_ema(ltp, prev_ema_15, 15) if pd.notna(prev_ema_15) else np.nan
    else:
        prev_ema_9  = data.iloc[-1, 5] if len(data) > 0 else np.nan
        prev_ema_15 = data.iloc[-1, 6] if len(data) > 0 else np.nan

        new_ema_9  = rolling_ema(ltp, prev_ema_9, 9) if pd.notna(prev_ema_9) else np.nan
        new_ema_15 = rolling_ema(ltp, prev_ema_15, 15) if pd.notna(prev_ema_15) else np.nan

        new_candle = pd.DataFrame(
            [{'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                'volume': incremental_vol, 'ema_9': new_ema_9, 'ema_15': new_ema_15}],
            index=[timestamp]
        )
        data = pd.concat([data, new_candle])


    return data, total_vol