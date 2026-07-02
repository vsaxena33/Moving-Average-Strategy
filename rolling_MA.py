"""
================================================================================
ROLLING MOVING AVERAGES ENGINE
================================================================================

This module provides lightweight, O(1) mathematical updates for running 
Exponential Moving Averages (EMA) and Simple Moving Averages (SMA) on live streams.
"""

# ============================================================
# Rolling EMA Calculation for Live Candle
# ============================================================
def rolling_ema(ltp, prev_ema, length):
    """
    Calculate the Exponential Moving Average (EMA) for a new tick using the previous EMA value.

    Parameters
    ----------
    ltp : float
        The latest traded price (current market price).

    prev_ema : float
        The EMA value from the last closed candle.

    length : int
        The period length for the EMA calculation (e.g., 9 or 15).

    Returns
    -------
    float
        The updated EMA value based on the latest tick.
    """
    multiplier = 2 / (length + 1)
    new_ema = (ltp - prev_ema) * multiplier + prev_ema
    return new_ema