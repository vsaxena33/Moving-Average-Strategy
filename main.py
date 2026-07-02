"""
================================================================================
# REAL-TIME EMA MOMENTUM TRADING & ALGORITHMIC EXECUTION ENGINE
================================================================================

This program implements a fully automated, real-time momentum trading system
using live market data streamed through the FYERS WebSocket API. The engine
constructs OHLCV candles from tick-by-tick market updates, continuously
maintains Exponential Moving Averages (EMAs), and executes a rule-based
trend-following strategy with dynamic risk management.

The system continuously processes live market data to:

1. Build Real-Time OHLCV Candles:
   Aggregates streaming market ticks into minute-level OHLCV candles while
   converting cumulative exchange volume into incremental candle volume.
   Open, High, Low, Close, and Volume are updated dynamically on every tick.

2. Maintain Live Exponential Moving Averages:
   Calculates EMA(9) and EMA(15) using historical market data and updates
   the current candle's EMA values in real time using rolling EMA equations,
   providing an up-to-date representation of short-term market momentum.

3. Detect Momentum-Based Trade Opportunities:
   Evaluates trading conditions exactly once per completed candle to prevent
   duplicate signal generation and reduce intrabar noise.

   * LONG Entry:
     Triggered when:
     • EMA(9) remains above EMA(15)
     • Bullish EMA separation is expanding
     • Price closes above EMA(9)

   * SHORT Entry:
     Triggered when:
     • EMA(15) remains above EMA(9)
     • Bearish EMA separation is expanding
     • Price closes below EMA(9)

4. Manage Active Risk & Dynamic Exits:

   * Executes simulated fills using live Market Depth Bid/Ask prices.
   * Applies hard Stop Loss and Take Profit levels immediately on every tick.
   * Implements structure-based trailing stop logic using recently closed
     candle highs and lows.
   * Dynamically shifts Take Profit targets as Stop Loss levels advance,
     preserving the original Risk-to-Reward profile.
   * Exits positions automatically upon detection of an opposing EMA crossover.

5. Persist Execution Records:
   All trade executions are chronologically stored in 'trades_log.csv' for
   auditability, performance analysis, and strategy evaluation.

The project demonstrates production-grade concepts used in:

* Quantitative Trading & Strategy Automation
* Event-Driven WebSocket Architecture
* Real-Time Time-Series Data Processing
* Streaming Technical Indicator Computation
* State-Based Trading System Design
* Automated Risk Management & Position Control

Libraries Used:

* FYERS API v3   (fyersModel, FyersWebsocket)
* TA-Lib         (Historical EMA initialization)
* pandas & numpy (OHLCV processing and state management)
* pytz           (Timezone-aware timestamp handling)

# Author: Vaibhav Saxena
================================================================================
"""


# ============================================================
# Imports
# ============================================================
from fyers_apiv3.FyersWebsocket import data_ws
from fyers_apiv3 import fyersModel
from credentials import client_id
from historical_data import fetch_historical_data
from candle_engine import Candlestick
import talib as ta


# ============================================================
# STARTING THE PROGRAM
# ============================================================
if __name__ == "__main__":
    '''
    ============================================================
    HOW THE SYSTEM WORKS
    ============================================================
    
    Historical Data
            ↓
    EMA Initialization
            ↓
    Live WebSocket Ticks
            ↓
    Real-Time Candle Builder
            ↓
    EMA Update
            ↓
    Signal Evaluation
            ↓
    Trade Execution & Logging
    
    ============================================================
    '''
    
    # 1. Read your secret keys (Like a password for the stock market)
    try:
        with open('access_token.txt', 'r') as file:
            access_token = file.read()
    except FileNotFoundError:
        print("Error: access_token.txt not found! Please login first.")
        exit()

    # 2. Fetch data from earlier today and add moving averages
    print("Fetching morning data...")
    fyers_connection = fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path='')
    historical_df = fetch_historical_data(fyers=fyers_connection)
    
    # 3. Calculate moving averages and add them as new columns in the DataFrame
    # Calculate EMA using 'close' as the source and a length of 9
    historical_df['ema_9'] = ta.EMA(historical_df['close'], 9)
    # Calculate EMA using 'close' as the source and a length of 15
    historical_df['ema_15'] = ta.EMA(historical_df['close'], 15)

    # 4. Initialize our data manager
    candlestick = Candlestick(historical_df, fyers_connection)

    # 5. Create a FyersDataSocket instance with the provided parameters
    fyersSocket = data_ws.FyersDataSocket(
        access_token=access_token,          # Access token in the format "appid:accesstoken"
        log_path="",                        # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=False,                     # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,                # Save response in a log file instead of printing it.
        reconnect=True,                     # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=candlestick.onopen,      # Callback function to subscribe to data upon connection.
        on_close=candlestick.onclose,       # Callback function to handle WebSocket connection close events.
        on_error=candlestick.onerror,       # Callback function to handle WebSocket errors.
        on_message=candlestick.onmessage    # Callback function to handle incoming messages from the WebSocket.
    )

    candlestick.fyersSocket = fyersSocket   # Link the WebSocket to the candlestick manager

    # 6. Establish a connection to the Fyers WebSocket
    print("Connecting to live stream...")
    fyersSocket.connect()