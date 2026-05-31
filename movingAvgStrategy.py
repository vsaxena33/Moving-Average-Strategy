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
import datetime as dt
import pandas as pd
import pytz
import talib as ta
import numpy as np


# ============================================================
# Configuration
# ============================================================
symbol = 'NSE:RELIANCE-EQ'
timeZone = 'Asia/Kolkata'
resolution = "1"


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


# ============================================================
# Log Data
# ============================================================
def log_trade(action, symbol, price):
    """
    Append trade details to a CSV log.

    Parameters:
        action (str): "BUY" or "SELL".
        symbol (str): Symbol.
        price (float): Execution price.
    """
    with open("trades_log.csv", "a") as file:
        file.write(f"{dt.datetime.now(pytz.timezone(timeZone))},{action},{symbol},{price}\n")

# ============================================================
# Candlestick Class to Manage State and WebSocket Callbacks
# ============================================================
class Candlestick:
    """
    This class acts like a real-time market data manager.

    Responsibilities:
    - Receives live market ticks from the websocket.
    - Updates candles continuously.
    - Stores latest market state in memory.
    - Maintains live support/resistance structures.

    Think of this class as the "brain"
    controlling the live chart.
    """
    # ============================================================
    # Initialization
    # ============================================================
    def __init__(self, data, fyers):
        """
        Initializes the Candlestick class.
        """
        self.data = data
        self.fyers = fyers
        self.last_total_volume = None
        
        # Trading State Variables
        self.position = None     # LONG / SHORT / None
        self.sl = None           # Active stop loss
        self.tp = None           # Active take profit
        self.trigger = None      # Price level used for trailing logic

        # Prevents duplicate signal evaluation on the same candle
        self.last_evaluated_candle = None


    # ============================================================
    # Cleaner function
    # ============================================================    
    def _clear_position(self):
        self.position = None
        self.sl = None
        self.tp = None
        self.trigger = None


    # ============================================================
    # WebSocket Callback to Handle Incoming Messages
    # ============================================================
    def onmessage(self, message):
        """
        Callback function to handle incoming messages from the FyersDataSocket WebSocket.

        Parameters:
            message (dict): The received message from the WebSocket.

        """
        self.data, self.last_total_volume = update_live_data(data=self.data, message=message, last_total_volume=self.last_total_volume)
        print(self.data.tail())

        # --- Guard Conditions ---
        if len(self.data) < 2:
            return
        
        ltp = message.get('ltp')
        if ltp is None:
            return

        # --- Last Closed Candle ---
        closed_candle = self.data.iloc[-2]
        closed_candle_time = self.data.index[-2]
        is_new_candle = closed_candle_time != self.last_evaluated_candle

        # --------------------------------------------------------
        # TICK-LEVEL Exit: Hard TP/SL (runs on EVERY tick)
        # --------------------------------------------------------
        if self.position == 'LONG' and (ltp >= self.tp or ltp <= self.sl):
            print(f"[EXIT LONG] Hard TP/SL hit at {ltp}")
            bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
            log_trade("Sell", symbol, bid)
            self._clear_position()

        elif self.position == 'SHORT' and (ltp <= self.tp or ltp >= self.sl):
            print(f"[EXIT SHORT] Hard TP/SL hit at {ltp}")
            ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
            log_trade("Buy", symbol, ask)
            self._clear_position()

        # --------------------------------------------------------
        # CANDLE-LEVEL Logic (runs only once per closed candle)
        # --------------------------------------------------------
        if not is_new_candle:
            return

        # Mark this candle as evaluated ONCE, at the top
        self.last_evaluated_candle = closed_candle_time

        ema_9       = self.data['ema_9'].iloc[-2]
        ema_15      = self.data['ema_15'].iloc[-2]
        prev_ema_9  = self.data['ema_9'].iloc[-3]
        prev_ema_15 = self.data['ema_15'].iloc[-3]

        # ✅ Skip signal logic entirely if EMAs aren't ready yet
        if any(pd.isna(v) for v in [ema_9, ema_15, prev_ema_9, prev_ema_15]):
            return

        # --------------------------------------------------------
        # CANDLE-LEVEL Exit: Cross signals + Trailing SL
        # --------------------------------------------------------
        # Move TP and SL together by the same amount.
        #
        # Example:
        # Old SL = 100
        # Old TP = 120
        #
        # New SL = 105
        #
        # Risk locked in = +5
        # TP shifted to 125
        #
        # This preserves the original reward:risk structure
        # while protecting accumulated profit.
        if self.position == 'LONG':

            if prev_ema_15 < prev_ema_9 and ema_15 > ema_9:
                print(f"[EXIT LONG] Death cross at {closed_candle_time}")
                bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                log_trade("Sell", symbol, bid)
                self._clear_position()

            elif closed_candle['close'] > self.trigger:
                new_sl = closed_candle['low']
                if new_sl > self.sl:
                    diff    = new_sl - self.sl
                    self.tp += diff
                    self.sl  = new_sl
                    self.trigger = closed_candle['high']

        elif self.position == 'SHORT':

            if prev_ema_15 > prev_ema_9 and ema_15 < ema_9:
                print(f"[EXIT SHORT] Golden cross at {closed_candle_time}")
                ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                log_trade("Buy", symbol, ask)
                self._clear_position()

            elif closed_candle['close'] < self.trigger:
                new_sl = closed_candle['high']
                if new_sl < self.sl:
                    diff    = self.sl - new_sl
                    self.tp -= diff
                    self.sl  = new_sl
                    self.trigger = closed_candle['low']
        

        # --------------------------------------------------------
        # CANDLE-LEVEL Entry (only if flat after exit above)
        # --------------------------------------------------------
        if not self.position:

            # Long Entry Conditions:
            #
            # 1. EMA spread is increasing
            #    -> bullish momentum is accelerating.
            #
            # 2. Candle closes above EMA-9
            #    -> confirms short-term strength.
            #
            # 3. EMA-9 remains above EMA-15
            #    -> trend filter.
            #
            # Together these conditions attempt to enter only
            # when bullish momentum is expanding.
            if ((prev_ema_9 - prev_ema_15 < ema_9 - ema_15) and
                (closed_candle['close'] > ema_9) and
                (ema_9 - ema_15 > 0)):
                ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                log_trade("Buy", symbol, ask)
                self.sl       = closed_candle['low']
                self.tp       = ask + (ask - self.sl) * 2
                self.trigger  = closed_candle['high']
                self.position = 'LONG'
                print(f"[BUY] at {ask} | SL: {self.sl} | TP: {self.tp}")

            # Short Entry Conditions:
            #
            # 1. EMA spread is increasing
            #    -> bearish momentum is accelerating.
            #
            # 2. Candle closes above EMA-9
            #    -> confirms short-term strength.
            #
            # 3. EMA-15 remains above EMA-9
            #    -> trend filter.
            #
            # Together these conditions attempt to enter only
            # when bearish momentum is expanding.
            elif ((prev_ema_15 - prev_ema_9 < ema_15 - ema_9) and
                  (closed_candle['close'] < ema_9) and
                  (ema_15 - ema_9 > 0)):
                bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                log_trade("Sell", symbol, bid)
                self.sl       = closed_candle['high']
                self.tp       = bid - (self.sl - bid) * 2
                self.trigger  = closed_candle['low']
                self.position = 'SHORT'
                print(f"[SELL] at {bid} | SL: {self.sl} | TP: {self.tp}")

    # ============================================================
    # Websocket Callback to Handle Errors Events
    # ============================================================
    def onerror(self, message):
        """
        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    # ============================================================
    # Websocket Callback to Handle Connection Close Events
    # ============================================================
    def onclose(self, message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    # ============================================================
    # Websocket Callback to Handle Subscription Upon Connection
    # ============================================================
    def onopen(self):
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"

        # Subscribe to the specified symbols and data type
        symbols = [symbol]
        fyersSocket.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyersSocket.keep_running()


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

    # 6. Establish a connection to the Fyers WebSocket
    print("Connecting to live stream...")
    fyersSocket.connect()
