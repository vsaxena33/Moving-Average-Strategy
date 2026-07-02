# ============================================================
# Import
# ============================================================
from candle_engine import update_live_data
from log_trade import log_trade
import pandas as pd


# ============================================================
# Configuration
# ============================================================
symbol     = 'NSE:RELIANCE-EQ'


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
        self.fyersSocket = None  # Will be set later when the websocket is created
        
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

        ema_9       = self.data['ema_9'].iloc[-2]
        ema_15      = self.data['ema_15'].iloc[-2]

        # --------------------------------------------------------
        # TICK-LEVEL Exit: Hard TP/SL (runs on EVERY tick)
        # --------------------------------------------------------
        if self.position == 'LONG' and (ltp < ema_15 or ltp >= self.tp):#(ltp >= self.tp or ltp <= self.sl):
            print(f"[EXIT LONG] Hard TP/SL hit at {ltp}")
            bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
            log_trade("Sell", symbol, bid)
            self._clear_position()

        elif self.position == 'SHORT' and (ltp > ema_15 or ltp <= self.tp):#(ltp <= self.tp or ltp >= self.sl):
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

            if (prev_ema_15 < prev_ema_9 and ema_15 > ema_9) or (prev_ema_15 - prev_ema_9 < ema_15 - ema_9):
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

            if (prev_ema_15 > prev_ema_9 and ema_15 < ema_9) or (prev_ema_9 - prev_ema_15 < ema_9 - ema_15):
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
            avg_length = (self.data.tail()['high'] - self.data.tail()['low']).mean()
            if ((prev_ema_9 - prev_ema_15 < ema_9 - ema_15) and
                (closed_candle['close'] > ema_9) and
                (closed_candle['low'] <= ema_9 + avg_length * 0.05) and
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
                  (closed_candle['high'] >= ema_9 - avg_length * 0.05) and
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
        self.fyersSocket.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        self.fyersSocket.keep_running()