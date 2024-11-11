import talib as ta
import pandas as pd
from untrade.client import Client
import warnings
warnings.filterwarnings("ignore")
import numpy as np

def read_data (path):
    df = pd.read_csv(path)
    return df

def heikin_ashi_candlesticks(data):
    # Calculate HA Close
    data["ha_close"] = (data["low"] + data["high"] + data["close"] + data["open"]) / 4
    
    # Initialize HA Open and calculate for each row
    for i in range(len(data)):
        if i == 0:
            # For the first row
            data.at[i, "ha_open"] = (data.at[i, "open"] + data.at[i, "close"]) / 2
        else:
            # For subsequent rows
            data.at[i, "ha_open"] = (data.at[i - 1, "ha_open"] + data.at[i - 1, "ha_close"]) / 2
    
    # Calculate HA High and HA Low
    data["ha_high"] = data[["ha_open", "ha_close", "high"]].max(axis=1)
    data["ha_low"] = data[["ha_open", "ha_close", "low"]].min(axis=1)
    
    return data



def include_macd(df,short_period , large_period , signal_period):
    # df['macd'] , df['macdsignal'] , df['macdhist'] = ta.MACD(df['close'], fastperiod=short_period, slowperiod=large_period, signalperiod=signal_period)
    macd , macdsignal , macdhist = ta.MACD(df['ha_close'], fastperiod=short_period, slowperiod=large_period, signalperiod=signal_period)
    return macd , macdsignal , macdhist


def include_triple_ema(df, period_fastest, period_fast, period_slow,twohundred):
    # Calculate three EMAs with specified periods
    EMA_15 = ta.EMA(df['ha_close'], timeperiod=period_fastest)
    EMA_25 = ta.EMA(df['ha_close'], timeperiod=period_fast)
    EMA_50 = ta.EMA(df['ha_close'], timeperiod=period_slow)
    EMA_200 = ta.EMA(df['ha_close'], timeperiod=twohundred)
    
    return EMA_15, EMA_25, EMA_50 ,EMA_200

def include_adx(df,df_period):
    plusdi = ta.PLUS_DI(df['high'],df['low'],df['close'],timeperiod = df_period)
    minusdi = ta.MINUS_DI(df['high'],df['low'],df['close'],timeperiod = df_period)
    adx = ta.ADX(df['ha_high'] , df['ha_low'] ,df['ha_close'],df_period)
    

    return plusdi,minusdi,adx
# def include_adx(df , input):











def perform_backtest(csv_file_path,l,type):
    """
    Perform backtesting using the untrade SDK.

    Parameters:
    - csv_file_path (str): Path to the CSV file containing historical price data and signals.

    Returns:
    - result (generator): Result is a generator object that can be iterated over to get the backtest results.
    """
    # Create an instance of the untrade client
    client = Client()

    # Perform backtest using the provided CSV file path
    result = client.backtest(
        file_path=csv_file_path,
        leverage=l,  # Adjust leverage as needed
        jupyter_id="<id>",  # the one you use to login to jupyter.untrade.io
        result_type=type,
        # result_type = None,
    )
    # result_type can be one of the following:
    # "Q" - Quarterly Analysis(last dates of quarters)
    # "QS" - Quarterly Analysis(start dates of quarters)
    # "M" -  Monthly Analysis(last date of months)
    # "MS"- Monthly Analysis(start dates of months)
    # "Y" - Yearly Analysis(last dates of years)
    # "YS" - Yearly Analysis(start dates of years)
    # "6M" - Semi Annual Analysis(last dates of 6 months)
    # "6MS" - Semi Annual Analysis(start dates of 6 months)

    return result





def convert_csv(df,path_to_save):
    df.to_csv(path_to_save)



def generate_signals(data, tp_multiplier=2, sl_multiplier=1.5, rsi_period=14, atr_period=14):
    """
    Generate trading signals with triple EMA crossover, dynamic stop loss, and partial take profit.
    
    Parameters:
    - data (DataFrame): Historical price data.
    - adx_threshold (float): Threshold for ADX to confirm a strong trend.
    - tp_percentages (tuple): Take profit percentages for each partial take profit level.
    - initial_sl_percent (float): Initial stop loss percentage.
    
    Returns:
    - data (DataFrame): Data with signals and SL/TP levels.
    """
    data['RSI'] = ta.RSI(data['ha_close'], timeperiod=rsi_period)
    data['ATR'] = ta.ATR(data['ha_high'], data['ha_low'], data['ha_close'], timeperiod=atr_period)
    data['Volume_MA'] = data['volume'].rolling(window=20).mean()  
    
    inmarket = 0          # 0 means no position; 1 means a position is open
    long_short = 0        # 1 for long, -1 for short
    data['signals'] = 0
    data['trade_type'] = 'None'
    # data['TP'] = 0
    # data['SL'] = 0
    data['entry_price'] = 0  # Column to track entry price
    data['exit_price'] = 0   # Column to track exit price
    # tp_hit_count = 0      # Track the number of TP levels hit
    
    for i in range(len(data)):
    # Skip rows with any NaN values to avoid calculation errors
        if data.isna().iloc[i].any():
            continue

        # Check for entry signals when no position is open
        if inmarket == 0:
            if data['adx'].iloc[i]>25:
                entry_price = data['ha_close'].iloc[i]
                rsi_value = data['RSI'].iloc[i]
                # Long entry signal
                if (
                    # rsi_value > 20 and  # Avoid oversold levels
                    data['MACD'].iloc[i] > data['MACD_Signal'].iloc[i] and
                    data['+di'].iloc[i] > data['-di'].iloc[i] and 
                    data['ha_close'].iloc[i] > data['ha_open'].iloc[i]# Green Heikin Ashi candle
                    # and data['volume'].iloc[i] > data['Volume_MA'].iloc[i]
                    and data['MACD'].iloc[i]<0
                    ):  
                    
                    data.loc[i, 'signals'] = 1
                    data.loc[i, 'trade_type'] = 'long'
                    data.loc[i, 'entry_price'] = entry_price  # Record entry price
                    inmarket=  1
                    long_short = 1
                    # Set initial TP and SL using ATR
                    # data.loc[i, 'TP'] = entry_price + (data['ATR'].iloc[i] * tp_multiplier)
                    # data.loc[i, 'SL'] = entry_price - (data['ATR'].iloc[i] * sl_multiplier)

                # Short entry signal
                elif (
                    # rsi_value < 70 and 
                    data['MACD'].iloc[i] < data['MACD_Signal'].iloc[i] and
                    data['-di'].iloc[i] > data['+di'].iloc[i] and
                    data['ha_close'].iloc[i] < data['ha_open'].iloc[i] # Red Heikin Ashi candle
                    # and data['volume'].iloc[i] > data['Volume_MA'].iloc[i]
                    and data['MACD'].iloc[i]>0
                    ):  

                    data.loc[i, 'signals'] = -1
                    data.loc[i, 'trade_type'] = 'short'
                    data.loc[i, 'entry_price'] = entry_price  # Record entry price
                    inmarket= 1
                    long_short = -1
                    # Set initial TP and SL using ATR
                    # data.loc[i, 'TP'] = entry_price - (data['ATR'].iloc[i] * tp_multiplier)
                    # data.loc[i, 'SL'] = entry_price + (data['ATR'].iloc[i] * sl_multiplier)

        # Exit signals and trailing SL adjustment
        elif inmarket == 1:
            current_price = data['ha_close'].iloc[i]

            # For Long Position
            if long_short == 1:
                # if current_price >= data['TP'].iloc[i-1]:  # Take-Profit Hit
                #     data.loc[i, 'exit_price'] = current_price
                #     data.loc[i, 'signals'] = -1
                #     data.loc[i,'trade_type']="close"
                #     inmarket = 0  # Close position
                #     long_short = 0

                # if current_price <= data['SL'].iloc[i-1]:  # Stop-Loss Hit
                #     data.loc[i, 'exit_price'] = current_price
                #     data.loc[i, 'signals'] = -1
                #     data.loc[i,'trade_type']="close"
                #     inmarket =  0  # Close position
                #     long_short = 0

                if data['EMA_25'].iloc[i] < data['EMA_50'].iloc[i] and data['+di'].iloc[i]<data['-di'].iloc[i] and data['EMA_25'].iloc[i]>data['EMA_200'].iloc[i]:  # Dynamic Exit
                    data.loc[i, 'exit_price'] = current_price
                    data.loc[i, 'signals'] = -1
                    data.loc[i,'trade_type']="close"
                    inmarket = 0  # Close position
                    long_short = 0

                # Exit on dynamic SL
                # elif (data['EMA_15'].iloc[i] > data["ha_close"].iloc[i]):
                #     data.loc[i, 'exit_price'] = current_price  # Record exit price on SL
                #     data.loc[i, 'signals'] = -1
                #     data.loc[i,'trade_type']="close"
                #     inmarket = 0
                #     long_short = 0

            # For Short Position
            elif long_short == -1:
                # if current_price <= data['TP'].iloc[i-1]:  # Take-Profit Hit
                #     data.loc[i, 'exit_price'] = current_price
                #     data.loc[i, 'signals'] = 1
                #     data.loc[i,'trade_type']="close"
                #     inmarket = 0  # Close position
                #     long_short = 0

                # if current_price >= data['SL'].iloc[i-1]:  # Stop-Loss Hit
                #     data.loc[i, 'exit_price'] = current_price
                #     data.loc[i, 'signals'] = 1
                #     data.loc[i,'trade_type']="close"
                #     inmarket =  0  # Close position
                #     long_short = 0

                if data['EMA_25'].iloc[i] > data['EMA_50'].iloc[i] and data['+di'].iloc[i]>data['-di'].iloc[i] and data['EMA_25'].iloc[i]<data['EMA_200'].iloc[i]:  # Dynamic Exit
                    data.loc[i, 'exit_price'] = current_price
                    data.loc[i, 'signals'] = 1
                    data.loc[i,'trade_type']="close"
                    inmarket = 0  # Close position
                    long_short = 0

                # # Exit on dynamic SL
                # elif (data['EMA_15'].iloc[i] < data["ha_close"].iloc[i]):
                #     data.loc[i, 'exit_price'] = current_price  # Record exit price on SL
                #     data.loc[i, 'signals'] = 1
                #     data.loc[i,'trade_type']="close"
                #     inmarket = 0
                #     long_short = 0
    return data

# Example usage of the modified functions
macd_list = [(8,26,9)]
ema_periods = (15,25,50,220)  # (fastest, fast, slow)
adx_list = [7]
# data_pth = ['btcusdt_4h.csv','ethusdt_4h.csv']
data_pth = ['btcusdt_4h.csv']

for a, b, c in macd_list:
    for ax in adx_list:
        for data_path in data_pth:
            data = read_data(data_path)
            data = heikin_ashi_candlesticks(data)
            data['+di'], data['-di'],data['adx'] = include_adx(data, ax)
            data['EMA_15'], data['EMA_25'], data['EMA_50'], data['EMA_200'] = include_triple_ema(data, *ema_periods)
            
            data['MACD'], data['MACD_Signal'], data['MACD_Hist'] = include_macd(data, a, b, c)
            data = generate_signals(data)
            
            # Save and backtest results
            signal_path = './signals_new.csv'
            data.to_csv(signal_path, index=False)
            backtest_result = perform_backtest(signal_path, 1, "Q")
            last_value = None
            for value in backtest_result:
                print(value)
