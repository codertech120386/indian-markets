import os
from datetime import datetime, timedelta
import pandas as pd

import config
from utils import get_instrument_token
from algo_trading_helpers import get_trend, determine_trend, check_candles, mark_candle_pattern_column


# from utils.trend_functions import get_trend, determine_trend, check_candles
# from utils.candle_patterns import mark_candle_pattern_column


def get_data(kt, symbol: str, period: str, interval: str):
    symbol_lower = "_".join(symbol.split(".")).lower()
    file_name = f"strategies/candlestick-1m-5m/scripts_csvs/{symbol_lower}_{interval}.csv"
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        df["Datetime"] = pd.to_datetime(df["Datetime"])
    else:

        instrument_token_series = get_instrument_token(instrument_name=symbol)
        instrument_token = instrument_token_series.to_list()[0]
        from_datetime = get_data_period(period)
        to_datetime = datetime.now()
        interval = config.ONE_MINUTE
        kite_historical_data = kt.historical_data(
            instrument_token,
            from_datetime,
            to_datetime,
            interval,
            continuous=False,
            oi=False,
        )

        df = pd.DataFrame.from_dict(kite_historical_data)
        df["Datetime"] = pd.to_datetime(df["date"])
        df.drop(df[df["volume"] == 0].index, inplace=True, axis=0)
        df.drop(df[df["open"] == df["close"]].index, inplace=True, axis=0)

        df = df.rename(
            columns={
                "open": "Open",
                "close": "Close",
                "high": "High",
                "low": "Low",
                "volume": "Volume",
            }
        )
        df = df.drop(labels="date", axis=1)
        df["period_low"] = 0.0
        df["period_high"] = 0.0
        df["candle_pattern"] = 0  # -1 for sell, 0 for no position and 1 for buy
        df["position"] = 0  # -1 for sell, 0 for no position and 1 for buy
        df["position_active"] = 0  # 1 for active and 0 for non-active
        df["sma_10"] = round(get_trend(df, 10), 2)
        df["sma_20"] = round(get_trend(df, 20), 2)
        df["sma_30"] = round(get_trend(df, 30), 2)
        df["trend"] = df.apply(determine_trend, axis=1)
        df["category"] = check_candles(df, 5, "sma_30")
        df.reset_index(inplace=True)
        df.to_csv(file_name)

    mark_candle_pattern_column(df)
    return df, file_name


def get_data_period(period: str):
    amount = '0'
    unit = 'd'
    from_datetime = datetime.now()
    for char in period:
        if char.isdigit():
            amount += str(char)
        else:
            unit = char
    if unit == 'd':
        from_datetime = datetime.now() - timedelta(days=float(amount))
    elif unit == 'h':
        from_datetime = datetime.now() - timedelta(hours=float(amount))
    elif unit == 'm':
        from_datetime = datetime.now() - timedelta(minutes=float(amount))
    # From last & days

    return from_datetime
