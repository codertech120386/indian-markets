import pandas_ta as ta


def get_trend(df, length: int):
    return ta.sma(df["Close"], length)


def determine_trend(df):
    if df["sma_10"] > df["sma_20"] > df["sma_30"]:
        return 1  # Uptrend
    elif df["sma_10"] < df["sma_20"] < df["sma_30"]:
        return -1  # Downtrend
    else:
        return 0  # No trend


def check_candles(df, back_candles, ma_column):
    categories = [0 for _ in range(back_candles)]
    for i in range(back_candles, len(df)):
        if all(df["Close"][i - back_candles: i] > df[ma_column][i - back_candles: i]):
            categories.append(1)  # Uptrend
        elif all(
                df["Close"][i - back_candles: i] < df[ma_column][i - back_candles: i]
        ):
            categories.append(-1)  # Downtrend
        else:
            categories.append(0)  # No trend
    return categories
