def mark_candle_pattern_column(df):
    for idx, row in df.iterrows():
        cur_cand = df.iloc[idx]
        if idx == 0:
            if bearish_patterns_for_index_zero(cur_cand, idx) == -1:
                df.at[df.index[idx], "candle_pattern"] = -1
            elif bullish_patterns_for_index_zero(cur_cand, idx) == 1:
                df.at[df.index[idx], "candle_pattern"] = 1
        else:
            prev_cand = df.iloc[idx - 1]
            if bearish_patterns_when_index_not_zero(prev_cand, cur_cand, idx) == -1:
                df.at[df.index[idx], "candle_pattern"] = -1
            elif bullish_patterns_when_index_not_zero(prev_cand, cur_cand, idx) == 1:
                df.at[df.index[idx], "candle_pattern"] = 1


def bearish_patterns_for_index_zero(cur_cand, idx: int) -> int:
    if shooting_star(cur_cand) == -1:
        # print(f"shooting star: {idx}")
        return -1
    elif black_marubozu(cur_cand) == -1:
        # print(f"black marubozu: {idx}")
        return -1


def bearish_patterns_when_index_not_zero(prev_cand, cur_cand, idx: int) -> int:
    if bearish_patterns_for_index_zero(cur_cand, idx) == -1:
        return -1

    if bearish_engulfing(prev_cand, cur_cand, idx) == -1:
        # print(f"bearish engulfing: {idx}")
        return -1


def bullish_patterns_for_index_zero(cur_cand, idx: int) -> int:
    if hanging_man(cur_cand) == 1:
        # print(f"hanging man: {idx}")
        return 1
    elif white_marubozu(cur_cand) == 1:
        #         print(f"white marubozu: {idx}")
        return 1


def bullish_patterns_when_index_not_zero(prev_cand, cur_cand, idx: int) -> int:
    if bullish_patterns_for_index_zero(cur_cand, idx) == -1:
        return 1
    if bullish_engulfing(prev_cand, cur_cand, idx) == 1:
        # print(f"bullish engulfing: {idx}")
        return 1


def bullish_engulfing(prev_cand, cur_cand, index: int) -> int:
    if (
            cur_cand["Close"] > prev_cand["Open"]
            and cur_cand["Open"] < prev_cand["Close"]
            and prev_cand["Close"] < prev_cand["Open"]
    ):
        return 1


def bearish_engulfing(prev_cand, cur_cand, index: int) -> int:
    if (
            cur_cand["Close"] < prev_cand["Open"]
            and cur_cand["Open"] > prev_cand["Close"]
            and prev_cand["Close"] > prev_cand["Open"]
    ):
        return -1


def shooting_star(cur_cand) -> int:
    # check that there is no bottom wick
    if (cur_cand["Close"] - cur_cand["Low"]) == 0 and (
            # check that there is top wick is greater than 3 times the size of the body
            (cur_cand["High"] - cur_cand["Open"])
            > 3 * (cur_cand["Open"] - cur_cand["Close"])
    ):
        return -1


def hanging_man(cur_cand) -> int:
    # check that there is no top wick
    if (cur_cand["High"] - cur_cand["Close"]) == 0 and (
            # check that there is bottom wick is greater than 3 times the size of the body
            (cur_cand["Open"] - cur_cand["Low"])
            > 3 * (cur_cand["Close"] - cur_cand["Open"])
    ):
        return 1


def black_marubozu(cur_cand) -> int:
    if (
            # check that body is at least 0.5% of the current close
            (cur_cand["Open"] - cur_cand["Close"]) > (0.005 * cur_cand["Close"])
            and (cur_cand["Close"] - cur_cand["Low"]) < (0.001 * cur_cand["Close"])
            and (cur_cand["High"] - cur_cand["Open"]) < (0.001 * cur_cand["Close"])
            and (
            # check that body is 4 times the top wick
            (cur_cand["Open"] - cur_cand["Close"])
            > 4 * (cur_cand["High"] - cur_cand["Open"])
    )
            and (
            # check that body is 4 times the bottom wick
            (cur_cand["Open"] - cur_cand["Close"])
            > 4 * (cur_cand["Close"] - cur_cand["Low"])
    )
            and cur_cand["Open"] > cur_cand["Close"]
    ):
        return -1


def white_marubozu(cur_cand) -> int:
    if (
            # check that body is at least 0.5% of the current close
            (cur_cand["Close"] - cur_cand["Open"]) > (0.005 * cur_cand["Close"])
            and (cur_cand["High"] - cur_cand["Close"]) < (0.001 * cur_cand["Close"])
            and (cur_cand["Open"] - cur_cand["Low"]) < (0.001 * cur_cand["Close"])
            and (
            # check that body is 4 times the top wick
            (cur_cand["Close"] - cur_cand["Open"])
            > 4 * (cur_cand["High"] - cur_cand["Close"])
    )
            and (
            # check that body is 4 times the bottom wick
            (cur_cand["Close"] - cur_cand["Open"])
            > 4 * (cur_cand["Open"] - cur_cand["Low"])
    )
            and cur_cand["Close"] > cur_cand["Open"]
    ):
        return 1
