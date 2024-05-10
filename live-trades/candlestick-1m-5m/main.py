import time
from datetime import datetime, timedelta
import schedule

import pandas as pd
from algo_trading_helpers import get_file_name, KiteApp, drop_unnamed_cols_in_df, mark_candle_pattern_column, \
    candle_1m_5m_scalping_data_with_signals as data_with_signals, create_new_row_in_df, calc_trade_limits

from zerodha.kite_functions import get_instrument_token
import config

symbol_name = "PFC"
kt = KiteApp(enc_token=config.enc_token)
buy = "buy"
sell = "sell"


def check_if_trade_was_active_in_prev_section(df, current_index: int):
    start_index = 0
    if (current_index + 1) % 5 != 0:
        start_index = current_index
    else:
        start_index = current_index - current_index % 5
    if len(df) > 5:
        # if (
        #         df.iloc[current_index - 5: current_index]["position_active"].sum() != 0 and
        #         df.iloc[start_index: current_index]["position_active"].sum() != 0
        # ):
        if (
                (-1 in df.iloc[current_index - 5: current_index]["position_active"].values or
                 1 in df.iloc[current_index - 5: current_index]["position_active"].values) and
                df.iloc[start_index: current_index]["position_active"].sum() != 0
        ):
            return True
        return False
    return True


def trade_without_trend(df, price: float, current_index: int):
    # Selling logic
    # if len(df) > 5 and (price < self.data.period_low[-5]):
    if len(df) > 5 and (price < df.iloc[-5]["period_low"]):
        if not self.position.is_short:
            sl, tp, final_sl, final_tp = calc_trade_limits(last_open=float(self.data.Open[-1]),
                                                           last_close=float(self.data.Close[-1]), pos=sell,
                                                           price=price,
                                                           risk_to_reward=self.risk_to_reward)
            self.sell_trade(price=price, current_index=current_index, sl=sl, tp=tp, final_sl=final_sl,
                            final_tp=final_tp)
    # Buying logic
    elif len(self.data) > 5 and price > self.data.period_high[-5]:
        if not self.position.is_long:
            sl, tp, final_sl, final_tp = calc_trade_limits(last_open=float(self.data.Open[-1]),
                                                           last_close=float(self.data.Close[-1]), pos=buy,
                                                           price=price,
                                                           risk_to_reward=self.risk_to_reward)
            self.buy_trade(price=price, current_index=current_index, sl=sl, tp=tp, final_sl=final_sl,
                           final_tp=final_tp)


def order_placing_logic(df):
    current_candle = df.iloc[-1]
    current_index = current_candle["index"]
    price = float(current_candle["Close"])
    position = current_candle["position"]

    if check_if_trade_was_active_in_prev_section(current_index=current_index):
        if position:
            df.at[df.index[current_index], "position_active"] = 1
        else:
            # TODO experimenting trading with trend and without
            # self.trade_with_trend(price, current_index)
            trade_without_trend(
                df=df,
                current_candle=current_candle,
                price=price,
                current_index=current_index,
                position=position,
            )


def update_df_for_new_candle(file_name, historical_data_list):
    df = pd.read_csv(file_name)
    df["Datetime"] = pd.to_datetime(df["Datetime"])

    drop_unnamed_cols_in_df(df=df)
    for current_candle_dict in historical_data_list:
        print(f"type of date: {current_candle_dict['date'].minute}")
        if df.iloc[-1].Datetime.minute < current_candle_dict["date"].minute:
            print("new row to be created")
            df = create_new_row_in_df(df=df, current_candle_dict=current_candle_dict)
            mark_candle_pattern_column(df)
            data_with_signals(df, file_name)
            order_placing_logic(df.iloc[-20:])


def fetch_quote():
    global symbol_name
    current_time = datetime.now()
    quotes = kt.quote(["NSE:PFC"])
    for symbol in quotes.keys():
        symbol_name = symbol
        print(f"current_time: {current_time}")
        file_name = get_file_name(symbol=symbol)
        symbol_data = quotes[symbol]

        if 0 < current_time.second <= 2:
            from_datetime = datetime.now() - timedelta(minutes=1)  # From last & days
            to_datetime = datetime.now()
            trading_symbol = symbol.split(":")[1]
            instrument_token_series = get_instrument_token(trading_symbol)
            instrument_token = instrument_token_series.to_list()[0]
            historical_data_list = kt.historical_data(
                instrument_token=instrument_token,
                from_date=from_datetime,
                to_date=to_datetime,
                interval="minute",
            )
            candle_1m_start_close = symbol_data["ohlc"]["close"]
            update_df_for_new_candle(
                file_name=file_name,
                historical_data_list=historical_data_list,
            )


schedule.every(1).seconds.do(fetch_quote)

while True:
    schedule.run_pending()
    time.sleep(1)
