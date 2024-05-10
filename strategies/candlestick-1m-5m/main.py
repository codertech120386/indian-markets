import multiprocessing
import numpy as np
from backtesting import Backtest, Strategy
from algo_trading_helpers import KiteApp, data_for_1m_5m_scalping_with_signals, drop_unnamed_cols_in_df, \
    calculate_size_of_trade, update_df_after_trade, calc_trade_limits

from config import NSE_INSTRUMENTS_LIST, cash, enc_token
from data_processing import get_data

multiprocessing.set_start_method("fork")

period = "5d"
interval = "1m"
buy = "buy"
sell = "sell"


def test_stocks():
    for trading_symbol in NSE_INSTRUMENTS_LIST:
        kt = KiteApp(enc_token=enc_token)
        data, file_name = get_data(
            kt=kt,
            symbol=trading_symbol,
            period=period,
            interval=interval,
        )
        data = data_for_1m_5m_scalping_with_signals(df=data, file_name=file_name)

        class ScalpingStrategy(Strategy):
            sl_number = 0.0033
            tp_number = 0.00375
            risk_to_reward = 1.5
            file_name = None
            start_index = 0

            def init(self):
                self.start_index = 0

            def check_if_trade_was_active_in_prev_section(self, current_index):
                if len(self.data) > 5:
                    if (current_index + 1) % 5 == 0:
                        self.start_index = current_index
                    # if (
                    #         self.data.position_active[
                    #         self.start_index - 5: self.start_index
                    #         ].sum()
                    #         == 0
                    #         and self.data.position_active[
                    #             self.start_index: current_index
                    #             ].sum()
                    #         == 0
                    # ):
                    if (-1 not in self.data.position_active[self.start_index - 5: self.start_index] and
                            1 not in self.data.position_active[self.start_index - 5: self.start_index] and
                            -1 not in self.data.position_active[self.start_index: self.start_index] and
                            1 not in self.data.position_active[self.start_index: self.start_index]):
                        return True
                    return False
                return True

            def trade_without_trend(self, price: float, current_index: int):
                # Selling logic
                if len(self.data) > 5 and (price < self.data.period_low[-5]):
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

            def trade_with_trend(self, price: float, current_index: int):
                # Selling logic
                if (
                        len(self.data) > 5
                        and price < self.data.period_low[-5]
                        and self.data.trend[-1] == -1
                        and self.data.category[-1] == -1
                ):
                    if not self.position.is_short:
                        self.sell_trade(price=price, current_index=current_index)
                # Buying logic
                elif (
                        len(self.data) > 5
                        and price > self.data.period_high[-5]
                        and self.data.trend[-1] == 1
                        and self.data.category[-1] == 1
                ):
                    self.buy_trade(price=price, current_index=current_index)

            @staticmethod
            def update_dataframe_after_trade(pos: str, current_index, final_sl, final_tp):
                nonlocal data
                return update_df_after_trade(df=data, pos=pos, final_sl=final_sl,
                                             final_tp=final_tp, current_index=current_index)

            def sell_trade(self, price: float, current_index: int, sl: float, tp: float, final_sl: float,
                           final_tp: float):
                if self.price_conditions_for_trade_are_met(pos=sell, final_sl=final_sl, final_tp=final_tp, price=price):
                    size = calculate_size_of_trade(cash=cash, sl=sl, price=price)
                    self.sell(size=size, sl=final_sl, tp=final_tp)
                    self.update_dataframe_after_trade(pos=sell, current_index=current_index, final_sl=final_sl,
                                                      final_tp=final_tp)

            def buy_trade(self, price: float, current_index, sl: float, tp: float, final_sl: float,
                          final_tp: float):
                if self.price_conditions_for_trade_are_met(pos=buy, final_sl=final_sl, final_tp=final_tp, price=price):
                    size = calculate_size_of_trade(cash=cash, sl=sl, price=price)
                    self.buy(size=size, sl=final_sl, tp=final_tp)
                    self.update_dataframe_after_trade(pos=buy, current_index=current_index, final_sl=final_sl,
                                                      final_tp=final_tp)

            def price_conditions_for_trade_are_met(
                    self, pos: str, price: float, final_sl: float, final_tp: float
            ):
                if pos == sell:
                    # checking that final_sl is greater than sl number else we might not be able to beat even commissions
                    # and also that candle_stick pattern is in favour of that trade

                    return (
                            final_sl > (price + (price * self.sl_number))
                            and self.data.candle_pattern[-1] == -1
                    )
                # checking that final_sl is less than sl number else we might not be able to beat even commissions
                # and also that candle_stick pattern is in favour of that trade
                return (
                        final_sl < (price - (price * self.sl_number))
                        and self.data.candle_pattern[-1] == 1
                )

            def next(self):
                current_index = self.data.index[-1]
                price = float(self.data.Close[-1])

                if self.position.is_long:
                    data.at[data.index[current_index], "position_active"] = 1
                elif self.position.is_short:
                    data.at[data.index[current_index], "position_active"] = -1
                else:
                    if self.check_if_trade_was_active_in_prev_section(
                            current_index=current_index
                    ):
                        # self.trade_with_trend(price, current_index)
                        self.trade_without_trend(price, current_index)

        drop_unnamed_cols_in_df(data)
        bt = Backtest(data, ScalpingStrategy, cash=cash)

        # param_grid = {
        #     "sl_number": list(np.arange(0.0020, 0.0100, 0.0001)),
        #     "tp_number": list(np.arange(0.0025, 0.0100, 0.0001)),
        # }
        # res = bt.optimize(**param_grid, maximize="Return [%]")
        #
        # print(f"trades: {res['_trades'].to_string()}")
        # print(f"res: {res}")

        # performance_dict = {
        #     "trading_symbol": trading_symbol,
        #     "trades": int(res["# Trades"]),
        #     "% return": round(res["Return [%]"], 2),
        #     "Win [%]": round(res["Win Rate [%]"], 2),
        #     "sl number": res["_strategy"].sl_number,
        #     "tp number": res["_strategy"].tp_number,
        #     "Max. Drawdown [%]": round(res["Max. Drawdown [%]"], 2),
        #     "Avg. Drawdown [%]": round(res["Avg. Drawdown [%]"], 2),
        #     "Equity Peak [$]": round(res["Equity Peak [$]"], 2),
        #     "Equity Final [$]": round(res["Equity Final [$]"], 2),
        # }
        #
        # print(f"performance dict: {performance_dict}")

        stats = bt.run()
        print(f"stats: {stats}")
        print(f"trades: {stats._trades}")
        data.to_csv(file_name)
        # bt.plot()


test_stocks()
