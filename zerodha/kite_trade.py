import requests
import dateutil.parser


class KiteApp:
    def __init__(self, enc_token: str):
        self.headers = {"Authorization": f"enctoken {enc_token}"}
        self.session = requests.session()
        self.root_url = "https://api.kite.trade"
        # self.root_url = "https://kite.zerodha.com/oms"
        self.session.get(self.root_url, headers=self.headers)

    def instruments(self, exchange=None):
        data = self.session.get(
            f"{self.root_url}/instruments", headers=self.headers
        ).text.split("\n")
        Exchange = []
        for i in data[1:-1]:
            row = i.split(",")
            if exchange is None or exchange == row[11]:
                Exchange.append(
                    {
                        "instrument_token": int(row[0]),
                        "exchange_token": row[1],
                        "tradingsymbol": row[2],
                        "name": row[3][1:-1],
                        "last_price": float(row[4]),
                        "expiry": (
                            dateutil.parser.parse(row[5]).date()
                            if row[5] != ""
                            else None
                        ),
                        "strike": float(row[6]),
                        "tick_size": float(row[7]),
                        "lot_size": int(row[8]),
                        "instrument_type": row[9],
                        "segment": row[10],
                        "exchange": row[11],
                    }
                )
        return Exchange

    def quote(self, instruments):
        data = self.session.get(
            f"{self.root_url}/quote", params={"i": instruments}, headers=self.headers
        ).json()["data"]
        return data

    def ltp(self, instruments):
        data = self.session.get(
            f"{self.root_url}/quote/ltp",
            params={"i": instruments},
            headers=self.headers,
        ).json()["data"]
        return data

    def historical_data(
            self, instrument_token, from_date, to_date, interval, continuous=False, oi=False
    ):
        params = {
            "from": from_date,
            "to": to_date,
            "interval": interval,
            "continuous": 1 if continuous else 0,
            "oi": 1 if oi else 0,
        }
        url = f"{self.root_url}/instruments/historical/{instrument_token}/{interval}"
        lst = self.session.get(
            url=url,
            params=params,
            headers=self.headers,
        ).json()
        lst = lst["data"]["candles"]
        records = []
        for i in lst:
            record = {
                "date": dateutil.parser.parse(i[0]),
                "open": i[1],
                "high": i[2],
                "low": i[3],
                "close": i[4],
                "volume": i[5],
            }
            if len(i) == 7:
                record["oi"] = i[6]
            records.append(record)
        return records

    def margins(self):
        margins = self.session.get(
            f"{self.root_url}/user/margins", headers=self.headers
        ).json()["data"]
        return margins

    def orders(self):
        orders = self.session.get(
            f"{self.root_url}/orders", headers=self.headers
        ).json()["data"]
        return orders

    def positions(self):
        positions = self.session.get(
            f"{self.root_url}/portfolio/positions", headers=self.headers
        ).json()["data"]
        return positions

    def place_order(
            self,
            variety,
            exchange,
            tradingsymbol,
            transaction_type,
            quantity,
            product,
            order_type,
            price=None,
            validity=None,
            disclosed_quantity=None,
            trigger_price=None,
            squareoff=None,
            stoploss=None,
            trailing_stoploss=None,
            tag=None,
    ):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]
        order_id = self.session.post(
            f"{self.root_url}/orders/{variety}", data=params, headers=self.headers
        ).json()["data"]["order_id"]
        return order_id

    def modify_order(
            self,
            variety,
            order_id,
            parent_order_id=None,
            quantity=None,
            price=None,
            order_type=None,
            trigger_price=None,
            validity=None,
            disclosed_quantity=None,
    ):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]

        order_id = self.session.put(
            f"{self.root_url}/orders/{variety}/{order_id}",
            data=params,
            headers=self.headers,
        ).json()["data"]["order_id"]
        return order_id

    def cancel_order(self, variety, order_id, parent_order_id=None):
        order_id = self.session.delete(
            f"{self.root_url}/orders/{variety}/{order_id}",
            data={"parent_order_id": parent_order_id} if parent_order_id else {},
            headers=self.headers,
        ).json()["data"]["order_id"]
        return order_id

# from kiteconnect import KiteConnect, KiteTicker
#
#
# class KiteApp(KiteConnect):
#     def __init__(self, api_key, user_id, enctoken):
#         self.api_key = api_key
#         self.user_id = user_id
#         self.enctoken = enctoken
#         self.headers = {
#             "x-kite-version": "3",
#             "Authorization": f"enctoken {self.enctoken}",
#         }
#         super().__init__(api_key)
#
#
# kite = KiteApp(
#     "vE2iW71EmmAsiXd3gAkck33zMGhmsE94",
#     "GA2335",
#     "zmmGUyBJXSMFYVNIaoLXDDfxcz545ZlCwpqtJQ6RDRzwt90%2F9UF91ZbWGv%2BL11RSQ0cfqxSbrlU3vsOWKGgMd7L0Xtk8orwXqBOqbooudVCvuN%2FEXUNELQ%3D%3D",
# )
#
# print(f"kite: {kite}")
# print(kite.ltp("NSE:SBIN"))
