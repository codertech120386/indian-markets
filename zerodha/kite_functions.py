import pandas as pd


def get_instrument_token(instrument_name, file_name):
    print("get instrument called")
    df = pd.read_csv(file_name)
    row = df.loc[df["tradingsymbol"] == instrument_name]
    return row["instrument_token"]
