import pandas as pd


def roc(close: pd.Series, period: int = 10) -> pd.Series:
    return (close - close.shift(period)) / close.shift(period) * 100


def convert_to_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    # Stack the data to move the ticker symbols into the index
    df = df.stack(level=0, future_stack=True)
    df.index.names = ["Date", "Ticker"]

    # Set 'Ticker' and 'Date' as multi-index
    return df.reset_index().set_index(["Ticker", "Date"])


def add_indicator_day(df: pd.DataFrame) -> pd.DataFrame:
    df["SMA"] = df.groupby(level=0)["Close"].transform(
        lambda x: x.rolling(200).mean().round(2)
    )
    return df


def resample_month(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()

    df["Month"] = df["Date"].dt.strftime("%y-%m")

    df = df.groupby(["Month", "Ticker"]).agg(
        Date=("Date", "last"),
        Open=("Open", "first"),
        Close=("Close", "last"),
        SMA=("SMA", "last"),
    )

    return df


def add_indicator_month(df: pd.DataFrame) -> pd.DataFrame:
    for interval in [1, 3, 6, 12]:
        df[f"ROC_{interval}"] = df.groupby(level=1)["Close"].transform(
            lambda x: roc(x, interval)  # .shift(1)
        )

    return df
