import os

import pandas as pd
from dotenv import load_dotenv
from nasdaq_100_ticker_history import tickers_as_of

load_dotenv("strategy.env")


def ema(close: pd.Series, period: int = 200) -> pd.Series:
    return round(
        close.ewm(
            span=period, min_periods=period, adjust=False, ignore_na=False
        ).mean(),
        2,
    )


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
        lambda x: x.rolling(int(os.getenv("BREATH_SMA"))).mean().round(2)
    )
    df["Uptrend"] = df.Close > df.SMA
    return df


def build_regime_df(df: pd.DataFrame) -> pd.DataFrame:
    breath_df = df.reset_index()

    date_tickers = {
        date: tickers_as_of(date.year, date.month, date.day)
        for date in breath_df[breath_df.Date > "2017-01-01"]["Date"].unique()
    }

    for date, tickers in date_tickers.items():
        mask = (breath_df["Date"] == date) & (~breath_df["Ticker"].isin(tickers))
        breath_df.loc[mask, "Uptrend"] = False

    breath = pd.merge(
        breath_df.groupby("Date").agg(Breath=("Uptrend", "sum")),
        df.loc[os.getenv("INDEX")]["Close"],
        left_index=True,
        right_index=True,
    )

    return breath


def add_regime_filter(df: pd.DataFrame) -> pd.DataFrame:
    df["index_ma"] = df.Close.rolling(int(os.getenv("INDEX_SMA"))).mean()
    df["breath_slow"] = ema(df.Breath, int(os.getenv("BREATH_SLOW")))
    df["breath_fast"] = ema(df.Breath, int(os.getenv("BREATH_FAST")))

    return df


def resample_month_regime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()

    df["Month"] = (df["Date"]).dt.strftime("%y-%m")

    df = df.groupby("Month").agg(
        Close=("Close", "last"),
        index_ma=("index_ma", "last"),
        breath_slow=("breath_slow", "last"),
        breath_fast=("breath_fast", "last"),
    )

    return df


def resample_month(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()

    df["Month"] = (df["Date"]).dt.strftime("%y-%m")
    # df["Month"] = (df["Date"] + pd.DateOffset(days=30)).dt.strftime("%y-%m")

    df = df.groupby(["Month", "Ticker"]).agg(
        Date=("Date", "last"),
        Open=("Open", "first"),
        Close=("Close", "last"),
        SMA=("SMA", "last"),
        Uptrend=("Uptrend", "last"),
        # cross=("cross", "last"),
    )

    return df


def add_indicator_month(df: pd.DataFrame) -> pd.DataFrame:
    for interval in [1, 3, 6, 12]:
        df[f"ROC_{interval}"] = df.groupby(level=1)["Close"].transform(
            lambda x: roc(x, interval).shift(1)
        )

    return df
