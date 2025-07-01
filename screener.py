import datetime
import pickle

import pandas as pd
import yfinance as yf
from nasdaq_100_ticker_history import tickers_as_of

from tools import calc
from tools import strategy as momentum


def load_stocks(symbols):
    symbols = [ticker for ticker in symbols if ticker != "GOOG"]  # skip GOOG

    return yf.download(
        symbols + ["QQQ", "SPY"],  # quick & dirty
        start="2015-01-01",
        group_by="ticker",
        rounding=True,
        threads=False,
        auto_adjust=False,
    )


def pre_processing(df: pd.DataFrame) -> pd.DataFrame:
    df = calc.convert_to_multiindex(df)

    # add one day in future
    # df_help = df.copy().reset_index()
    # df_help = df_help[df_help.Date == df_help.Date.max()]
    # df_help.Date = df_help.Date + pd.DateOffset(days=26)
    # df_help = df_help.set_index(["Ticker", "Date"])
    # df = pd.concat([df, df_help])

    df = calc.add_indicator_day(df)

    regime_df = calc.build_regime_df(df)
    regime_df = calc.add_regime_filter(regime_df)
    regime_df = calc.resample_month_regime(regime_df)

    df = calc.resample_month(df)
    df = calc.add_indicator_month(df)
    return df, regime_df


def ndx_100_ticker(year_month: str) -> list:
    # year = 2000 + int(year_month[:2])
    # month = year_month[-2:]
    symbol_date = datetime.datetime.strptime(year_month, "%y-%m").date()

    return sorted(
        list(
            tickers_as_of(
                symbol_date.year,
                symbol_date.month,
                1,  # calendar.monthrange(symbol_date.year, symbol_date.month)[1],
            )
        )
    )


def match_available_ticker(df_ticker: list, ticker: list) -> list:
    return list(set(ticker).intersection(df_ticker))


def backtest(df: pd.DataFrame, regime_df: pd.DataFrame) -> dict():
    trade_ticker = {}

    for year_month in df.reset_index().Month.unique():
        print(f"##  {year_month}")
        available_ticker = ndx_100_ticker(year_month)

        monthly_ticker = match_available_ticker(
            df_ticker=df.reset_index().Ticker.unique(),
            ticker=available_ticker,
        )

        if len(list(set(available_ticker) - set(monthly_ticker))):
            print(
                f"Missing symbols  : {list(set(available_ticker) - set(monthly_ticker))}"
            )

        trade_ticker[year_month] = momentum.strategy(
            df.loc[
                (year_month, monthly_ticker),
                :,
            ]
            .dropna()
            .reset_index()
            .drop("Month", axis=1)
            .set_index("Ticker"),
            regime_df.loc[year_month],
            next(reversed(trade_ticker.values())) if len(trade_ticker) > 0 else [],
        )

    return {year_month: list(ticker) for year_month, ticker in trade_ticker.items()}


def get_nasdaq_symbols() -> list:
    nasdaq_tickers = dict()
    print("Loading NASDAQ tickers from 2017 up to 2026")
    for year in range(2017, 2026, 1):
        for month in range(1, 13, 1):
            symbol_date = datetime.date(year, month, 1)

            nasdaq_tickers[f"{year - 2000}-{month:02}"] = list(
                tickers_as_of(
                    symbol_date.year,
                    symbol_date.month,
                    1,  # calendar.monthrange(symbol_date.year, symbol_date.month)[1],
                )
            )

    all = []

    for value in nasdaq_tickers.values():
        all = all + value
    nasdaq_tickers["all"] = list(set(all))

    return nasdaq_tickers["all"]


def load_ndx_100_stocks(cache: bool = True) -> pd.DataFrame:
    df = None
    if cache:
        try:
            with open("./data/stocks.pkl", "rb") as file:
                df = pickle.load(file)
        except Exception as e:
            print(e)
    if df is None:
        print("Loading NASDAQ 100 stocks from Yahoo Finance")
        df = load_stocks(get_nasdaq_symbols())
        df.to_pickle("./data/stocks.pkl")

    return df.round(2)


def main() -> None:
    stocks = load_ndx_100_stocks()
    stocks, regime = pre_processing(stocks)

    # reduce Data for backtest
    stocks = stocks.loc[stocks.reset_index().Month.unique()[-22:]].ffill()  # 22

    trades = backtest(stocks, regime)

    for year_month, symbols in trades.items():
        trades[year_month].sort()

        if len(symbols) < 10:
            for i in range(len(symbols), 5):
                trades[year_month].append("")

    with open("trades.md", "w") as text_file:
        text_file.write(pd.DataFrame.from_dict(trades).T.to_markdown())


if __name__ == "__main__":
    main()
