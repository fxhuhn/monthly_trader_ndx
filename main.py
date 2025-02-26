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
    """
    df_help = df.copy().reset_index()
    df_help = df_help[df_help.Date == df_help.Date.max()]
    df_help.Date = df_help.Date + pd.DateOffset(days=25)
    df_help = df_help.set_index(["Ticker", "Date"])
    df = pd.concat([df, df_help])
    """

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


def backtest(
    df: pd.DataFrame, regime_df: pd.DataFrame
):  # -> tuple(pd.DataFrame, float):
    trade_ticker = {}
    start = 10_000
    change_matrix = []
    depot = []

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

    for year_month, ticker in trade_ticker.items():
        trades = df.loc[(year_month, ticker), ["Open", "Close"]]
        if len(trades) > 0:
            trades = trades.round({"Open": 2, "Close": 2, "Profit": 1, "Gewinn": 2})

            trades["Profit"] = (trades.Close - trades.Open) / trades.Open * 100
            trades["qty"] = (start / len(trades)) // trades.Open
            trades["Gewinn"] = (trades.Close - trades.Open) * trades.qty
            gewinn = trades.Gewinn.sum()

            trades = trades.round({"Open": 2, "Close": 2, "Profit": 1, "Gewinn": 2})
            trades.sort_values("Ticker").dropna().to_csv(
                f"./data/trades/{year_month}.csv", header=True, mode="w"
            )

            change_matrix = change_matrix + [
                list((year_month[:2], year_month[-2:], trades.Profit.mean()))
            ]

            start = (
                start
                - (trades.Open * trades.qty).sum()
                + (trades.Close * trades.qty).sum()
            )
        else:
            gewinn = 0
        depot = depot + [{"year_month": year_month, "depot": start, "monthly": gewinn}]

    change_matrix = pd.DataFrame(
        change_matrix, columns=["Year", "Month", "Change"]
    ).set_index(["Year", "Month"])

    pd.DataFrame(depot).round(2).to_csv(
        "./data/depot.csv", header=True, mode="w", index=False
    )

    return change_matrix, start


def get_nasdaq_symbols() -> list:
    nasdaq_tickers = dict()
    for year in range(2016, 2026, 1):
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
        except Exception:
            df = load_stocks(get_nasdaq_symbols())
            df.to_pickle("./data/stocks.pkl")
    if df is None:
        df = load_stocks(get_nasdaq_symbols())
        df.to_pickle("./data/stocks.pkl")

    return df.round(2)


def main() -> None:
    stocks = load_ndx_100_stocks()
    stocks, regime = pre_processing(stocks)

    # reduce Data for backtest
    stocks = stocks.loc[
        stocks.reset_index().Month.unique()[-82:]
    ]  # 11:166, 18:82, 21:46, 23:22

    trade_matrix, profit = backtest(stocks, regime)

    output = trade_matrix.unstack(level=1)
    output.loc[:, "Average"] = output.mean(axis=1)
    output.loc["Average", :] = output.mean(axis=0)
    # output.loc[:, "Yearly"] = output.Average.mul(12)
    output.loc[:, "Yearly"] = output.Average.mul(output.count(axis=1) - 1)
    # calc yearly average
    output.loc["Average", "Yearly"] = output.loc[:, "Yearly"][:-1].mean()
    output = output.round(2)

    with open("matrix.md", "w") as text_file:
        text_file.write(
            output.to_markdown(floatfmt=".2f")
            .replace("(", " ")
            .replace(")", " ")
            .replace("'", " ")
            .replace(" ,", "  ")
            .replace("nan", "   ")
            .replace("Change", "      ")
        )
    print(output)

    print(f"{profit:,.0f}")


if __name__ == "__main__":
    main()
