import pandas as pd
import yfinance as yf

spy = yf.download("spy", start="2000-01-01")
ndx = yf.download("qqq", start="2000-01-01")


depot = pd.read_csv("./data/depot.csv")
depot["Date"] = pd.to_datetime(
    (depot.year_month.str[:2].astype(int) + 2000).astype(str)
    + "-"
    + depot.year_month.str[-2:]
    + "-01"
)
depot.set_index("Date", inplace=True)

spy = spy[depot.iloc[0].name :].copy()
ndx = ndx[depot.iloc[0].name :].copy()

spy["invest_spy"] = spy.Open * (10_000 // spy.Open.values[1])
ndx["invest_ndx"] = ndx.Open * (10_000 // ndx.Open.values[1])

performance = pd.merge(
    left=spy["invest_spy"], right=ndx["invest_ndx"], left_index=True, right_index=True
)

performance = pd.merge(
    left=performance, right=depot["depot"], left_index=True, right_index=True
)

performance.plot().figure.savefig("performance.png")
