import pandas as pd


def strategy(df, spy) -> pd.DataFrame:
    MAX_TICKER = 5

    ticker = []

    df["ROC"] = df["ROC_1"] + df["ROC_3"] + df["ROC_6"] + df["ROC_12"]

    print(
        df.loc[df["ROC"].nlargest(10).index][
            ["ROC", "ROC_1", "ROC_3", "ROC_6", "ROC_12"]
        ].sort_values("ROC")
    )
    print("---")

    # ticker = list(set(df.index.unique()) - set(ticker))
    if spy.SMA > spy.Close:
        return []
    return df["ROC"].nlargest(MAX_TICKER).index
