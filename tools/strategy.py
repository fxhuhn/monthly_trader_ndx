import pandas as pd


def strategy(df, regime, last_ticker) -> pd.DataFrame:
    MAX_TICKER = 5

    print(
        f"market breath : {regime.breath_fast} > {regime.breath_slow} {regime.breath_fast>regime.breath_slow}"
    )
    print(
        f"market regime : {regime.Close:.1f} > {regime.index_ma:.1f} {regime.Close>regime.index_ma}"
    )
    print(f"market        : {regime.breath} ")

    # ticker = []

    df["ROC"] = df["ROC_1"] + df["ROC_3"] + df["ROC_6"] + df["ROC_12"]

    print(
        df.loc[df["ROC"].nlargest(10).index][
            ["ROC", "ROC_1", "ROC_3", "ROC_6", "ROC_12"]
        ].sort_values("ROC", ascending=False)
    )
    print("---")

    # ticker = list(set(df.index.unique()) - set(ticker))
    if regime.breath_fast > regime.breath_slow and regime.Close > regime.index_ma:
        return df["ROC"].nlargest(MAX_TICKER).index
    else:
        if len(last_ticker) > 0:
            return list(set(df["ROC"].nlargest(MAX_TICKER).index) - set(last_ticker))

    return []
