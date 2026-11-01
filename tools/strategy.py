import pandas as pd


def strategy(df, regime, last_ticker) -> pd.DataFrame:
    MAX_TICKER = 5

    print(f"market        : {regime.breath} ")
    print(
        f"market breath : {regime.breath_fast:.1f} > {regime.breath_slow:.1f} {regime.breath_fast > regime.breath_slow}"
    )
    print(
        f"market regime : {regime.Close:.1f} > {regime.index_ma:.1f} {regime.Close > regime.index_ma}"
    )

    # ticker = []

    df["ROC"] = df["ROC_1"] + df["ROC_3"] + df["ROC_6"] + df["ROC_12"]

    print("--- Demo")
    print(
        df.loc[df["ROC"].nlargest(10).index][
            ["ROC", "ROC_1", "ROC_3", "ROC_6", "ROC_12"]
        ].sort_values("ROC", ascending=False)
    )
    print("---")

    BUFFER_RANK = MAX_TICKER + 1  # Platz 6: Puffer-Schwelle für bestehende Werte

    ranked = df.sort_values("ROC", ascending=False)
    top_buffer_tickers = set(ranked.head(BUFFER_RANK).index)

    # 1. Bestehende Werte behalten, wenn sie noch in den Top 6 sind
    keep = [t for t in last_ticker if t in top_buffer_tickers][:MAX_TICKER]

    ticker = []
    if regime.breath_fast > regime.breath_slow and regime.Close > regime.index_ma:
        ticker = list(keep)
        # 2. Im Bullenmarkt: Freie Plätze von oben nach unten bis MAX_TICKER (5) auffüllen
        for t in ranked.index:
            if len(ticker) >= MAX_TICKER:
                break
            if t not in ticker:
                ticker.append(t)
    else:
        # Im Bärenmarkt: Nur bestehende Positionen halten (keine Neukäufe)
        ticker = keep

    print("")
    """
    print(
        df[df.index.isin(ticker)][
            ["ROC", "ROC_1", "ROC_3", "ROC_6", "ROC_12"]
        ].sort_values("ROC", ascending=False)
    )
    """
    print(df[df.index.isin(ticker)].sort_values("ROC", ascending=False))
    print("---")

    return ticker
