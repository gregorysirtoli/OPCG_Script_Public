from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import timezone

from .features import safe_div

def prep_cards(cards: pd.DataFrame, asof: pd.Timestamp, sets: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Normalizza i campi Cards e aggiunge feature statiche per clustering/modello.
    Se releaseDate manca, usa quello della collection Sets tramite setId.
    """
    df = cards.copy()

    # mappature Sets (se disponibili)
    sets_release = {}
    sets_name = {}
    if sets is not None and not sets.empty:
        sets_release = pd.to_datetime(sets.set_index("id")["releaseDate"], errors="coerce", utc=True).to_dict()
        if "name" in sets.columns:
            sets_name = sets.set_index("id")["name"].to_dict()

    def first_or_empty(x):
        if isinstance(x, list):
            return x[0] if x else ""
        return x if x is not None else ""

    def list_to_key(x):
        if isinstance(x, list):
            vals = [str(v).strip() for v in x if v]
            return "|".join(sorted(set(vals)))
        return str(x).strip() if x is not None else ""

    df["id"] = df.get("id")
    df["rarityName"] = df.get("rarityName", "").fillna("")
    df["rarityId"] = df.get("rarityId", "").fillna("")
    df["printing"] = df.get("printing", "").fillna("")
    df["setId"] = df.get("setId", "").fillna("")
    df["setName"] = df.get("setName", "").fillna("").replace("", None)
    df["illustrator"] = df.get("illustrator", "").fillna("")
    df["cardType"] = df.get("cardType", "").apply(first_or_empty).fillna("")
    df["subTypes"] = df.get("subTypes", "").apply(list_to_key)
    df["attribute"] = df.get("attribute", "").apply(list_to_key)
    df["alternate"] = pd.to_numeric(df.get("alternate"), errors="coerce").fillna(0).astype(int)
    df["cost"] = pd.to_numeric(df.get("cost"), errors="coerce").fillna(0)
    df["power"] = pd.to_numeric(df.get("power"), errors="coerce").fillna(0)

    # color è spesso lista
    df["color_1"] = df.get("color", "").apply(first_or_empty)

    # setName fallback da Sets se manca
    df["setName"] = df["setName"].fillna(df["setId"].map(sets_name)).fillna("")

    # releaseDate: preferisci quello della carta, altrimenti quello del set
    rd_cards = pd.to_datetime(df.get("releaseDate", pd.NaT), errors="coerce", utc=True)
    rd_sets = df["setId"].map(sets_release)
    rd = rd_cards.fillna(rd_sets)

    if asof.tzinfo is None:
        asof = asof.tz_localize(timezone.utc)
    age_days = (asof.tz_convert("UTC") - rd).dt.days
    df["card_age_weeks"] = (age_days / 7.0).clip(lower=0).fillna(0)

    return df

def prep_prices_daily(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices.copy()

    # campi visti nel tuo esempio :contentReference[oaicite:4]{index=4}
    df["createdAt"] = pd.to_datetime(df["createdAt"], errors="coerce", utc=True)
    df = df.dropna(subset=["itemId", "createdAt"])

    price_fields = [
        "pricePrimary",
        "pricePriceCharting",
        "cmPriceAvg",
        "cmPriceLow",
        "cmAvg7d",
        "cmPriceTrend",
        "cmAvg30d",
        "priceUngraded",
        "cmAvg1d",
    ]

    # converte tutti a numerico
    for f in price_fields:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce")
        else:
            df[f] = pd.NA

    # media dinamica: somma dei valori validi / conteggio valori validi
    price_matrix = df[price_fields].astype("float64")

    valid = (price_matrix.notna()) & (price_matrix > 0)
    sum_prices = price_matrix.where(valid).sum(axis=1, skipna=True)
    cnt_prices = valid.sum(axis=1)

    df["price"] = (sum_prices / cnt_prices).where(cnt_prices > 0)

    # spread proxy: max - min tra i prezzi validi (stessa logica del valid)
    df["price_min"] = price_matrix.where(valid).min(axis=1, skipna=True)
    df["price_max"] = price_matrix.where(valid).max(axis=1, skipna=True)
    df["spread"] = (df["price_max"] - df["price_min"]).where(cnt_prices > 0)

    df["sellers"] = pd.to_numeric(df.get("sellers"), errors="coerce")
    df["listings"] = pd.to_numeric(df.get("listings"), errors="coerce")
    df = df.dropna(subset=["price"])

    df["date"] = df["createdAt"].dt.floor("D")

    # ultimo record del giorno per itemId/date
    df = df.sort_values(["itemId", "createdAt"])
    daily = df.groupby(["itemId", "date"], as_index=False).tail(1)

    daily = daily[["itemId", "date", "price", "sellers", "listings", "spread"]].sort_values(["itemId", "date"])

    return daily

def reindex_daily_fill(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Per ogni itemId: reindex a frequenza giornaliera continua e forward-fill.
    Questo rende coerenti finestre 7/14/28 giorni anche con buchi.
    """
    out = []
    for item_id, g in daily.groupby("itemId", sort=False):
        g = g.sort_values("date").set_index("date")
        idx = pd.date_range(g.index.min(), g.index.max(), freq="D", tz="UTC")
        g2 = g.reindex(idx)
        g2["itemId"] = item_id

        # forward fill prezzo e liquidità
        g2["price"] = g2["price"].ffill()
        g2["sellers"] = g2["sellers"].ffill()
        g2["listings"] = g2["listings"].ffill()
        g2["spread"] = g2["spread"].ffill()

        g2 = g2.dropna(subset=["price"])  # sicurezza
        g2 = g2.reset_index().rename(columns={"index": "date"})
        out.append(g2)

    if not out:
        return daily
    return pd.concat(out, ignore_index=True)

def add_features_daily(df: pd.DataFrame, win_ret: dict, win_vol: int, win_mom: int, win_liq: int) -> pd.DataFrame:
    """
    df: itemId, date (daily), price, sellers, listings
    """
    d = df.copy()
    d = d.sort_values(["itemId", "date"])

    d["log_price"] = np.log1p(d["price"])
    g = d.groupby("itemId", group_keys=False)

    # returns su orizzonti temporali
    d["ret_7d"]  = g["price"].pct_change(win_ret["7d"], fill_method=None)
    d["ret_14d"] = g["price"].pct_change(win_ret["14d"], fill_method=None)
    d["ret_28d"] = g["price"].pct_change(win_ret["28d"], fill_method=None)
    d["ret_56d"] = g["price"].pct_change(win_ret["56d"], fill_method=None)

    # volatilità su log returns
    d["log_ret_1d"] = g["log_price"].diff(1)
    d["vol_28d"] = g["log_ret_1d"].rolling(win_vol).std().reset_index(level=0, drop=True)

    # --- Spread (se esistono i campi nel daily)
    # spread arriva già da prep_prices_daily() (proxy max-min sui price_fields)
    # se manca per qualche riga, resta NaN e poi verrà fillna(0) nel training/predict
    if "spread" not in d.columns:
        d["spread"] = np.nan

    # --- Liquidity index: listings / price
    d["liq_index"] = safe_div(d["listings"], d["price"])

    # --- Shock indicator: movimento anomalo vs volatilità recente
    eps = 1e-9
    d["ret_1d"] = g["price"].pct_change(1, fill_method=None)
    d["shock"] = (d["ret_1d"].abs() / (d["vol_28d"].abs() + eps))

    # (opzionale) clamp shock per evitare outlier estremi
    d["shock"] = d["shock"].clip(0, 50)


    # momentum
    d["mom_14d"] = g["log_ret_1d"].rolling(win_mom).mean().reset_index(level=0, drop=True)

    # liquidità
    d["sellers_chg_28d"] = g["sellers"].pct_change(win_liq, fill_method=None)
    d["listings_chg_28d"] = g["listings"].pct_change(win_liq, fill_method=None)

    d["price_to_listings"] = safe_div(d["price"], d["listings"])
    d["sellers_to_listings"] = safe_div(d["sellers"], d["listings"])

    d = d.replace([np.inf, -np.inf], np.nan)
    return d

def add_target_28d(d: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """
    target date-based: future_ret_28d usando shift(-horizon_days) dopo reindex giornaliero
    + protezioni anti-inf/outlier
    """
    df = d.copy()
    df = df.sort_values(["itemId", "date"])
    g = df.groupby("itemId", group_keys=False)

    df["future_price"] = g["price"].shift(-horizon_days)

    # Protezioni: prezzi non validi
    df = df[(df["price"] > 0) & (df["future_price"].isna() | (df["future_price"] > 0))]

    df["future_ret_28d"] = (df["future_price"] - df["price"]) / df["price"]

    # rimuovi inf e valori fuori scala
    df["future_ret_28d"] = df["future_ret_28d"].replace([np.inf, -np.inf], np.nan)

    # clamp robusto (evita target assurdi tipo +50000%)
    df["future_ret_28d"] = df["future_ret_28d"].clip(lower=-0.95, upper=5.0)

    return df

def filter_min_history(df: pd.DataFrame, min_days: int) -> pd.DataFrame:
    # richiediamo almeno min_days punti daily dopo reindex (quindi ~ giorni reali)
    counts = df.groupby("itemId")["date"].nunique()
    valid = set(counts[counts >= min_days].index)
    return df[df["itemId"].isin(valid)].copy()
