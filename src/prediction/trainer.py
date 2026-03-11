from __future__ import annotations

import os
import time
import joblib
import pandas as pd
from datetime import datetime, timezone

from .config import MongoConfig, MLConfig
from .io_mongo import get_db, load_collection
from .dataset import (
    prep_cards, prep_prices_daily, reindex_daily_fill,
    add_features_daily, add_target_28d, filter_min_history
)
from .clustering import fit_clusters
from .features import assign_tier
from .modeling import fit_tier_models, TierModels


def train_all(artifacts_dir: str = "./artifacts", mongo: MongoConfig = MongoConfig(), ml: MLConfig = MLConfig()):
    os.makedirs(artifacts_dir, exist_ok=True)
    db = get_db(mongo.uri, mongo.db_name)

    t0 = time.time()
    def log_step(msg: str):
        elapsed = time.time() - t0
        print(f"[train] +{elapsed:6.1f}s {msg}")

    asof = pd.Timestamp(datetime.now(timezone.utc))

    # carico dati (limitati nel tempo e nei campi per velocizzare)
    date_from = asof - pd.Timedelta(days=400)
    cards = load_collection(
        db,
        mongo.col_cards,
        match={"type": "Cards"},
        projection={
            "id": 1,
            "rarityName": 1,
            "rarityId": 1,
            "printing": 1,
            "color": 1,
            "setId": 1,
            "setName": 1,
            "illustrator": 1,
            "cardType": 1,
            "subTypes": 1,
            "attribute": 1,
            "alternate": 1,
            "cost": 1,
            "power": 1,
            "releaseDate": 1,
        },
    )
    sets = load_collection(
        db,
        getattr(mongo, "col_sets", "Sets"),
        projection={"id": 1, "releaseDate": 1, "name": 1},
    )
    prices = load_collection(
        db,
        mongo.col_prices,
        match={"createdAt": {"$gte": date_from.to_pydatetime()}},
        projection={
            "itemId": 1,
            "createdAt": 1,
            "pricePrimary": 1,
            "pricePriceCharting": 1,
            "cmPriceAvg": 1,
            "cmPriceLow": 1,
            "cmAvg7d": 1,
            "cmPriceTrend": 1,
            "cmAvg30d": 1,
            "priceUngraded": 1,
            "cmAvg1d": 1,
            "sellers": 1,
            "listings": 1,
            "spread": 1,
        },
    )
    log_step(f"load cards={len(cards):,} sets={len(sets):,} prices={len(prices):,} (from {date_from.date()} to {asof.date()})")

    # prep
    cards_p = prep_cards(cards, asof, sets)
    log_step("prep_cards done")
    daily = prep_prices_daily(prices)
    log_step(f"prep_prices_daily rows={len(daily):,}")
    daily = reindex_daily_fill(daily)
    log_step(f"reindex_daily_fill rows={len(daily):,}")

    # filtro min history
    daily = filter_min_history(daily, ml.min_history_days)
    log_step(f"filter_min_history rows={len(daily):,}")

    # features
    win_ret = {"7d": ml.win_ret_1, "14d": ml.win_ret_2, "28d": ml.win_ret_3, "56d": ml.win_ret_4}
    feat = add_features_daily(daily, win_ret, ml.win_vol, ml.win_mom, ml.win_liq)
    log_step(f"add_features_daily rows={len(feat):,}")

    # target 28d
    feat = add_target_28d(feat, ml.horizon_days)
    log_step(f"add_target_28d rows={len(feat):,}")

    # serve target per training
    train_df = feat.dropna(subset=["future_ret_28d"]).copy()
    log_step(f"dropna target rows={len(train_df):,}")

    # safety: elimina eventuali valori non finiti rimasti
    train_df = train_df.replace([float("inf"), float("-inf")], pd.NA)
    train_df = train_df.dropna(subset=["future_ret_28d"])
    log_step(f"clean inf/NaN rows={len(train_df):,}")

    # join Cards (Prices.itemId -> Cards.id)
    train_df = train_df.merge(
        cards_p[[
            "id", "rarityName", "rarityId", "printing", "color_1",
            "setId", "setName", "illustrator", "cardType",
            "subTypes", "attribute",
            "alternate", "cost", "power", "card_age_weeks"
        ]],
        left_on="itemId",
        right_on="id",
        how="left"
    ).dropna(subset=["id"])
    log_step(f"merge cards rows={len(train_df):,}")

    # clustering DNA
    cluster_pipe, cluster_ids = fit_clusters(
        train_df[[
            "rarityName","rarityId","printing","color_1",
            "setId","setName","illustrator","cardType",
            "subTypes","attribute",
            "alternate","cost","power","card_age_weeks"
        ]].copy(),
        n_clusters=ml.n_clusters
    )
    train_df["clusterId"] = cluster_ids.values
    log_step("fit_clusters done")

    # tier per riga (al tempo t)
    train_df["tier"] = train_df["price"].apply(lambda p: assign_tier(float(p), ml.low_max, ml.mid_max))

    # colonne modello
    cat_cols = [
        "rarityName", "rarityId", "printing", "color_1",
        "setId", "setName", "illustrator", "cardType",
        "subTypes", "attribute"
    ]
    num_cols = [
        "log_price",
        "ret_7d", "ret_14d", "ret_28d", "ret_56d",
        "vol_28d", "mom_14d",
        "sellers_chg_28d", "listings_chg_28d",
        "price_to_listings", "sellers_to_listings",
        "alternate", "cost", "power", "card_age_weeks", "clusterId",
        "spread", "liq_index", "shock"
    ]

    # pulizia NaN numerici (OneHotEncoder gestisce cat)
    train_df[num_cols] = train_df[num_cols].fillna(0)

    tier_models: dict[str, TierModels] = {}
    for tier in ["low", "mid", "high"]:
        df_t = train_df[train_df["tier"] == tier].copy()
        if len(df_t) < 200:
            # evita training su tier troppo piccoli
            continue
        log_step(f"fit_tier_models tier={tier} rows={len(df_t):,}")
        tier_models[tier] = fit_tier_models(
            df=df_t,
            y_col="future_ret_28d",
            cat_cols=cat_cols,
            num_cols=num_cols,
            quantiles=ml.quantiles
        )

    artifacts = {
        "asof": asof.to_pydatetime(),
        "ml_config": ml,
        "mongo_config": mongo,
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "tier_models": tier_models,
        "cluster_pipe": cluster_pipe,
    }

    joblib.dump(artifacts, os.path.join(artifacts_dir, "optcg_quantile_artifacts.joblib"))
    print(f"✅ Training completato. Salvato in {os.path.join(artifacts_dir, 'optcg_quantile_artifacts.joblib')}")
