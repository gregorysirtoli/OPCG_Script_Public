from __future__ import annotations

import os
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

    # carico dati
    cards = load_collection(db, mongo.col_cards, match={"type": "Cards"})
    prices = load_collection(db, mongo.col_prices)

    asof = pd.Timestamp(datetime.now(timezone.utc))

    # prep
    cards_p = prep_cards(cards, asof)
    daily = prep_prices_daily(prices)
    daily = reindex_daily_fill(daily)

    # filtro min history
    daily = filter_min_history(daily, ml.min_history_days)

    # features
    win_ret = {"7d": ml.win_ret_1, "14d": ml.win_ret_2, "28d": ml.win_ret_3, "56d": ml.win_ret_4}
    feat = add_features_daily(daily, win_ret, ml.win_vol, ml.win_mom, ml.win_liq)

    # target 28d
    feat = add_target_28d(feat, ml.horizon_days)

    # serve target per training
    train_df = feat.dropna(subset=["future_ret_28d"]).copy()

    # safety: elimina eventuali valori non finiti rimasti
    train_df = train_df.replace([float("inf"), float("-inf")], pd.NA)
    train_df = train_df.dropna(subset=["future_ret_28d"])

    # join Cards (Prices.itemId -> Cards.id)
    train_df = train_df.merge(
        cards_p[["id", "rarityName", "printing", "color_1", "setId", "alternate", "card_age_weeks"]],
        left_on="itemId",
        right_on="id",
        how="left"
    ).dropna(subset=["id"])

    # clustering DNA
    cluster_pipe, cluster_ids = fit_clusters(
        train_df[["rarityName","printing","color_1","setId","alternate","card_age_weeks"]].copy(),
        n_clusters=ml.n_clusters
    )
    train_df["clusterId"] = cluster_ids.values

    # tier per riga (al tempo t)
    train_df["tier"] = train_df["price"].apply(lambda p: assign_tier(float(p), ml.low_max, ml.mid_max))

    # colonne modello
    cat_cols = ["rarityName", "printing", "color_1", "setId"]
    num_cols = [
        "log_price",
        "ret_7d", "ret_14d", "ret_28d", "ret_56d",
        "vol_28d", "mom_14d",
        "sellers_chg_28d", "listings_chg_28d",
        "price_to_listings", "sellers_to_listings",
        "alternate", "card_age_weeks", "clusterId",
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
