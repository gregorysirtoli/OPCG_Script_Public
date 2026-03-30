from __future__ import annotations

import joblib
import pandas as pd
from datetime import datetime, timezone

from .config import MongoConfig, MLConfig
from .io_mongo import get_db, load_collection, upsert_many
from .dataset import (
    prep_cards, prep_prices_daily, reindex_daily_fill,
    add_features_daily, filter_min_history
)
from .features import assign_tier
from .clustering import predict_clusters
from .modeling import predict_tier_models


def predict_and_store(artifacts_dir: str = "./artifacts", mongo: MongoConfig = MongoConfig(), ml: MLConfig = MLConfig()):
    db = get_db(mongo.uri, mongo.db_name)

    artifacts = joblib.load(f"{artifacts_dir}/optcg_quantile_artifacts.joblib")
    ml_artifact = artifacts.get("ml_config", ml)
    low_min = getattr(ml_artifact, "low_min", 0.5)
    low_max = getattr(ml_artifact, "low_max", 10.0)
    mid_max = getattr(ml_artifact, "mid_max", 100.0)
    high_max = getattr(ml_artifact, "high_max", 750.0)
    tier_models = artifacts["tier_models"]
    cluster_pipe = artifacts["cluster_pipe"]
    cat_cols = artifacts["cat_cols"]
    num_cols = artifacts["num_cols"]

    asof = pd.Timestamp(datetime.now(timezone.utc))

    cards = load_collection(db, mongo.col_cards, match={"type": "Cards"})
    sets = load_collection(db, getattr(mongo, "col_sets", "Sets"))
    prices = load_collection(db, mongo.col_prices)

    cards_p = prep_cards(cards, asof, sets)

    daily = prep_prices_daily(prices)
    daily = reindex_daily_fill(daily, max_ffill_days=ml_artifact.max_ffill_days)
    daily = filter_min_history(daily, ml_artifact.min_history_days)

    win_ret = {
        "7d": ml_artifact.win_ret_1,
        "14d": ml_artifact.win_ret_2,
        "28d": ml_artifact.win_ret_3,
        "56d": ml_artifact.win_ret_4,
    }
    feat = add_features_daily(daily, win_ret, ml_artifact.win_vol, ml_artifact.win_mom, ml_artifact.win_liq)

    latest = feat.sort_values(["itemId", "date"]).groupby("itemId", as_index=False).tail(1)
    latest = latest.merge(
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

    latest["clusterId"] = predict_clusters(
        cluster_pipe,
        latest[[
            "rarityName", "rarityId", "printing", "color_1",
            "setId", "setName", "illustrator", "cardType",
            "subTypes", "attribute",
            "alternate", "cost", "power", "card_age_weeks"
        ]].copy()
    ).values

    latest["tier"] = latest["price"].apply(
        lambda p: assign_tier(float(p), low_max, mid_max, high_max, low_min)
    )
    latest[num_cols] = latest[num_cols].fillna(0)

    preds_docs: list[dict] = []
    all_scored_rows = []

    for tier, models in tier_models.items():
        df_t = latest[latest["tier"] == tier].copy()
        if df_t.empty:
            continue

        pred_map = predict_tier_models(models, df_t)
        for q, arr in pred_map.items():
            pred_map[q] = pd.Series(arr, index=df_t.index)

        df_t["pred_q20_28d"] = pred_map.get(0.2, pd.Series(0, index=df_t.index))
        df_t["pred_q50_28d"] = pred_map.get(0.5, pd.Series(0, index=df_t.index))
        df_t["pred_q80_28d"] = pred_map.get(0.8, pd.Series(0, index=df_t.index))

        all_scored_rows.append(df_t)

        for _, r in df_t.iterrows():
            preds_docs.append({
                "_id": str(r["id"]),
                "cardId": str(r["id"]),
                "asOfDate": asof.to_pydatetime(),
                "tier": r["tier"],
                "clusterId": int(r["clusterId"]),
                "priceNow": float(r["price"]),
                "pred_q20_28d": float(r["pred_q20_28d"]),
                "pred_q50_28d": float(r["pred_q50_28d"]),
                "pred_q80_28d": float(r["pred_q80_28d"]),
                "lastPriceDate": r["date"].to_pydatetime(),
            })

    if preds_docs:
        upsert_many(db, mongo.col_pred, preds_docs, key_fields=["_id"])

    if not all_scored_rows:
        print("Nessuna predizione generata (tier vuoti o dati insufficienti).")
        return

    scored = pd.concat(all_scored_rows, ignore_index=True)
    top_n = 120
    rank_by_tier = {}

    for tier in ["low", "mid", "high", "grail"]:
        df_t = scored[scored["tier"] == tier].copy()
        if df_t.empty:
            rank_by_tier[tier] = {"top_up": [], "top_down": []}
            continue

        top_up = df_t.sort_values("pred_q80_28d", ascending=False).head(top_n)["id"].tolist()
        top_down = df_t.sort_values("pred_q20_28d", ascending=True).head(top_n)["id"].tolist()
        rank_by_tier[tier] = {"top_up": top_up, "top_down": top_down}

    top_up_global = scored.sort_values("pred_q80_28d", ascending=False).head(top_n)["id"].tolist()
    top_down_global = scored.sort_values("pred_q20_28d", ascending=True).head(top_n)["id"].tolist()

    rank_doc = {
        "asOfDate": asof.to_pydatetime(),
        "top_up": top_up_global,
        "top_down": top_down_global,
        "byTier": {
            "low": rank_by_tier["low"],
            "mid": rank_by_tier["mid"],
            "high": rank_by_tier["high"],
            "grail": rank_by_tier["grail"],
        },
        "meta": {
            "horizon_days": ml_artifact.horizon_days,
            "min_history_days": ml_artifact.min_history_days,
            "topN_per_tier": top_n,
            "tiers": {
                "low": {"min_inclusive": low_min, "max_exclusive": low_max},
                "mid": {"min_inclusive": low_max, "max_inclusive": mid_max},
                "high": {"min_exclusive": mid_max, "max_exclusive": high_max},
                "grail": {"min_inclusive": high_max},
            },
        },
    }

    db[mongo.col_rank].replace_one(
        {"_id": "latest"},
        {"_id": "latest", **rank_doc},
        upsert=True,
    )

    print("Predizione completata.")
    print("Top up sample:", top_up_global[:5])
    print("Top down sample:", top_down_global[:5])
