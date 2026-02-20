from __future__ import annotations

import joblib
import pandas as pd
from datetime import datetime, timezone

from config import MongoConfig, MLConfig
from io_mongo import get_db, load_collection, upsert_many
from dataset import (
    prep_cards, prep_prices_daily, reindex_daily_fill,
    add_features_daily, filter_min_history
)
from features import assign_tier
from clustering import predict_clusters
from modeling import predict_tier_models

def predict_and_store(artifacts_dir: str = "./artifacts", mongo: MongoConfig = MongoConfig(), ml: MLConfig = MLConfig()):
    db = get_db(mongo.uri, mongo.db_name)

    artifacts = joblib.load(f"{artifacts_dir}/optcg_quantile_artifacts.joblib")
    tier_models = artifacts["tier_models"]
    cluster_pipe = artifacts["cluster_pipe"]
    cat_cols = artifacts["cat_cols"]
    num_cols = artifacts["num_cols"]

    asof = pd.Timestamp(datetime.now(timezone.utc))

    cards = load_collection(db, mongo.col_cards, match={"type": "Cards"})
    prices = load_collection(db, mongo.col_prices)

    cards_p = prep_cards(cards, asof)

    daily = prep_prices_daily(prices)
    daily = reindex_daily_fill(daily)
    daily = filter_min_history(daily, ml.min_history_days)

    win_ret = {"7d": ml.win_ret_1, "14d": ml.win_ret_2, "28d": ml.win_ret_3, "56d": ml.win_ret_4}
    feat = add_features_daily(daily, win_ret, ml.win_vol, ml.win_mom, ml.win_liq)

    # prendo l’ultima riga per itemId = stato corrente
    latest = feat.sort_values(["itemId", "date"]).groupby("itemId", as_index=False).tail(1)

    # join Cards
    latest = latest.merge(
        cards_p[["id", "rarityName", "printing", "color_1", "setId", "alternate", "card_age_weeks"]],
        left_on="itemId",
        right_on="id",
        how="left"
    ).dropna(subset=["id"])

    # clusterId
    latest["clusterId"] = predict_clusters(
        cluster_pipe,
        latest[["rarityName","printing","color_1","setId","alternate","card_age_weeks"]].copy()
    ).values

    # tier (basato sul prezzo corrente)
    latest["tier"] = latest["price"].apply(lambda p: assign_tier(float(p), ml.low_max, ml.mid_max))

    # fill numerici
    latest[num_cols] = latest[num_cols].fillna(0)

    # predizioni quantili per tier
    preds_docs: list[dict] = []
    all_scored_rows = []

    for tier, models in tier_models.items():
        df_t = latest[latest["tier"] == tier].copy()
        if df_t.empty:
            continue

        pred_map = predict_tier_models(models, df_t)  # q -> ndarray

        # NO CLIP: mantieni i valori reali del modello
        for q, arr in pred_map.items():
            pred_map[q] = pd.Series(arr, index=df_t.index)
            # pred_map[q] = pd.Series(arr, index=df_t.index).clip(-0.95, 5.0)
            # oppure
            # pred_map[q] = pd.Series(arr, index=df_t.index)
            #pred_map[q] = pred_map[q].clip(
            #    lower=pred_map[q].quantile(0.01),
            #    upper=pred_map[q].quantile(0.99)
            #)

        df_t["pred_q20_28d"] = pred_map.get(0.2, pd.Series(0, index=df_t.index))
        df_t["pred_q50_28d"] = pred_map.get(0.5, pd.Series(0, index=df_t.index))
        df_t["pred_q80_28d"] = pred_map.get(0.8, pd.Series(0, index=df_t.index))

        all_scored_rows.append(df_t)

        for _, r in df_t.iterrows():
            preds_docs.append({
                "_id": str(r["id"]),                 # <-- AGGIUNGI QUESTO
                "cardId": str(r["id"]),              # <-- opzionale (puoi anche toglierlo)
                "asOfDate": asof.to_pydatetime(),
                "tier": r["tier"],
                "clusterId": int(r["clusterId"]),
                "priceNow": float(r["price"]),
                "pred_q20_28d": float(r["pred_q20_28d"]),
                "pred_q50_28d": float(r["pred_q50_28d"]),
                "pred_q80_28d": float(r["pred_q80_28d"]),
                "lastPriceDate": r["date"].to_pydatetime(),
            })


    # upsert predictions (1 doc per cardId)
    SAVE_PREDICTIONS = True
    if SAVE_PREDICTIONS:
        upsert_many(db, mongo.col_pred, preds_docs, key_fields=["_id"])

    if not all_scored_rows:
        print("⚠️ Nessuna predizione generata (tier vuoti o dati insufficienti).")
        return

    scored = pd.concat(all_scored_rows, ignore_index=True)

    TOP_N = 120

    rank_by_tier = {}

    for tier in ["low", "mid", "high"]:
        df_t = scored[scored["tier"] == tier].copy()
        if df_t.empty:
            rank_by_tier[tier] = {"top_up": [], "top_down": []}
            continue

        top_up = df_t.sort_values("pred_q80_28d", ascending=False).head(TOP_N)["id"].tolist()
        top_down = df_t.sort_values("pred_q20_28d", ascending=True).head(TOP_N)["id"].tolist()

        rank_by_tier[tier] = {
            "top_up": top_up,
            "top_down": top_down
        }

    # (opzionale) ranking globale “mix” prendendo i migliori score da tutti
    top_up_global = scored.sort_values("pred_q80_28d", ascending=False).head(TOP_N)["id"].tolist()
    top_down_global = scored.sort_values("pred_q20_28d", ascending=True).head(TOP_N)["id"].tolist()

    rank_doc = {
        "asOfDate": asof.to_pydatetime(),

        # ranking globale (opzionale, puoi anche toglierlo)
        "top_up": top_up_global,
        "top_down": top_down_global,

        # ranking per tier (quello che ti serve)
        "byTier": {
            "low":  {"top_up": rank_by_tier["low"]["top_up"],  "top_down": rank_by_tier["low"]["top_down"]},
            "mid":  {"top_up": rank_by_tier["mid"]["top_up"],  "top_down": rank_by_tier["mid"]["top_down"]},
            "high": {"top_up": rank_by_tier["high"]["top_up"], "top_down": rank_by_tier["high"]["top_down"]},
        },

        "meta": {
            "horizon_days": ml.horizon_days,
            "min_history_days": ml.min_history_days,
            "topN_per_tier": TOP_N,
            "tiers": {
                "low": {"max": ml.low_max},
                "mid": {"max": ml.mid_max},
                "high": {"min_exclusive": ml.mid_max},
            }
        }
    }

    db[mongo.col_rank].replace_one(
        {"_id": "latest"},
        {"_id": "latest", **rank_doc},
        upsert=True
    )

    print("✅ Predizione completata.")
    print("Top up sample:", top_up[:5])
    print("Top down sample:", top_down[:5])
