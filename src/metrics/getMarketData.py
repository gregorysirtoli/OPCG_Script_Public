from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from pymongo.database import Database

GRADING_FEES = 30

def _to_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict) and "$numberDecimal" in v:
        try:
            return float(v["$numberDecimal"])
        except Exception:
            return None
    try:
        return float(v)
    except Exception:
        return None

def _round2(n: Optional[float]) -> Optional[float]:
    if n is None:
        return None
    if not isinstance(n, (int, float)):
        return None
    return float(f"{n:.2f}")

def _calc_pct_change(now: Optional[float], base: Optional[float]) -> Optional[float]:
    if now is None or base is None or base == 0:
        return None
    return _round2(((now - base) / base) * 100.0)

def _best_usd(doc: Dict[str, Any]) -> Optional[float]:
    if not doc:  # None or {}
        return None
     
    # All prices in USD. If no pricePrimary, use priority fallback.
    for k in ("cmPriceTrend", "priceUngraded", "pricePriceCharting",
              "cmPriceAvg", "cmAvg1d", "cmAvg7d", "cmAvg30d", "cmPriceLow"):
        v = _to_number(doc.get(k))
        if v is not None:
            return v
    return None

@dataclass
class SeriesInfo:
    price_now_usd: Optional[float] # USD
    used_primary: bool # true if pricePrimary

@dataclass
class Baselines:
    b1: Optional[float]
    b7: Optional[float]
    b30: Optional[float]
    b90: Optional[float]
    b180: Optional[float]
    b365: Optional[float]

def _get_closest_around(
    prices: List[Dict[str, Any]],
    target_dt: datetime,
    max_days: Optional[float] = None, # es. 3, 7, 30, ecc.
) -> Optional[Dict[str, Any]]:
    closest_doc = None
    closest_diff_days = None

    for p in prices:
        cad = _parse_created_at(p)
        if cad is None:
            continue

        diff_days = abs((cad - target_dt).total_seconds()) / 86400.0

        if max_days is not None and diff_days > max_days:
            continue

        if closest_diff_days is None or diff_days < closest_diff_days:
            closest_doc = p
            closest_diff_days = diff_days

    return closest_doc

def _pick_series_now(latest: Optional[Dict[str, Any]]) -> SeriesInfo:
    # If pricePrimary then "now" otherwise best_usd() (fallback)
    p_primary = _to_number((latest or {}).get("pricePrimary"))
    if p_primary is not None:
        return SeriesInfo(price_now_usd=p_primary, used_primary=True)
    p_fallback = _best_usd(latest or {})
    return SeriesInfo(price_now_usd=p_fallback, used_primary=False)

def _pick_baselines(
    used_primary: bool,
    b1doc: Optional[Dict[str, Any]],
    b7doc: Optional[Dict[str, Any]],
    b30doc: Optional[Dict[str, Any]],
    b90doc: Optional[Dict[str, Any]],
    b180doc: Optional[Dict[str, Any]],
    b365doc: Optional[Dict[str, Any]],
) -> Baselines:
    if used_primary:
        getter = lambda d: _to_number((d or {}).get("pricePrimary"))
    else:
        getter = lambda d: _best_usd(d or {})

    return Baselines(
        b1=getter(b1doc),
        b7=getter(b7doc),
        b30=getter(b30doc),
        b90=getter(b90doc),
        b180=getter(b180doc),
        b365=getter(b365doc),
    )

def _parse_created_at(doc: Dict[str, Any]) -> Optional[datetime]:
    cad = doc.get("createdAt")
    if isinstance(cad, dict) and "$date" in cad:
        try:
            # gestisce ISO + Z
            return datetime.fromisoformat(cad["$date"].replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(cad, datetime):
        return cad
    return None

def _as_number_or_none(v: Any) -> Optional[float]:
    n = _to_number(v)
    return _round2(n) if n is not None else None

def compute_market_data_for_item(prices: List[Dict[str, Any]], graded_first: Dict[str, Any]) -> Dict[str, Any]:
    latest = prices[0] if prices else None

    # Current series (USD)
    s = _pick_series_now(latest)

    # Baselines (USD)
    now = datetime.now(timezone.utc)
    b1doc = _get_closest_around(prices, now - timedelta(days=1), max_days=2)
    b7doc = _get_closest_around(prices, now - timedelta(days=7), max_days=3.5)
    b30doc = _get_closest_around(prices, now - timedelta(days=30), max_days=7)
    b90doc = _get_closest_around(prices, now - timedelta(days=90), max_days=14)
    b180doc = _get_closest_around(prices, now - timedelta(days=180), max_days=21)
    b365doc = _get_closest_around(prices, now - timedelta(days=365), max_days=30)
    baselines = _pick_baselines(s.used_primary, b1doc, b7doc, b30doc,  b90doc, b180doc, b365doc)

    # Prices reference
    price_1d   = _as_number_or_none(baselines.b1)
    price_7d   = _as_number_or_none(baselines.b7)
    price_30d  = _as_number_or_none(baselines.b30)
    price_90d  = _as_number_or_none(baselines.b90)
    price_180d = _as_number_or_none(baselines.b180)
    price_365d = _as_number_or_none(baselines.b365)

    # Delta prices (USD)
    def _delta(now_: Optional[float], base_: Optional[float]) -> Optional[float]:
        if now_ is None or base_ is None:
            return None
        return _round2(now_ - base_)

    price_change_1d   = _delta(s.price_now_usd, baselines.b1)
    price_change_7d   = _delta(s.price_now_usd, baselines.b7)
    price_change_30d  = _delta(s.price_now_usd, baselines.b30)
    price_change_90d  = _delta(s.price_now_usd, baselines.b90)
    price_change_180d = _delta(s.price_now_usd, baselines.b180)
    price_change_365d = _delta(s.price_now_usd, baselines.b365)

    # Delta percent (USD)
    pct1   = _calc_pct_change(s.price_now_usd, baselines.b1)
    pct7   = _calc_pct_change(s.price_now_usd, baselines.b7)
    pct30  = _calc_pct_change(s.price_now_usd, baselines.b30)
    pct90  = _calc_pct_change(s.price_now_usd, baselines.b90)
    pct180 = _calc_pct_change(s.price_now_usd, baselines.b180)
    pct365 = _calc_pct_change(s.price_now_usd, baselines.b365)

    # sellers & listings from doc (latest)
    sellers = latest.get("sellers") if latest else None
    listings = latest.get("listings") if latest else None

    # priceSecondary USD
    price_secondary = None
    for key in ("cmPriceTrend", "cmPriceAvg", "cmAvg1d", "cmAvg7d", "cmAvg30d", "cmPriceLow"):
        v = _to_number((latest or {}).get(key))
        if v is not None:
            price_secondary = v
            break

    # Spread between cmPriceTrend and cmPriceLow
    cm_trend = _to_number((latest or {}).get("cmPriceTrend"))
    cm_low   = _to_number((latest or {}).get("cmPriceLow"))

    if cm_trend is not None and cm_trend > 0 and cm_low is not None:
        # quanto il low è "scontato" rispetto al trend, in %
        low_vs_trend_discount_pct = _round2(((cm_trend - cm_low) / cm_trend) * 100.0)
    else:
        low_vs_trend_discount_pct = None

    # graded (USD)
    psa10_usd = _to_number(graded_first.get("psa10"))
    bsg10_usd = _to_number(graded_first.get("bsg10"))  # error: from "bsg10" to "bgs10", i'm tard!

    # gradingProfit percent (%)
    if s.price_now_usd:
        effective_cost = s.price_now_usd + (GRADING_FEES or 0.0)
        if effective_cost > 0:
            gp_psa10 = _round2(((psa10_usd - effective_cost) / effective_cost) * 100) if psa10_usd is not None else None
            gp_bsg10 = _round2(((bsg10_usd - effective_cost) / effective_cost) * 100) if bsg10_usd is not None else None
        else:
            gp_psa10 = None
            gp_bsg10 = None
    else:
        gp_psa10 = None
        gp_bsg10 = None

    return {
        "createdAt": datetime.now(timezone.utc),
        # ---- output marketData (USD) ----
        "sellers": sellers if sellers is not None else None,
        "listings": listings if listings is not None else None,
        "price": _as_number_or_none(s.price_now_usd), # USD
        "pricePrimary": _as_number_or_none((latest or {}).get("pricePrimary")), # USD
        "priceSecondary": _as_number_or_none(price_secondary), # USD

        "priceTrend": _as_number_or_none(cm_trend), # USD
        "priceLow": _as_number_or_none(cm_low), # USD

        "psa10": _as_number_or_none(psa10_usd), # USD
        "bgs10": _as_number_or_none(bsg10_usd), # USD

        "gradingProfitPsa10": gp_psa10, # %
        "gradingProfitBsg10": gp_bsg10, # %

        # reference prices (baseline)
        "price1d": price_1d,
        "price7d": price_7d,
        "price30d": price_30d,
        "price90d": price_90d,
        "price180d": price_180d,
        "price365d": price_365d,
 
        "priceChange1d": price_change_1d, # USD
        "priceChange7d": price_change_7d, # USD
        "priceChange30d": price_change_30d, # USD
        "priceChange90d": price_change_90d, # USD
        "priceChange180d": price_change_180d, # USD
        "priceChange365d": price_change_365d, # USD

        "percentageChange1d": pct1, # %
        "percentageChange7d": pct7, # %
        "percentageChange30d": pct30, # %
        "percentageChange90d": pct90, # %
        "percentageChange180d": pct180, # %
        "percentageChange365d": pct365, # %

        # spread low vs trend
        "spread": low_vs_trend_discount_pct, # %
    }

def update_cards_market_data(db: Database, days_back: int = 400, limit_ids: Optional[List[str]] = None) -> Tuple[int, int]:
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]

    q_cards: Dict[str, Any] = {}
    if limit_ids:
        q_cards["id"] = {"$in": limit_ids}

    ids = [c["id"] for c in coll_cards.find(q_cards, {"id": 1}) if "id" in c]
    if not ids:
        return (0, 0)

    since = datetime.now(timezone.utc) - timedelta(days=days_back)

    cur = coll_prices.find(
        {"itemId": {"$in": ids}, "createdAt": {"$gte": since}},
        {
            "itemId": 1, "createdAt": 1,
            "pricePrimary": 1,
            "cmPriceTrend": 1, "cmAvg30d": 1, "cmAvg7d": 1, "cmAvg1d": 1, "cmPriceAvg": 1, "cmPriceLow": 1,
            "priceUngraded": 1, "pricePriceCharting": 1,
            "listings": 1, "sellers": 1,
            "psa10": 1, "bsg10": 1,
        }
    ).sort([("itemId", 1), ("createdAt", -1)])

    per_item: Dict[str, List[Dict[str, Any]]] = {}
    for p in cur:
        per_item.setdefault(p["itemId"], []).append(p)

    # graded first (psa10/bsg10) - $first on createdAt desc
    graded_first: Dict[str, Dict[str, Any]] = {}
    cur2 = coll_prices.aggregate([
        {"$match": {"itemId": {"$in": ids}, "$or": [{"psa10": {"$type": "number"}}, {"bsg10": {"$type": "number"}}]}},
        {"$sort": {"itemId": 1, "createdAt": -1}},
        {"$group": {"_id": "$itemId", "psa10": {"$first": "$psa10"}, "bsg10": {"$first": "$bsg10"}}}
    ])
    for g in cur2:
        graded_first[g["_id"]] = {"psa10": g.get("psa10"), "bsg10": g.get("bsg10")}

    from pymongo import UpdateOne
    ops: List[UpdateOne] = []
    touched = 0
    updated = 0
    now = datetime.now(timezone.utc)
    for cid in ids:
        prices = per_item.get(cid, [])
        md = compute_market_data_for_item(prices, graded_first.get(cid, {}))
        touched += 1
        if any(v is not None for v in md.values()):
            ops.append(UpdateOne({"id": cid}, {"$set": {"marketData": md, "updatedAt": now}}))
    if ops:
        res = coll_cards.bulk_write(ops, ordered=False)
        updated = (res.modified_count or 0) + (res.upserted_count or 0)
    return (touched, updated)
