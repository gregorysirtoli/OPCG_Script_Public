from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import exp, log1p, sqrt, tanh
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pymongo import UpdateOne
from pymongo.database import Database

GRADING_FEES = 30
SET_TREND_DAYS = 90
SET_PRICE_LOOKBACK_DAYS = max(SET_TREND_DAYS + 15, 45)
SET_TOP_BASE_PRICE = 20.0
SET_PULL_RATES_SEED_PATH = Path(__file__).resolve().parents[2] / "docs" / "set_pull_rates.seed.json"


def _load_pull_rate_seed() -> Dict[Tuple[str, str], Dict[str, Any]]:
    try:
        raw = json.loads(SET_PULL_RATES_SEED_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not isinstance(raw, list):
        return out
    for doc in raw:
        if not isinstance(doc, dict):
            continue
        set_id = doc.get("setId")
        language = doc.get("language")
        if isinstance(set_id, str) and isinstance(language, str):
            out[(set_id.upper(), language.lower())] = doc
        base_set_code = doc.get("baseSetCode")
        if isinstance(base_set_code, str) and isinstance(language, str):
            out[(base_set_code.upper(), language.lower())] = doc
    return out


PULL_RATE_SEED = _load_pull_rate_seed()

def _clamp(n: Optional[float], low: float, high: float) -> Optional[float]:
    if n is None:
        return None
    return max(low, min(high, float(n)))

def _score_0_100(n: Optional[float]) -> Optional[float]:
    n = _clamp(n, 0.0, 100.0)
    return _round2(n)

def _scale_0_100(value: Optional[float], cap: float) -> Optional[float]:
    if value is None or cap <= 0:
        return None
    return _score_0_100((value / cap) * 100.0)

def _compute_liquidity_score(
    sellers: Any,
    listings: Any,
    spread_pct: Optional[float],
) -> Optional[float]:
    sellers_n = _to_number(sellers)
    listings_n = _to_number(listings)

    sellers_score = _scale_0_100(sellers_n, 20.0)
    listings_score = _scale_0_100(listings_n, 40.0)
    spread_score = None
    if spread_pct is not None:
        # lower spread is generally healthier / easier to exit
        spread_score = _score_0_100(100.0 - min(max(spread_pct, 0.0), 40.0) * 2.5)

    weighted_sum = 0.0
    total_weight = 0.0
    for score, weight in (
        (sellers_score, 0.45),
        (listings_score, 0.35),
        (spread_score, 0.20),
    ):
        if score is None:
            continue
        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

def _compute_weighted_momentum(
    pct7: Optional[float],
    pct30: Optional[float],
    pct90: Optional[float],
) -> Optional[float]:
    weighted_sum = 0.0
    total_weight = 0.0
    for value, weight in ((pct7, 0.5), (pct30, 0.3), (pct90, 0.2)):
        if value is None:
            continue
        weighted_sum += value * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

def _compute_momentum_score(weighted_momentum_pct: Optional[float]) -> Optional[float]:
    if weighted_momentum_pct is None:
        return None
    # map roughly -50%..+50% to 0..100 and clamp
    return _score_0_100(((weighted_momentum_pct + 50.0) / 100.0) * 100.0)

def _compute_grading_attractiveness(
    gp_psa10: Optional[float],
    gp_bsg10: Optional[float],
    liquidity_score: Optional[float],
) -> Optional[float]:
    best_profit = max(
        gp_psa10 if gp_psa10 is not None else float("-inf"),
        gp_bsg10 if gp_bsg10 is not None else float("-inf"),
    )
    if best_profit == float("-inf"):
        best_profit = None

    profit_score = None
    if best_profit is not None:
        # cap at 150% upside for scoring
        profit_score = _score_0_100((max(best_profit, 0.0) / 150.0) * 100.0)

    # If we do not have any grading profit reference, this card should not
    # surface as a grading candidate just because it has good liquidity.
    if profit_score is None:
        return None

    weighted_sum = 0.0
    total_weight = 0.0
    for score, weight in ((profit_score, 0.75), (liquidity_score, 0.25)):
        if score is None:
            continue
        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

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


def _safe_round2(n: float) -> float:
    return float(f"{n:.2f}")


def _median(nums: List[float]) -> float:
    if not nums:
        return 0.0
    arr = sorted(nums)
    size = len(arr)
    mid = size // 2
    if size % 2:
        return float(arr[mid])
    return float((arr[mid - 1] + arr[mid]) / 2.0)


def _quantile(nums: List[float], q: float) -> float:
    if not nums:
        return 0.0
    arr = sorted(nums)
    pos = (len(arr) - 1) * q
    base = int(pos)
    rest = pos - base
    nxt = min(len(arr) - 1, base + 1)
    return float(arr[base] + (arr[nxt] - arr[base]) * rest)


def _iqr(nums: List[float]) -> float:
    return _quantile(nums, 0.75) - _quantile(nums, 0.25)


def _weighted_median(pairs: List[Dict[str, float]]) -> float:
    if not pairs:
        return 0.0
    arr = sorted(pairs, key=lambda x: x["value"])
    total = sum(p["weight"] for p in arr)
    if total <= 0:
        return 0.0
    acc = 0.0
    for pair in arr:
        acc += pair["weight"]
        if acc >= total / 2.0:
            return float(pair["value"])
    return float(arr[-1]["value"])


def _pick_market_price(md: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(md, dict):
        return None
    for key in (
        "price",
        "priceTrend",
        "price7d",
        "price30d",
        "price90d",
        "price1d",
        "priceSecondary",
        "pricePrimary",
    ):
        value = _to_number(md.get(key))
        if value is not None and value > 0:
            return _safe_round2(value)
    return None


def _normalize_bucket_key(value: Optional[str]) -> str:
    text = (value or "").strip()
    lowered = text.lower()
    if lowered in ("", "unknown", "don!!"):
        return "DON!!"
    return text


def _norm_to_list(value: Any, normalize: bool = False) -> List[str]:
    raw = value if isinstance(value, list) else ([value] if value else [])
    out = [str(x).strip() for x in raw if str(x).strip()]
    if normalize:
        return [_normalize_bucket_key(x) for x in out]
    return out


def _bucket_add(
    bucket_map: Dict[str, Dict[str, float]],
    key: str,
    value_to_add: float,
    has_price: bool,
) -> None:
    if key not in bucket_map:
        bucket_map[key] = {"cards": 0.0, "priced": 0.0, "total": 0.0}
    bucket_map[key]["cards"] += 1.0
    if has_price:
        bucket_map[key]["priced"] += 1.0
        bucket_map[key]["total"] += value_to_add


def _finalize_buckets(bucket_map: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    entries = list(bucket_map.items())
    total_cards = sum(v["cards"] for _, v in entries)
    total_value = sum(v["total"] for _, v in entries)
    rows: List[Dict[str, Any]] = []
    for key, value in entries:
        priced = int(value["priced"])
        rows.append(
            {
                "key": key,
                "label": key,
                "cards": int(value["cards"]),
                "priced": priced,
                "totalUSD": _safe_round2(value["total"]),
                "avgPrice": _safe_round2(value["total"] / priced) if priced > 0 else 0.0,
                "pctByCount": _safe_round2((value["cards"] / total_cards) * 100.0) if total_cards else 0.0,
                "pctByValue": _safe_round2((value["total"] / total_value) * 100.0) if total_value else 0.0,
            }
        )
    rows.sort(key=lambda x: x["totalUSD"], reverse=True)
    return rows


def _price_weight(usd: float) -> float:
    return 1.0 / (1.0 + exp(-(usd - 2.0) / 3.0))


def _liq_weight(listings: float) -> float:
    return tanh((listings or 0.0) / 180.0)


def _card_weight(usd: Optional[float], listings: Optional[float]) -> float:
    price = max(0.0, usd or 0.0)
    liq = max(0.0, listings or 0.0)
    return max(0.0, min(1.0, _price_weight(price) * _liq_weight(liq)))


def _lift(x: float, gamma: float) -> float:
    x = max(0.0, min(1.0, x))
    return max(0.0, min(1.0, x**gamma))


def _infer_pack_divisor(set_doc: Dict[str, Any]) -> int:
    language = str(set_doc.get("language") or "").lower()
    set_id = str(set_doc.get("id") or "").upper()
    if language == "en":
        return 24
    if language == "jp":
        return 12
    return 12 if set_id.endswith("JP") else 24


def _normalize_set_language(set_doc: Dict[str, Any]) -> str:
    language = str(set_doc.get("language") or "").lower()
    if language in ("en", "jp"):
        return language
    set_id = str(set_doc.get("id") or "").upper()
    return "jp" if set_id.endswith("JP") else "en"


def _get_pull_rate_model(set_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    set_id = str(set_doc.get("id") or "").upper()
    language = _normalize_set_language(set_doc)
    base_set_code = set_id[:-2] if set_id.endswith("JP") else set_id
    return (
        PULL_RATE_SEED.get((set_id, language))
        or PULL_RATE_SEED.get((base_set_code, language))
    )


def _classify_pull_rate_bucket(set_doc: Dict[str, Any], card_doc: Dict[str, Any]) -> Optional[str]:
    rarity = str(card_doc.get("rarityName") or card_doc.get("rarity") or "").strip().lower()
    name = str(card_doc.get("name") or "").strip().lower()
    local_id = str(card_doc.get("localId") or "").strip().lower()
    card_id = str(card_doc.get("id") or "").strip().lower()
    color_values = [v.lower() for v in _norm_to_list(card_doc.get("color"))]
    alternate = card_doc.get("alternate")
    is_parallel = bool(alternate) or "alternate art" in name or "[aa]" in name or "parallel" in name

    is_don = "don" in name or local_id.startswith("don")
    if is_don and "gold" in name:
        return "gold_don"

    if "manga" in name:
        return "red_super_parallel" if str(set_doc.get("id") or "").upper().startswith("OP13") else "sp"

    if str(set_doc.get("id") or "").upper().startswith("OP13"):
        if any(token in name for token in ("3rd anniversary", "3rd anniv", "anniversary")):
            return "anniversary_special"
        if is_parallel and any(token in name for token in ("super parallel", "sp")):
            return "sp"
        if is_parallel and any(token in name for token in ("luffy", "ace", "sabo")) and "red" in color_values:
            return "red_super_parallel"

    if "leader" in rarity:
        return "leader_parallel" if is_parallel else None
    if "secret rare" in rarity or rarity == "sec":
        return "sec_parallel" if is_parallel else "sec_base"
    if "super rare" in rarity or rarity == "sr":
        return "sr_parallel" if is_parallel else "sr_base"

    if is_parallel and any(token in card_id for token in ("god", "demon")):
        return "god_pack"

    return None


def _build_pull_rate_ev(
    set_doc: Dict[str, Any],
    priced_rows: List[Dict[str, Any]],
    booster_box_price: Optional[float],
) -> Optional[Dict[str, Any]]:
    model = _get_pull_rate_model(set_doc)
    if not model:
        return None

    bucket_prices: Dict[str, List[float]] = {}
    classified_count = 0
    unclassified_priced = 0
    for row in priced_rows:
        price = row.get("price")
        if price is None or price <= 0:
            continue
        bucket = _classify_pull_rate_bucket(set_doc, row["doc"])
        if bucket:
            bucket_prices.setdefault(bucket, []).append(price)
            classified_count += 1
        else:
            unclassified_priced += 1

    bucket_values: Dict[str, Dict[str, Any]] = {}
    expected_core = 0.0
    realistic_core = 0.0
    expected_full = 0.0
    realistic_full = 0.0

    for bucket in model.get("buckets", []):
        if not isinstance(bucket, dict):
            continue
        key = bucket.get("key")
        if not isinstance(key, str):
            continue
        prices = sorted(bucket_prices.get(key, []))
        exp_price = _safe_round2(_quantile(prices, 0.4)) if prices else 0.0
        real_price = _safe_round2(_quantile(prices, 0.5)) if prices else 0.0
        avg_per_box = _to_number(((bucket.get("estimatedPerBox") or {}).get("avg")))
        if avg_per_box is None:
            min_per_box = _to_number(((bucket.get("estimatedPerBox") or {}).get("min")))
            max_per_box = _to_number(((bucket.get("estimatedPerBox") or {}).get("max")))
            if min_per_box is not None and max_per_box is not None:
                avg_per_box = (min_per_box + max_per_box) / 2.0
            else:
                avg_per_box = 0.0

        expected_contribution = _safe_round2(avg_per_box * exp_price)
        realistic_contribution = _safe_round2(avg_per_box * real_price)
        include_core = bool(bucket.get("includeInCoreEv"))
        include_full = bool(bucket.get("includeInFullEv"))

        if include_core:
            expected_core += expected_contribution
            realistic_core += realistic_contribution
        if include_full:
            expected_full += expected_contribution
            realistic_full += realistic_contribution

        bucket_values[key] = {
            "cards": len(prices),
            "expectedPrice": exp_price,
            "realisticPrice": real_price,
            "avgPerBox": _safe_round2(avg_per_box),
            "expectedContribution": expected_contribution,
            "realisticContribution": realistic_contribution,
            "includeInCoreEv": include_core,
            "includeInFullEv": include_full,
        }

    total_priced = sum(1 for row in priced_rows if row.get("price") is not None and row.get("price") > 0)
    classified_pct = _safe_round2((classified_count / total_priced) * 100.0) if total_priced else 0.0

    realistic_core = _safe_round2(realistic_core)
    expected_core = _safe_round2(expected_core)
    realistic_full = _safe_round2(realistic_full)
    expected_full = _safe_round2(expected_full)

    roi_core = None
    roi_full = None
    if booster_box_price is not None and booster_box_price > 0:
        roi_core = _safe_round2(((realistic_core - booster_box_price) / booster_box_price) * 100.0)
        roi_full = _safe_round2(((realistic_full - booster_box_price) / booster_box_price) * 100.0)

    return {
        "modelSetId": model.get("setId"),
        "modelLanguage": model.get("language"),
        "status": model.get("status"),
        "expectedValueCore": expected_core,
        "realisticExpectedValueCore": realistic_core,
        "expectedValueFull": expected_full,
        "realisticExpectedValueFull": realistic_full,
        "roiCore": roi_core,
        "roiFull": roi_full,
        "coverage": {
            "pricedCards": total_priced,
            "classifiedCards": classified_count,
            "unclassifiedCards": unclassified_priced,
            "classifiedPct": classified_pct,
        },
        "bucketValues": bucket_values,
    }

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


def _get_closest_at_or_before(
    prices: List[Dict[str, Any]],
    target_dt: datetime,
) -> Optional[Dict[str, Any]]:
    best_doc = None
    best_created_at = None
    for price_doc in prices:
        created_at = _parse_created_at(price_doc)
        if created_at is None or created_at > target_dt:
            continue
        if best_created_at is None or created_at > best_created_at:
            best_created_at = created_at
            best_doc = price_doc
    return best_doc

def _pick_series_now(latest: Optional[Dict[str, Any]]) -> SeriesInfo:
    doc = latest or {}

    # 1) prefer cmPriceTrend
    trend = _to_number(doc.get("cmPriceTrend"))
    if trend is not None:
        return SeriesInfo(price_now_usd=trend, used_primary=True)

    # 2) fallback pricePrimary
    p_primary = _to_number(doc.get("pricePrimary"))
    if p_primary is not None:
        return SeriesInfo(price_now_usd=p_primary, used_primary=True)

    # 3) fallback cascade
    p_fallback = _best_usd(doc)
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
        def getter(d: Optional[Dict[str, Any]]) -> Optional[float]:
            dd = d or {}
            v = _to_number(dd.get("cmPriceTrend"))
            if v is not None:
                return v
            return _to_number(dd.get("pricePrimary"))
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


def _effective_price_from_snapshot(doc: Optional[Dict[str, Any]]) -> Optional[float]:
    if not doc:
        return None
    primary = _to_number(doc.get("pricePrimary"))
    if primary is not None:
        return _safe_round2(primary)
    trend = _to_number(doc.get("cmPriceTrend"))
    if trend is not None:
        return _safe_round2(trend)
    low = _to_number(doc.get("cmPriceLow"))
    if low is not None:
        return _safe_round2(low)
    return None

def compute_market_data_for_item(prices: List[Dict[str, Any]], graded_first: Dict[str, Any]) -> Dict[str, Any]:
    latest = prices[0] if prices else None

    # Current series (USD)
    s = _pick_series_now(latest)

    # Baselines (USD)
    now = datetime.now(timezone.utc)
    b1doc = _get_closest_around(prices, now - timedelta(days=1), max_days=1.75)
    b7doc = _get_closest_around(prices, now - timedelta(days=7), max_days=3.5)
    b30doc = _get_closest_around(prices, now - timedelta(days=30), max_days=7)
    b90doc = _get_closest_around(prices, now - timedelta(days=90), max_days=14)
    b180doc = _get_closest_around(prices, now - timedelta(days=180), max_days=56)
    b365doc = _get_closest_around(prices, now - timedelta(days=365), max_days=112)
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

    liquidity_score = _compute_liquidity_score(
        sellers=sellers,
        listings=listings,
        spread_pct=low_vs_trend_discount_pct,
    )
    momentum_weighted_pct = _compute_weighted_momentum(pct7, pct30, pct90)
    momentum_score = _compute_momentum_score(momentum_weighted_pct)
    grading_attractiveness_score = _compute_grading_attractiveness(
        gp_psa10=gp_psa10,
        gp_bsg10=gp_bsg10,
        liquidity_score=liquidity_score,
    )

    return {
        "updatedAt": datetime.now(timezone.utc),
        # marketData
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

        # derived deterministic signals for discovery / ranking
        "liquidityScore": liquidity_score, # 0..100
        "momentumWeighted": momentum_weighted_pct, # %
        "momentumScore": momentum_score, # 0..100
        "gradingAttractivenessScore": grading_attractiveness_score, # 0..100
    }


def _build_set_market_data(
    set_doc: Dict[str, Any],
    items: List[Dict[str, Any]],
    sealed_price: Optional[float],
    price_history_by_item: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    cards = [item for item in items if item.get("type") == "Cards"]
    non_cards = [item for item in items if item.get("type") != "Cards"]

    priced_rows = []
    for item in cards:
        market_data = item.get("marketData") or {}
        priced_rows.append(
            {
                "doc": item,
                "price": _pick_market_price(market_data),
                "listings": _to_number(market_data.get("listings")),
                "pct1": _to_number(market_data.get("percentageChange1d")),
                "pct7": _to_number(market_data.get("percentageChange7d")),
                "pct30": _to_number(market_data.get("percentageChange30d")),
                "spread": _to_number(market_data.get("spread")),
            }
        )

    cards_prices = [row["price"] for row in priced_rows if row["price"] is not None and row["price"] > 0]
    non_cards_prices = [
        price
        for price in (_pick_market_price(item.get("marketData")) for item in non_cards)
        if price is not None and price > 0
    ]
    psa10_prices = [
        value
        for value in (
            _to_number((row["doc"].get("marketData") or {}).get("psa10"))
            for row in priced_rows
        )
        if value is not None and value > 0
    ]
    bgs10_prices = [
        value
        for value in (
            _to_number((row["doc"].get("marketData") or {}).get("bgs10"))
            for row in priced_rows
        )
        if value is not None and value > 0
    ]

    cards_count = len(cards)
    non_cards_count = len(non_cards)
    total_count = cards_count + non_cards_count
    total_cards_price = _safe_round2(sum(cards_prices)) if cards_prices else 0.0
    total_other_types_price = _safe_round2(sum(non_cards_prices)) if non_cards_prices else 0.0
    total_price = _safe_round2(total_cards_price + total_other_types_price)

    booster_box_price = _safe_round2(sealed_price) if sealed_price is not None and sealed_price > 0 else None
    booster_pack_price = None
    if booster_box_price is not None:
        booster_pack_price = _safe_round2(booster_box_price / _infer_pack_divisor(set_doc))

    profitable_cards = 0
    if booster_pack_price is not None:
        profitable_cards = sum(1 for price in cards_prices if price >= booster_pack_price)

    hits = {
        "hits5": sum(1 for price in cards_prices if price >= 5.0),
        "hits10": sum(1 for price in cards_prices if price >= 10.0),
        "hits50": sum(1 for price in cards_prices if price >= 50.0),
        "hits100": sum(1 for price in cards_prices if price >= 100.0),
        "hits200": sum(1 for price in cards_prices if price >= 200.0),
        "hits500": sum(1 for price in cards_prices if price >= 500.0),
    }

    color_map: Dict[str, Dict[str, float]] = {}
    rarity_map: Dict[str, Dict[str, float]] = {}
    attribute_map: Dict[str, Dict[str, float]] = {}
    for row in priced_rows:
        doc = row["doc"]
        price = row["price"] if row["price"] is not None and row["price"] > 0 else 0.0
        has_price = price > 0

        colors = _norm_to_list(doc.get("color"), normalize=True) or ["DON!!"]
        rarities = _norm_to_list(doc.get("rarityName") or doc.get("rarity"), normalize=True) or ["DON!!"]
        attributes = _norm_to_list(doc.get("attribute") or doc.get("attributes"), normalize=True) or ["DON!!"]

        for values, bucket_map in (
            (colors, color_map),
            (rarities, rarity_map),
            (attributes, attribute_map),
        ):
            share = (price / len(values)) if has_price and values else 0.0
            for value in values:
                _bucket_add(bucket_map, value, share, has_price)

    def rarity_text(doc: Dict[str, Any]) -> str:
        return str(doc.get("rarityName") or doc.get("rarity") or "")

    def price_of(row: Dict[str, Any]) -> Optional[float]:
        price = row["price"]
        return price if price is not None and price > 0 else None

    prices_sr: List[float] = []
    prices_sec: List[float] = []
    prices_alt: List[float] = []
    prices_r: List[float] = []
    for row in priced_rows:
        price = price_of(row)
        if price is None:
            continue
        rarity = rarity_text(row["doc"]).upper()
        if "SR" in rarity:
            prices_sr.append(price)
        if "SEC" in rarity:
            prices_sec.append(price)
        if price >= 10.0 and "SEC" not in rarity and price < 300.0:
            prices_alt.append(price)
        if rarity == "R" or rarity.startswith("R "):
            prices_r.append(price)

    prices_manga = [price for price in cards_prices if price >= 300.0]
    rare_median = min(_safe_round2(_quantile(prices_r, 0.5)) if prices_r else 0.0, 0.4)

    p_sr_real = _safe_round2(_quantile(prices_sr, 0.5)) if prices_sr else 0.0
    p_sec_real = _safe_round2(_quantile(prices_sec, 0.5)) if prices_sec else 0.0
    p_alt_real = _safe_round2(_quantile(prices_alt, 0.5)) if prices_alt else 0.0
    p_mng_real = _safe_round2(_quantile(prices_manga, 0.5)) if prices_manga else 0.0

    p_sr_exp = _safe_round2(_quantile(prices_sr, 0.4)) if prices_sr else 0.0
    p_sec_exp = _safe_round2(_quantile(prices_sec, 0.4)) if prices_sec else 0.0
    p_alt_exp = _safe_round2(_quantile(prices_alt, 0.4)) if prices_alt else 0.0
    p_mng_exp = _safe_round2(_quantile(prices_manga, 0.4)) if prices_manga else 0.0

    realistic_expected_value = _safe_round2(
        (6.5 * p_sr_real)
        + (1.8 * p_alt_real)
        + (0.55 * p_sec_real)
        + (0.0413 * p_mng_real)
        + (24.0 * rare_median)
    )
    expected_value = _safe_round2(
        (5.5 * p_sr_exp)
        + (1.4 * p_alt_exp)
        + (0.25 * p_sec_exp)
        + (0.0138 * p_mng_exp)
        + (24.0 * rare_median * 0.6)
    )

    roi = None
    if booster_box_price is not None and booster_box_price > 0:
        roi = _safe_round2(((realistic_expected_value - booster_box_price) / booster_box_price) * 100.0)

    pull_rate_ev = _build_pull_rate_ev(
        set_doc=set_doc,
        priced_rows=priced_rows,
        booster_box_price=booster_box_price,
    )

    cap_ultra_cheap = 0.12
    cap_cheap = 0.24
    cap_normal = 0.38

    mix_abs_pairs: List[Dict[str, float]] = []
    mix_abs_values: List[float] = []
    for row in priced_rows:
        price = row["price"] or 0.0
        listings = row["listings"] or 0.0
        weight = _card_weight(price, listings)
        cap = cap_ultra_cheap if price < 1.0 else cap_cheap if price < 5.0 else cap_normal

        parts = []
        if row["pct1"] is not None:
            parts.append(0.25 * min(abs(row["pct1"]) / 100.0, cap))
        if row["pct7"] is not None:
            parts.append(0.50 * min(abs(row["pct7"]) / 100.0, cap))
        if row["pct30"] is not None:
            parts.append(0.35 * min(abs(row["pct30"]) / 100.0, cap))
        if not parts:
            continue

        mix_abs_pct = _median(parts) * 100.0
        mix_abs_pairs.append({"value": mix_abs_pct, "weight": weight})
        mix_abs_values.append(mix_abs_pct)

    mix_wmed = _weighted_median(mix_abs_pairs)
    f_raw = max(0.0, min(1.0, mix_wmed / 18.0))
    f_value = _lift(max(f_raw, 0.05 if mix_abs_pairs else 0.0), 0.88)

    tau = max(8.0, _median(mix_abs_values) + _iqr(mix_abs_values))
    w_mov = 0.0
    w_all = 0.0
    for pair in mix_abs_pairs:
        w_all += pair["weight"]
        if pair["value"] >= tau:
            w_mov += pair["weight"]
    movers_share = (w_mov + 0.75) / (w_all + 3.75) if (w_all + 3.75) > 0 else 0.0
    b_raw = max(0.0, min(1.0, (movers_share - 0.16) / 0.34))
    b_value = _lift(b_raw, 0.88)

    listings_vals = [row["listings"] for row in priced_rows if row["listings"] is not None and row["listings"] >= 0]
    lq50 = _quantile(listings_vals, 0.5) if listings_vals else 0.0
    l_norm = log1p(lq50) / log1p(450.0) if lq50 >= 0 else 0.0
    l_raw = max(0.0, min(1.0, 1.0 - l_norm))
    l_value = _lift(l_raw, 0.92)

    v_vals = [(row["price"] or 0.0) * (max(1.0, row["listings"] or 0.0) ** 0.3) for row in priced_rows]
    v_sum = sum(v_vals)
    hhi = sum((v / v_sum) ** 2 for v in v_vals) if v_sum > 0 else 0.0
    c_raw = max(0.0, min(1.0, (hhi - 0.14) / 0.32))
    c_value = _lift(c_raw, 1.0)

    signed_pairs = [
        {"value": row["pct7"], "weight": _card_weight(row["price"], row["listings"])}
        for row in priced_rows
        if row["pct7"] is not None
    ]
    med_signed_7d = _weighted_median(signed_pairs)
    if med_signed_7d <= -10.0:
        d_value = 0.08
    elif med_signed_7d <= -6.0:
        d_value = 0.04
    elif med_signed_7d >= 10.0:
        d_value = -0.07
    elif med_signed_7d >= 6.0:
        d_value = -0.03
    else:
        d_value = 0.0

    spread_pairs = [
        {"value": min(row["spread"], 30.0), "weight": _card_weight(row["price"], row["listings"])}
        for row in priced_rows
        if row["spread"] is not None
    ]
    spread_wmed = _weighted_median(spread_pairs)
    x_core = max(0.0, min(1.0, spread_wmed / 16.0))
    x_value = max(0.0, min(1.0, x_core * (0.6 + 0.4 * c_raw)))

    top_rows = sorted(
        [row for row in priced_rows if row["price"] is not None],
        key=lambda x: x["price"],
        reverse=True,
    )
    top_count = max(3, int(len(top_rows) * 0.1)) if top_rows else 0
    top_pairs = []
    for row in top_rows[:top_count]:
        if row["pct30"] is None:
            continue
        cap = cap_cheap if (row["price"] or 0.0) < 5.0 else cap_normal
        top_pairs.append(
            {
                "value": min(abs(row["pct30"]) / 100.0, cap) * 100.0,
                "weight": _card_weight(row["price"], row["listings"]),
            }
        )
    h_value = max(0.0, min(1.0, _weighted_median(top_pairs) / 15.0))

    rho_value = 0.0
    corr_rows = [row for row in priced_rows if row["price"] is not None and row["listings"] is not None]
    if len(corr_rows) >= 4:
        px = [row["price"] for row in corr_rows]
        lx = [log1p(max(0.0, row["listings"] or 0.0)) for row in corr_rows]
        mean_x = sum(px) / len(px)
        mean_y = sum(lx) / len(lx)
        num = 0.0
        dx = 0.0
        dy = 0.0
        for price, listings in zip(px, lx):
            vx = price - mean_x
            vy = listings - mean_y
            num += vx * vy
            dx += vx * vx
            dy += vy * vy
        rho = (num / sqrt(dx * dy)) if dx > 0 and dy > 0 else 0.0
        rho_value = max(0.0, min(1.0, (0.2 - rho) / 0.8))

    j_value = sqrt(l_value * c_value)
    score01 = (
        (0.44 * f_value)
        + (0.18 * b_value)
        + (0.15 * l_value)
        + (0.10 * c_value)
        + d_value
        + (0.12 * x_value)
        + (0.08 * h_value)
        + (0.05 * rho_value)
        + (0.05 * j_value)
    )
    score01 = max(0.0, min(1.0, score01 * 1.14))
    volatility_score = round(score01 * 100.0) / 10.0

    day_labels: List[str] = []
    end_day = datetime.now(timezone.utc)
    start_day = end_day - timedelta(days=SET_TREND_DAYS)
    cursor_day = datetime(
        start_day.year,
        start_day.month,
        start_day.day,
        tzinfo=timezone.utc,
    )
    final_day = datetime(
        end_day.year,
        end_day.month,
        end_day.day,
        tzinfo=timezone.utc,
    )
    while cursor_day <= final_day:
        day_labels.append(cursor_day.date().isoformat())
        cursor_day += timedelta(days=1)

    cards_only = [item for item in items if item.get("type") == "Cards"]
    trend_points: List[Dict[str, Any]] = []
    sales_volume: List[Dict[str, Any]] = []
    for iso_date in day_labels:
        day_end = datetime.fromisoformat(f"{iso_date}T23:59:59.999999+00:00")
        prev_day_end = day_end - timedelta(days=1)

        total = 0.0
        priced_count = 0
        units = 0
        usd = 0.0
        price_sum_for_avg = 0.0
        price_count_for_avg = 0
        cards_with_listings = 0

        for card in cards_only:
            card_id = card.get("id")
            if not isinstance(card_id, str):
                continue
            history = price_history_by_item.get(card_id, [])

            trend_snap = _get_closest_at_or_before(history, day_end)
            trend_price = _effective_price_from_snapshot(trend_snap)
            if trend_price is not None and trend_price > 0:
                total += trend_price
                priced_count += 1

            snap_today = trend_snap
            snap_prev = _get_closest_at_or_before(history, prev_day_end)

            listings_today = _to_number((snap_today or {}).get("listings"))
            listings_prev = _to_number((snap_prev or {}).get("listings"))
            price_today = _effective_price_from_snapshot(snap_today)
            price_prev = _effective_price_from_snapshot(snap_prev)

            if listings_today is not None or listings_prev is not None:
                cards_with_listings += 1

            price_ref = None
            if price_today is not None and price_prev is not None:
                price_ref = (price_today + price_prev) / 2.0
            elif price_today is not None:
                price_ref = price_today
            elif price_prev is not None:
                price_ref = price_prev

            if listings_today is not None and listings_prev is not None:
                sold_units = max(0.0, listings_prev - listings_today)
                if sold_units > 0:
                    units += int(sold_units)
                    if price_ref is not None and price_ref > 0:
                        usd += sold_units * price_ref

            if price_ref is not None and price_ref > 0:
                price_sum_for_avg += price_ref
                price_count_for_avg += 1

        trend_points.append(
            {
                "date": iso_date,
                "total": _safe_round2(total),
                "count": priced_count,
            }
        )
        avg_price = (
            _safe_round2(price_sum_for_avg / price_count_for_avg)
            if price_count_for_avg > 0
            else 0.0
        )
        sales_volume.append(
            {
                "date": iso_date,
                "units": units * 3,
                "usd": _safe_round2(usd * 3.0),
                "avgPrice": avg_price,
                "cardsWithListings": cards_with_listings,
            }
        )

    edges = [0.0, 5.0, 15.0, 50.0, 100.0, 250.0, float("inf")]
    bins = [0 for _ in range(len(edges) - 1)]
    for price in cards_prices:
        for idx in range(len(edges) - 1):
            low = edges[idx]
            high = edges[idx + 1]
            if price >= low and price < high:
                bins[idx] += 1
                break

    price_distribution = []
    for idx, count in enumerate(bins):
        low = edges[idx]
        high = edges[idx + 1]
        price_distribution.append(
            {
                "min": low,
                "max": None if high == float("inf") else high,
                "count": count,
                "label": (
                    f">= ${low:.2f}"
                    if high == float("inf")
                    else f"${low:.2f} - ${high:.2f}"
                ),
            }
        )

    top_movers = sorted(
        [
            {
                "id": row["doc"].get("id"),
                "localId": row["doc"].get("localId"),
                "name": row["doc"].get("name"),
                "price": row["price"],
                "score": _to_number(row["doc"].get("score")),
                "priceChange7d": row["doc"].get("marketData", {}).get("priceChange7d"),
                "percentageChange7d": row["doc"].get("marketData", {}).get("percentageChange7d"),
            }
            for row in priced_rows
            if (row["price"] or 0) >= SET_TOP_BASE_PRICE and row["pct7"] is not None
        ],
        key=lambda x: x["percentageChange7d"] or float("-inf"),
        reverse=True,
    )[:6]

    top_score = sorted(
        [
            {
                "id": row["doc"].get("id"),
                "localId": row["doc"].get("localId"),
                "name": row["doc"].get("name"),
                "price": row["price"],
                "score": _to_number(row["doc"].get("score")),
                "priceChange7d": row["doc"].get("marketData", {}).get("priceChange7d"),
                "percentageChange7d": row["doc"].get("marketData", {}).get("percentageChange7d"),
            }
            for row in priced_rows
            if (row["price"] or 0) >= SET_TOP_BASE_PRICE
            and isinstance(_to_number(row["doc"].get("score")), float)
        ],
        key=lambda x: x["score"] or float("-inf"),
        reverse=True,
    )[:6]

    return {
        "updatedAt": datetime.now(timezone.utc),
        "cardsCount": cards_count,
        "otherTypesCount": non_cards_count,
        "totalCount": total_count,
        "totalCardsPrice": total_cards_price,
        "totalOtherTypesPrice": total_other_types_price,
        "totalPrice": total_price,
        "psa10CardsCount": len(psa10_prices),
        "psa10TotalPrice": _safe_round2(sum(psa10_prices)) if psa10_prices else 0.0,
        "bgs10CardsCount": len(bgs10_prices),
        "bgs10TotalPrice": _safe_round2(sum(bgs10_prices)) if bgs10_prices else 0.0,
        **hits,
        "boosterBoxPrice": booster_box_price,
        "boosterPackPrice": booster_pack_price,
        "profitableCards": profitable_cards,
        "expectedValue": expected_value,
        "realisticExpectedValue": realistic_expected_value,
        "roi": roi,
        "*expectedValuePullRateCore": pull_rate_ev.get("expectedValueCore") if pull_rate_ev else None,
        "*realisticExpectedValuePullRateCore": pull_rate_ev.get("realisticExpectedValueCore") if pull_rate_ev else None,
        "*expectedValuePullRateFull": pull_rate_ev.get("expectedValueFull") if pull_rate_ev else None,
        "*realisticExpectedValuePullRateFull": pull_rate_ev.get("realisticExpectedValueFull") if pull_rate_ev else None,
        "*roiPullRateCore": pull_rate_ev.get("roiCore") if pull_rate_ev else None,
        "*roiPullRateFull": pull_rate_ev.get("roiFull") if pull_rate_ev else None,
        "*pullRateModel": pull_rate_ev,
        "volatility": volatility_score,
        "marketBreakdown": {
            "byColor": _finalize_buckets(color_map),
            "byRarity": _finalize_buckets(rarity_map),
            "byAttribute": _finalize_buckets(attribute_map),
        },
        "cards": {
            "charts": {
                "trend": trend_points,
                "priceDistribution": price_distribution,
                "salesVolume": sales_volume,
            },
            "top": {
                "movers": top_movers,
                "score": top_score,
            },
        },
    }


def update_sets_market_data(db: Database, set_ids: List[str]) -> Tuple[int, int]:
    if not set_ids:
        return (0, 0)

    coll_sets = db["Sets"]
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]

    sets = list(
        coll_sets.find(
            {"id": {"$in": set_ids}},
            {"id": 1, "language": 1, "sealedId": 1},
        )
    )
    if not sets:
        return (0, 0)

    set_id_list = [s["id"] for s in sets if isinstance(s.get("id"), str)]
    items_by_set: Dict[str, List[Dict[str, Any]]] = {set_id: [] for set_id in set_id_list}

    for item in coll_cards.find(
        {"setId": {"$in": set_id_list}},
        {
            "id": 1,
            "name": 1,
            "localId": 1,
            "setId": 1,
            "type": 1,
            "score": 1,
            "color": 1,
            "rarity": 1,
            "rarityName": 1,
            "attribute": 1,
            "attributes": 1,
            "marketData": 1,
        },
    ):
        set_id = item.get("setId")
        if isinstance(set_id, str):
            items_by_set.setdefault(set_id, []).append(item)

    sealed_ids = [
        s.get("sealedId")
        for s in sets
        if isinstance(s.get("sealedId"), str) and s.get("sealedId")
    ]
    sealed_latest: Dict[str, Dict[str, Any]] = {}
    if sealed_ids:
        for doc in coll_prices.find(
            {"itemId": {"$in": sealed_ids}},
            {
                "itemId": 1,
                "createdAt": 1,
                "pricePrimary": 1,
                "cmPriceTrend": 1,
                "cmAvg30d": 1,
                "cmAvg7d": 1,
                "cmAvg1d": 1,
                "cmPriceAvg": 1,
                "cmPriceLow": 1,
                "priceUngraded": 1,
                "pricePriceCharting": 1,
            },
        ).sort("createdAt", -1):
            item_id = doc.get("itemId")
            if isinstance(item_id, str) and item_id not in sealed_latest:
                sealed_latest[item_id] = doc

    card_ids = [
        item.get("id")
        for items in items_by_set.values()
        for item in items
        if item.get("type") == "Cards" and isinstance(item.get("id"), str)
    ]
    price_history_by_item: Dict[str, List[Dict[str, Any]]] = {}
    if card_ids:
        since = datetime.now(timezone.utc) - timedelta(days=SET_PRICE_LOOKBACK_DAYS)
        for doc in coll_prices.find(
            {"itemId": {"$in": card_ids}, "createdAt": {"$gte": since}},
            {
                "itemId": 1,
                "createdAt": 1,
                "pricePrimary": 1,
                "cmPriceTrend": 1,
                "cmPriceLow": 1,
                "listings": 1,
            },
        ):
            item_id = doc.get("itemId")
            if isinstance(item_id, str):
                price_history_by_item.setdefault(item_id, []).append(doc)
        for snaps in price_history_by_item.values():
            snaps.sort(
                key=lambda doc: doc.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

    ops: List[UpdateOne] = []
    touched = 0
    updated = 0
    for set_doc in sets:
        set_id = set_doc.get("id")
        if not isinstance(set_id, str):
            continue
        sealed_doc = sealed_latest.get(set_doc.get("sealedId"))
        sealed_price = _pick_series_now(sealed_doc).price_now_usd if sealed_doc else None
        market_data = _build_set_market_data(
            set_doc=set_doc,
            items=items_by_set.get(set_id, []),
            sealed_price=sealed_price,
            price_history_by_item=price_history_by_item,
        )
        touched += 1
        ops.append(
            UpdateOne(
                {"id": set_id},
                {"$set": {"marketData": market_data}},
            )
        )

    if ops:
        res = coll_sets.bulk_write(ops, ordered=False)
        updated = (res.modified_count or 0) + (res.upserted_count or 0)

    return (touched, updated)


def update_cards_market_data(
    db: Database,
    days_back: int = 400,
    limit_ids: Optional[List[str]] = None,
) -> Tuple[int, int, int, int]:
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]

    q_cards: Dict[str, Any] = {}
    q_cards["setId"] = "OP13"
    if limit_ids:
        q_cards["id"] = {"$in": limit_ids}

    base_cards = list(coll_cards.find(q_cards, {"id": 1, "setId": 1}))
    ids = [c["id"] for c in base_cards if "id" in c]
    if not ids:
        return (0, 0, 0, 0)

    affected_set_ids = sorted(
        set_id
        for set_id in coll_cards.distinct("setId", q_cards)
        if isinstance(set_id, str) and set_id.strip()
    )

    since = datetime.now(timezone.utc) - timedelta(days=days_back)

    cur = coll_prices.find(
        {"itemId": {"$in": ids}, "createdAt": {"$gte": since}},
        {
            "itemId": 1,
            "createdAt": 1,
            "pricePrimary": 1,
            "cmPriceTrend": 1,
            "cmAvg30d": 1,
            "cmAvg7d": 1,
            "cmAvg1d": 1,
            "cmPriceAvg": 1,
            "cmPriceLow": 1,
            "priceUngraded": 1,
            "pricePriceCharting": 1,
            "listings": 1,
            "sellers": 1,
            "psa10": 1,
            "bsg10": 1,
        },
    )

    per_item: Dict[str, List[Dict[str, Any]]] = {}
    for p in cur:
        per_item.setdefault(p["itemId"], []).append(p)

    # order by createdAt DESC
    for snaps in per_item.values():
        snaps.sort(
            key=lambda doc: doc.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

    # graded first (psa10/bsg10) - $first on createdAt desc
    graded_first: Dict[str, Dict[str, Any]] = {}

    for cid, snaps in per_item.items():
        best_doc = None
        best_ts = None

        for d in snaps:
            # check graded prices
            has_psa = isinstance(d.get("psa10"), (int, float))
            has_bsg = isinstance(d.get("bsg10"), (int, float))

            if not (has_psa or has_bsg):
                continue

            cad = d.get("createdAt")
            if not isinstance(cad, datetime):
                continue

            if best_ts is None or cad > best_ts:
                best_ts = cad
                best_doc = d

        if best_doc:
            graded_first[cid] = {
                "psa10": best_doc.get("psa10"),
                "bsg10": best_doc.get("bsg10"),
            }

    ops: List[UpdateOne] = []
    touched = 0
    updated = 0
    for cid in ids:
        prices = per_item.get(cid, [])
        md = compute_market_data_for_item(prices, graded_first.get(cid, {}))
        touched += 1
        if any(v is not None for v in md.values()):
            ops.append(UpdateOne({"id": cid}, {"$set": {"marketData": md}}))
    if ops:
        res = coll_cards.bulk_write(ops, ordered=False)
        updated = (res.modified_count or 0) + (res.upserted_count or 0)
    sets_touched, sets_updated = update_sets_market_data(db, affected_set_ids)
    return (touched, updated, sets_touched, sets_updated)
