"""
Logic:
- Computes daily global "units" and "volume" based on listings deltas,
  using pricePrimary (USD) as primary, falling back to cmPriceTrend (USD).
- Day boundaries use Europe/Rome timezone; writes ONLY the just-finished day.
- Considers ONLY Prices whose itemId exists in Cards with type="Cards".
- Upserts into the "SalesVolume" collection by `date` (YYYY-MM-DD).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from statistics import mean, median
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from src.core.emailer import send_email
import traceback

# Carica variabili da ambiente (.env o .env.local)
load_dotenv(".env.local")
load_dotenv()

# ========================= CONFIGURABLE CONSTANTS =========================
SALES_SCALING_FACTOR: float = float(5)
LOOKBACK_DAYS: int = int("7")
EUROPE_ROME = ZoneInfo("Europe/Rome")

# =============================== HELPERS =================================
def _to_utc(dt_local: datetime) -> datetime:
    if dt_local.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt_local.astimezone(timezone.utc)


def _day_bounds_rome(target: datetime) -> Tuple[datetime, datetime]:
    target_rome = target.astimezone(EUROPE_ROME)
    day_start_rome = datetime(target_rome.year, target_rome.month, target_rome.day, 0, 0, 0, 0, tzinfo=EUROPE_ROME)
    day_end_rome = datetime(target_rome.year, target_rome.month, target_rome.day, 23, 59, 59, 999000, tzinfo=EUROPE_ROME)
    return _to_utc(day_start_rome), _to_utc(day_end_rome)


def _get_closest_at_or_before(snaps: List[Dict[str, Any]], cutoff_utc: datetime) -> Optional[Dict[str, Any]]:
    for s in snaps:
        ts = s.get("createdAt")
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if isinstance(ts, datetime) and ts <= cutoff_utc:
            return s
    return None


def _num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x))
    except Exception:
        return None


def _effective_usd(snap: Optional[Dict[str, Any]]) -> Optional[float]:
    if not snap:
        return None
    pp = _num(snap.get("pricePrimary"))
    if isinstance(pp, (int, float)):
        return float(pp)
    cm = _num(snap.get("cmPriceTrend"))
    if isinstance(cm, (int, float)):
        return float(cm)
    return None


def _get_listings(snap: Optional[Dict[str, Any]]) -> Optional[int]:
    if not snap:
        return None
    v = snap.get("listings")
    if isinstance(v, (int, float)):
        return int(v)
    v = snap.get("totalListings")
    if isinstance(v, (int, float)):
        return int(v)
    return None


def _week_bounds_rome(target: datetime) -> Tuple[datetime, datetime]:
    target_rome = target.astimezone(EUROPE_ROME)
    monday_rome = target_rome - timedelta(days=target_rome.weekday())
    week_start_rome = datetime(monday_rome.year, monday_rome.month, monday_rome.day, 0, 0, 0, 0, tzinfo=EUROPE_ROME)
    sunday_rome = week_start_rome + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999000)
    return _to_utc(week_start_rome), _to_utc(sunday_rome)


# =============================== CORE ====================================
def compute_daily_sales_volume(db, day_rome: datetime) -> Dict[str, Any]:
    # 1) Day bounds (UTC) for day and previous day
    day_start_utc, day_end_utc = _day_bounds_rome(day_rome)
    prev_day = (day_start_utc - timedelta(seconds=1)).astimezone(EUROPE_ROME)
    prev_start_utc, prev_end_utc = _day_bounds_rome(prev_day)

    # 2) Earliest timestamp to cover missing previous-day snapshots
    earliest_needed_utc = prev_start_utc - timedelta(days=LOOKBACK_DAYS - 1)

    coll_cards: Collection = db["Cards"]
    coll_prices: Collection = db["Prices"]

    # 3) Eligible itemIds: Cards only
    item_ids: List[str] = [doc["id"] for doc in coll_cards.find({"type": "Cards"}, {"id": 1}) if isinstance(doc.get("id"), str)]
    if not item_ids:
        return {"date": day_rome.astimezone(EUROPE_ROME).date().isoformat(), "units": 0, "volume": 0.0, "listings": 0}

    # 4) Fetch snapshots for time window [earliest .. day_end]
    cursor = coll_prices.find(
        {
            "itemId": {"$in": item_ids},
            "createdAt": {
                "$gte": earliest_needed_utc,
                "$lte": day_end_utc,
            },
        },
        {
            "_id": 0,
            "itemId": 1,
            "createdAt": 1,
            "pricePrimary": 1,
            "cmPriceTrend": 1,
            "listings": 1,
            "totalListings": 1,
        },
    )

    # 5) Group per itemId
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for s in cursor:
        iid = s.get("itemId")
        if isinstance(iid, str):
            groups.setdefault(iid, []).append(s)

    # 5.1) Order by createdAt DESC
    for snaps in groups.values():
        snaps.sort(
            key=lambda doc: doc.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

    # 6) Accumulate
    total_units = 0
    total_usd = 0.0
    cards_with_listings = 0
    total_price_sum_usd = 0.0
    cards_with_price = 0

    for iid, snaps in groups.items():
        snap_today = _get_closest_at_or_before(snaps, day_end_utc)
        snap_prev = _get_closest_at_or_before(snaps, prev_end_utc)

        # Sum of prices for the day (one price per card, closest snapshot at or before day_end)
        p_day = _effective_usd(snap_today)
        if isinstance(p_day, (int, float)) and float(p_day) > 0:
            total_price_sum_usd += float(p_day)
            cards_with_price += 1

        l_today = _get_listings(snap_today)
        l_prev = _get_listings(snap_prev)

        if isinstance(l_today, int) or isinstance(l_prev, int):
            cards_with_listings += 1

        if isinstance(l_today, int) and isinstance(l_prev, int):
            sold_units = max(0, l_prev - l_today)
            if sold_units > 0:
                p_today = _effective_usd(snap_today)
                p_prev = _effective_usd(snap_prev)

                if isinstance(p_today, (int, float)) and isinstance(p_prev, (int, float)):
                    price_ref = (float(p_today) + float(p_prev)) / 2.0
                elif isinstance(p_today, (int, float)):
                    price_ref = float(p_today)
                elif isinstance(p_prev, (int, float)):
                    price_ref = float(p_prev)
                else:
                    price_ref = None

                if isinstance(price_ref, float) and price_ref > 0:
                    total_units += sold_units
                    total_usd += sold_units * price_ref

    # 7) Scale and return
    units_scaled = int(round(total_units * SALES_SCALING_FACTOR))
    volume_scaled = round(total_usd * SALES_SCALING_FACTOR, 2)
    listings_scaled = int(round(cards_with_listings * SALES_SCALING_FACTOR))
    date_dt = day_end_utc

    prices_sum = round(total_price_sum_usd, 2)

    return {
        "date": date_dt,
        "units": units_scaled,
        "volume": volume_scaled,
        "listings": listings_scaled,
        "totalItemValue": prices_sum,
        "totalItem": cards_with_price,
        "createdAt": datetime.now(timezone.utc),
    }

def upsert_sales_volume(db, day_rome: datetime, data: Dict[str, Any]) -> None:
    coll_sv: Collection = db["SalesVolume"]

    # Check for existing and delete
    coll_sv.delete_many({"date": data["date"]})
    # Insert new
    coll_sv.insert_one(data)


def compute_weekly_sales_volume(db, reference_day_rome: datetime) -> Dict[str, Any]:
    start_utc, end_utc = _week_bounds_rome(reference_day_rome)
    week_number = start_utc.astimezone(EUROPE_ROME).isocalendar().week
    coll_sv: Collection = db["SalesVolume"]

    docs: List[Dict[str, Any]] = list(
        coll_sv.find(
            {"date": {"$gte": start_utc, "$lte": end_utc}},
            {"_id": 0, "date": 1, "volume": 1, "units": 1},
        )
    )

    if not docs:
        return {
            "weekStart": start_utc,
            "weekEnd": end_utc,
            "weekNumber": week_number,
            "openVolume": 0,
            "highVolume": 0,
            "lowVolume": 0,
            "closeVolume": 0,
            "avgVolume": 0,
            "medianVolume": 0,
            "totalVolume": 0,
            "openUnits": 0,
            "highUnits": 0,
            "lowUnits": 0,
            "closeUnits": 0,
            "avgUnits": 0,
            "medianUnits": 0,
            "totalUnits": 0,
            "days": 0,
            "createdAt": datetime.now(timezone.utc),
        }

    # order by Date ASC per open/close
    docs.sort(key=lambda d: d.get("date", datetime.min.replace(tzinfo=timezone.utc)))

    vols = [float(d.get("volume", 0)) for d in docs]
    units = [int(d.get("units", 0)) for d in docs]

    return {
        "weekStart": start_utc,
        "weekEnd": end_utc,
        "weekNumber": week_number,
        "openVolume": vols[0],
        "highVolume": max(vols),
        "lowVolume": min(vols),
        "closeVolume": vols[-1],
        "avgVolume": round(mean(vols), 2),
        "medianVolume": round(median(vols), 2),
        "totalVolume": round(sum(vols), 2),
        "openUnits": units[0],
        "highUnits": max(units),
        "lowUnits": min(units),
        "closeUnits": units[-1],
        "avgUnits": round(mean(units), 2),
        "medianUnits": int(median(units)),
        "totalUnits": sum(units),
        "days": len(docs),
        "createdAt": datetime.now(timezone.utc),
    }

def upsert_sales_volume_weekly(db, data: Dict[str, Any]) -> None:
    coll_w: Collection = db["SalesVolumeWeekly"]
    coll_w.delete_many({"weekStart": data["weekStart"]})
    coll_w.insert_one(data)

# =============================== MAIN =================================
def main() -> int:
    try:
        MONGO_URI = os.environ["MONGODB_URI"]
        MONGODB_DB = os.environ["MONGODB_DB"]

        client = MongoClient(MONGO_URI)
        db = client[MONGODB_DB]

        # just-finished day in Europe/Rome
        now_rome = datetime.now(EUROPE_ROME)
        yesterday_rome = (now_rome - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)

        data = compute_daily_sales_volume(db, yesterday_rome)
        upsert_sales_volume(db, yesterday_rome, data)

        summary = f"[SalesVolume] Upserted {data}"
        print(f"{summary}")

        # Se oggi è lunedì, crea il riepilogo settimanale (lun-dom) della settimana appena conclusa.
        if now_rome.weekday() == 0:
            weekly_data = compute_weekly_sales_volume(db, yesterday_rome)
            upsert_sales_volume_weekly(db, weekly_data)
            summary += f"\n[SalesVolumeWeekly] Upserted {weekly_data}"

        send_email("[WORKFLOW] Sales Volume Report", summary)
        return 0

    except Exception:
        send_email("[WORKFLOW][ERROR] Sales Volume Report", traceback.format_exc())
        raise


if __name__ == "__main__":

    raise SystemExit(main())
