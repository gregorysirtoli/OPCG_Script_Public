"""
Valutazione del track record (vedi docs/trackrecord.md). Ogni giorno:

1. Chiamate (TrackRecordCalls) vecchie di 4 o 8 settimane -> TrackRecordOutcomes: rendimento
   lordo, netto della soglia di costo, scarto dal mercato della stessa fascia di prezzo nello
   stesso periodo (TrackRecordBenchmarks), calo massimo nel periodo, successo secondo lo stamp.
2. Previsioni settimanali (TrackRecordForecasts) vecchie di 28 giorni ->
   TrackRecordForecastOutcomes: dove cade il prezzo reale rispetto a Q20/Q80 ed errore di Q50
   contro la previsione "il prezzo resta uguale".
3. TrackRecordSummary: tabelle per stamp / fascia / liquidita' e numeri chiave.
   Una percentuale esce solo con almeno MIN_SAMPLE chiamate valutate.
4. TrackRecordDigests: impronta SHA-256 di chiamate e previsioni di ogni giorno chiuso,
   concatenata con quella del giorno prima. Con TRACKRECORD_DIGEST_FILE la lista viene
   scritta anche in un CSV da pubblicare.

Esiti, benchmark e impronte si scrivono solo in aggiunta: un esito gia' calcolato non viene
ricalcolato. Il riepilogo invece e' derivato e viene riscritto a ogni giro.

Uso:
    py -m src.trackrecord.evaluate
"""
from __future__ import annotations

import csv
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database

from src.core.config import load_settings
from src.core.emailer import send_email
from src.core.prices_graded import attach_graded
from src.metrics import getMarketData as gm
from src.trackrecord.ledger import (
    SIGNAL_RULES_VERSION,
    TRACK_RECORD_BENCHMARKS_COLLECTION,
    TRACK_RECORD_CALLS_COLLECTION,
    TRACK_RECORD_DIGESTS_COLLECTION,
    TRACK_RECORD_FORECAST_OUTCOMES_COLLECTION,
    TRACK_RECORD_FORECASTS_COLLECTION,
    TRACK_RECORD_MIN_PRICE_USD,
    TRACK_RECORD_OUTCOMES_COLLECTION,
    TRACK_RECORD_SUMMARY_COLLECTION,
    chain_hash,
    digest_lines,
    ensure_indexes,
    insert_append_only,
)

HORIZONS_DAYS = (28, 56)
FORECAST_HORIZON_DAYS = 28
# Prezzo di uscita: mediana dei prezzi giornalieri in [t+h-3gg, t+h+3gg], cosi' un singolo
# giorno anomalo non decide l'esito. Si valuta solo quando la finestra e' chiusa.
EXIT_WINDOW_DAYS = 3
# Prezzo di ingresso per previsioni e benchmark: ultima riga entro 2 giorni prima della data.
ENTRY_MAX_AGE_DAYS = 2
# Righe in piu' caricate prima della finestra, per riportare in avanti pricePriceCharting.
CARRY_FORWARD_LOOKBACK_DAYS = 10
# Sotto queste soglie la valutazione e' marcata thinMarket (prezzo poco affidabile).
MIN_EXIT_OBSERVATIONS = 3

# Soglia di costo = commissione di vendita + spedizione / prezzo. STIME da allineare alle
# tariffe reali di CardTrader e Cardmarket: se cambiano, alzare EVAL_METHOD_VERSION.
SELL_FEE_PCT = 7.0
SHIPPING_COST_USD = 1.5
EVAL_METHOD_VERSION = "m1"

MIN_SAMPLE = 30
CHUNK_SIZE = 300

STAMP_SIDES = {
    "ACCUMULATE": "buy",
    "BUY_SETUP": "buy",
    "REBOUND_WATCH": "speculative",
    "SELL_WATCH": "sell",
    "HOLD": "hold",
}

PRICE_PROJECTION = {
    "itemId": 1,
    "createdAt": 1,
    "pricePrimary": 1,
    "cmPriceTrend": 1,
    "cmPriceLow": 1,
    "cmAvg30d": 1,
    "priceYuyuTei": 1,
    "priceCardTrader": 1,
    "pricePriceCharting": 1,
    "listings": 1,
    "ctListings": 1,
}

Point = Tuple[datetime, float, Optional[float]]  # (createdAt, priceRedLine, listings totali)


# ---------------------------------------------------------------------------
# Prezzi
# ---------------------------------------------------------------------------

def _day_start(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _load_series(db: Database, item_ids: Iterable[str], start: datetime, end: datetime) -> Dict[str, List[Point]]:
    """Serie priceRedLine per carta in [start, end], crescente, con lo stesso calcolo di getMarketData."""
    out: Dict[str, List[Point]] = {}
    ids = sorted(set(item_ids))
    load_start = start - timedelta(days=CARRY_FORWARD_LOOKBACK_DAYS)
    for i in range(0, len(ids), CHUNK_SIZE):
        query = {"itemId": {"$in": ids[i : i + CHUNK_SIZE]}, "createdAt": {"$gte": load_start, "$lte": end}}
        rows = attach_graded(db, list(db["Prices"].find(query, PRICE_PROJECTION)), query, fields=("pricePriceCharting",))
        per_item: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            per_item[row["itemId"]].append(row)
        for item_id, snaps in per_item.items():
            snaps.sort(key=lambda d: d.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            gm._carry_forward_pricecharting(snaps)
            points: List[Point] = []
            for snap in reversed(snaps):
                created_at = gm._parse_created_at(snap)
                price = gm._compute_price_redline(snap)
                if created_at is None or created_at < start or price is None or price <= 0:
                    continue
                points.append((created_at, price, gm._snapshot_total_listings(snap)))
            if points:
                out[item_id] = points
    return out


def _entry_price(series: List[Point], at: datetime) -> Optional[float]:
    best = None
    for created_at, price, _ in series:
        if created_at <= at and at - created_at <= timedelta(days=ENTRY_MAX_AGE_DAYS):
            best = price
    return best


def _exit_window(series: List[Point], target: datetime) -> Dict[str, Any]:
    lo, hi = target - timedelta(days=EXIT_WINDOW_DAYS), target + timedelta(days=EXIT_WINDOW_DAYS)
    window = [p for p in series if lo <= p[0] <= hi]
    prices = [p[1] for p in window]
    # Unita' vendute STIMATE dal calo dei listing tra righe consecutive (come sales7d).
    sales = 0.0
    has_listings = False
    for prev, cur in zip(window, window[1:]):
        if prev[2] is not None and cur[2] is not None:
            has_listings = True
            sales += max(0.0, prev[2] - cur[2])
    return {
        "price": round(median(prices), 2) if prices else None,
        "observations": len(prices),
        "salesEstimate": int(round(sales)) if has_listings else None,
    }


def _max_drawdown_pct(series: List[Point], entry: float, start: datetime, end: datetime) -> Optional[float]:
    path = [p[1] for p in series if start < p[0] <= end]
    if not path or entry <= 0:
        return None
    return round((min(min(path), entry) / entry - 1.0) * 100.0, 2)


def _band_for_price(price: float) -> str:
    for band in gm.PRICE_BAND_RULES:
        if price < float(band["min"]):
            continue
        if band["max"] is not None and price >= float(band["max"]):
            continue
        return band["key"]
    return "UNKNOWN"


def _pct(a: float, b: float) -> float:
    return round((a / b - 1.0) * 100.0, 2)


def cost_threshold_pct(price: float) -> float:
    return round(SELL_FEE_PCT + SHIPPING_COST_USD / price * 100.0, 2)


# ---------------------------------------------------------------------------
# Benchmark: mercato della stessa fascia nello stesso periodo
# ---------------------------------------------------------------------------

def _get_benchmark(db: Database, day: datetime, horizon: int, item_types: Dict[str, Any]) -> Dict[str, Any]:
    bench_id = f"{day:%Y-%m-%d}|{horizon}"
    coll = db[TRACK_RECORD_BENCHMARKS_COLLECTION]
    existing = coll.find_one({"_id": bench_id})
    if existing:
        return existing

    entry_at = day + timedelta(days=1)  # fine del giorno della chiamata
    target = day + timedelta(days=horizon)
    series = _load_series(db, item_types.keys(), day - timedelta(days=ENTRY_MAX_AGE_DAYS), entry_at)
    exits = _load_series(db, item_types.keys(), target - timedelta(days=EXIT_WINDOW_DAYS), target + timedelta(days=EXIT_WINDOW_DAYS))

    returns: Dict[str, List[float]] = defaultdict(list)
    for item_id, points in series.items():
        entry = _entry_price(points, entry_at)
        exit_price = _exit_window(exits.get(item_id, []), target)["price"]
        if entry is None or exit_price is None or entry < TRACK_RECORD_MIN_PRICE_USD:
            continue
        band = _band_for_price(entry)
        gross = _pct(exit_price, entry)
        returns[f"{item_types.get(item_id) or 'UNKNOWN'}|{band}"].append(gross)
        returns[f"ALL|{band}"].append(gross)
        returns["ALL|ALL"].append(gross)

    doc = {
        "_id": bench_id,
        "day": f"{day:%Y-%m-%d}",
        "horizonDays": horizon,
        "createdAt": datetime.now(timezone.utc),
        "groups": {key: {"medianReturnPct": round(median(vals), 2), "n": len(vals)} for key, vals in returns.items()},
    }
    insert_append_only(coll, [doc])
    return doc


def _benchmark_return(bench: Dict[str, Any], item_type: Any, band: str) -> Optional[float]:
    groups = bench.get("groups") or {}
    for key in (f"{item_type or 'UNKNOWN'}|{band}", f"ALL|{band}"):
        group = groups.get(key)
        if group and group.get("n", 0) >= MIN_SAMPLE:
            return group["medianReturnPct"]
    return None


# ---------------------------------------------------------------------------
# 1. Chiamate
# ---------------------------------------------------------------------------

def signal_side(signal_type: str, key: str) -> str:
    if signal_type == "buyTier":
        return (gm.BUY_TIER_RULES.get(key) or {}).get("side") or "buy"
    return STAMP_SIDES.get(key, "hold")


def _judge(side: str, gross: float, net: float, cost: float, excess: Optional[float]) -> Optional[bool]:
    if side == "buy":
        return net > 0 and (excess is None or excess > 0)
    if side == "speculative":
        return net > 0
    if side == "sell":
        return gross < 0 and (excess is None or excess < 0)
    return abs(gross) <= cost  # hold: il prezzo resta entro la soglia di costo


def evaluate_calls(db: Database, now: datetime, item_types: Dict[str, Any]) -> int:
    calls = db[TRACK_RECORD_CALLS_COLLECTION]
    outcomes = db[TRACK_RECORD_OUTCOMES_COLLECTION]
    written = 0
    for horizon in HORIZONS_DAYS:
        cutoff = now - timedelta(days=horizon + EXIT_WINDOW_DAYS)
        done = set(outcomes.distinct("eventId", {"horizonDays": horizon}))
        pending = [c for c in calls.find({"calledAt": {"$lte": cutoff}}) if c["_id"] not in done]
        by_day: Dict[datetime, List[Dict[str, Any]]] = defaultdict(list)
        for call in pending:
            by_day[_day_start(call["calledAt"])].append(call)

        for day, day_calls in sorted(by_day.items()):
            bench = _get_benchmark(db, day, horizon, item_types)
            end = day + timedelta(days=horizon + EXIT_WINDOW_DAYS + 1)
            series = _load_series(db, [c["itemId"] for c in day_calls], day, end)
            docs = []
            for call in day_calls:
                docs.append(_build_outcome(call, horizon, series.get(call["itemId"], []), bench, now))
            written += insert_append_only(outcomes, docs)
    return written


def _build_outcome(call: Dict[str, Any], horizon: int, series: List[Point], bench: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    called_at = call["calledAt"]
    target = called_at + timedelta(days=horizon)
    entry = (call.get("price") or {}).get("value")
    side = signal_side(call["signalType"], call["toKey"])
    doc: Dict[str, Any] = {
        "_id": f"{call['_id']}|{horizon}d",
        "eventId": call["_id"],
        "horizonDays": horizon,
        "evaluatedAt": now,
        "methodVersion": EVAL_METHOD_VERSION,
        # copie dall'evento, per aggregare senza join
        "calledAt": called_at,
        "itemId": call["itemId"],
        "signalType": call["signalType"],
        "key": call["toKey"],
        "side": side,
        "rulesVersion": call.get("rulesVersion") or SIGNAL_RULES_VERSION,
        "priceBand": call.get("priceBand") or "UNKNOWN",
        "liquidityLevel": (call.get("liquidity") or {}).get("level") or "UNKNOWN",
        "entryPrice": entry,
    }
    exit_info = _exit_window(series, target)
    doc.update({
        "exitPrice": exit_info["price"],
        "exitObservations": exit_info["observations"],
        "exitSalesEstimate": exit_info["salesEstimate"],
    })
    if not entry or entry <= 0 or exit_info["price"] is None:
        doc.update({"status": "no_price", "success": None})
        return doc

    gross = _pct(exit_info["price"], entry)
    cost = cost_threshold_pct(entry)
    net = round(gross - cost, 2)
    bench_ret = _benchmark_return(bench, call.get("itemType"), doc["priceBand"])
    excess = round(gross - bench_ret, 2) if bench_ret is not None else None
    doc.update({
        "status": "evaluated",
        "thinMarket": exit_info["observations"] < MIN_EXIT_OBSERVATIONS or exit_info["salesEstimate"] == 0,
        "grossReturnPct": gross,
        "costThresholdPct": cost,
        "netReturnPct": net,
        "benchmarkReturnPct": bench_ret,
        "excessReturnPct": excess,
        "maxDrawdownPct": _max_drawdown_pct(series, entry, called_at, target),
        "success": _judge(side, gross, net, cost, excess),
    })
    return doc


# ---------------------------------------------------------------------------
# 2. Previsioni
# ---------------------------------------------------------------------------

def evaluate_forecasts(db: Database, now: datetime) -> int:
    forecasts = db[TRACK_RECORD_FORECASTS_COLLECTION]
    outcomes = db[TRACK_RECORD_FORECAST_OUTCOMES_COLLECTION]
    cutoff_day = (now - timedelta(days=FORECAST_HORIZON_DAYS + EXIT_WINDOW_DAYS)).strftime("%Y-%m-%d")
    done_days = set(outcomes.distinct("asOfDay"))
    written = 0
    for day in sorted(d for d in forecasts.distinct("asOfDay", {"asOfDay": {"$lte": cutoff_day}}) if d not in done_days):
        rows = list(forecasts.find({"asOfDay": day}))
        if not rows:
            continue
        as_of = rows[0]["asOfDate"]
        target = as_of + timedelta(days=FORECAST_HORIZON_DAYS)
        series = _load_series(db, [r["cardId"] for r in rows], as_of - timedelta(days=ENTRY_MAX_AGE_DAYS), target + timedelta(days=EXIT_WINDOW_DAYS))
        docs = []
        for row in rows:
            points = series.get(row["cardId"], [])
            entry = _entry_price(points, row["asOfDate"])
            exit_price = _exit_window(points, target)["price"]
            doc = {
                "_id": row["_id"],
                "forecastId": row["_id"],
                "cardId": row["cardId"],
                "asOfDay": day,
                "tier": row.get("tier"),
                "evaluatedAt": now,
                "entryPrice": entry,
                "exitPrice": exit_price,
            }
            if not entry or exit_price is None:
                doc["status"] = "no_price"
            else:
                realized = exit_price / entry - 1.0
                bucket = "below_q20" if realized < row["q20"] else ("above_q80" if realized > row["q80"] else "inside")
                doc.update({
                    "status": "evaluated",
                    "realizedReturn": round(realized, 4),
                    "bucket": bucket,
                    "absErrorQ50": round(abs(realized - row["q50"]), 4),
                    "absErrorNaive": round(abs(realized), 4),
                })
            docs.append(doc)
        written += insert_append_only(outcomes, docs)
    return written


# ---------------------------------------------------------------------------
# 3. Riepilogo
# ---------------------------------------------------------------------------

def _q(values: List[float], q: float) -> Optional[float]:
    return round(gm._quantile(values, q), 2) if values else None


def _call_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    evaluated = [r for r in rows if r.get("status") == "evaluated"]
    n = len(evaluated)
    enough = n >= MIN_SAMPLE
    net = [r["netReturnPct"] for r in evaluated]
    gross = [r["grossReturnPct"] for r in evaluated]
    excess = [r["excessReturnPct"] for r in evaluated if r.get("excessReturnPct") is not None]
    drawdown = [r["maxDrawdownPct"] for r in evaluated if r.get("maxDrawdownPct") is not None]
    wins = sum(1 for r in evaluated if r.get("success"))
    return {
        "calls": len(rows),
        "evaluated": n,
        "noPrice": len(rows) - n,
        "thinMarket": sum(1 for r in evaluated if r.get("thinMarket")),
        "sampleTooSmall": not enough,
        "successRatePct": round(wins / n * 100.0, 1) if enough else None,
        "medianNetReturnPct": _q(net, 0.5) if enough else None,
        "p80NetReturnPct": _q(net, 0.8) if enough else None,
        "medianExcessReturnPct": _q(excess, 0.5) if enough and excess else None,
        "medianMaxDrawdownPct": _q(drawdown, 0.5) if enough and drawdown else None,
        # Sell Watch: perdita evitata = quanto e' sceso il prezzo dopo la chiamata
        "medianAvoidedLossPct": _q([-g for g in gross], 0.5) if enough else None,
    }


def _forecast_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    evaluated = [r for r in rows if r.get("status") == "evaluated"]
    n = len(evaluated)
    enough = n >= MIN_SAMPLE

    def share(bucket: str) -> Optional[float]:
        return round(sum(1 for r in evaluated if r["bucket"] == bucket) / n * 100.0, 1) if enough else None

    err_model = _q([r["absErrorQ50"] * 100.0 for r in evaluated], 0.5) if enough else None
    err_naive = _q([r["absErrorNaive"] * 100.0 for r in evaluated], 0.5) if enough else None
    return {
        "forecasts": len(rows),
        "evaluated": n,
        "sampleTooSmall": not enough,
        "belowQ20Pct": share("below_q20"),  # atteso ~20
        "insidePct": share("inside"),  # atteso ~60
        "aboveQ80Pct": share("above_q80"),  # atteso ~20
        "medianAbsErrorQ50Pct": err_model,
        "medianAbsErrorNaivePct": err_naive,
        "beatsNaive": (err_model < err_naive) if err_model is not None and err_naive is not None else None,
    }


def build_summary(db: Database, now: datetime) -> Dict[str, Any]:
    outcomes = list(db[TRACK_RECORD_OUTCOMES_COLLECTION].find({}, {"_id": 0, "eventId": 0, "itemId": 0}))
    groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
    for r in outcomes:
        for band in ("ALL", r["priceBand"]):
            for liq in ("ALL", r["liquidityLevel"]):
                groups[(r["rulesVersion"], r["signalType"], r["key"], r["horizonDays"], band, liq)].append(r)

    calls_table = [
        {
            "rulesVersion": k[0], "signalType": k[1], "key": k[2], "horizonDays": k[3],
            "priceBand": k[4], "liquidityLevel": k[5], **_call_stats(rows),
        }
        for k, rows in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0]))
    ]

    forecast_rows = list(db[TRACK_RECORD_FORECAST_OUTCOMES_COLLECTION].find({}, {"tier": 1, "status": 1, "bucket": 1, "absErrorQ50": 1, "absErrorNaive": 1}))
    by_tier: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in forecast_rows:
        by_tier["ALL"].append(r)
        by_tier[r.get("tier") or "UNKNOWN"].append(r)
    forecasts_table = [{"tier": tier, **_forecast_stats(rows)} for tier, rows in sorted(by_tier.items())]

    def pick(key: str, horizon: int) -> Dict[str, Any]:
        return next(
            (row for row in calls_table
             if row["rulesVersion"] == SIGNAL_RULES_VERSION and row["signalType"] == "stamp" and row["key"] == key
             and row["horizonDays"] == horizon and row["priceBand"] == "ALL" and row["liquidityLevel"] == "ALL"),
            {},
        )

    first_call = db[TRACK_RECORD_CALLS_COLLECTION].find_one({}, {"calledAt": 1}, sort=[("calledAt", 1)])
    accumulate = pick("ACCUMULATE", 28)
    sell_watch = pick("SELL_WATCH", 28)
    calibration = next((r for r in forecasts_table if r["tier"] == "ALL"), {})
    summary = {
        "_id": "latest",
        "generatedAt": now,
        "liveSince": first_call.get("calledAt") if first_call else None,
        "currentRulesVersion": SIGNAL_RULES_VERSION,
        "methodVersion": EVAL_METHOD_VERSION,
        "method": {
            "priceSource": "priceRedLine (mediana delle fonti), USD",
            "exitWindowDays": EXIT_WINDOW_DAYS,
            "horizonsDays": list(HORIZONS_DAYS),
            "sellFeePct": SELL_FEE_PCT,
            "shippingCostUsd": SHIPPING_COST_USD,
            "minSample": MIN_SAMPLE,
            "benchmark": "mediana del rendimento di tutte le carte dello stesso tipo e fascia di prezzo, stesso periodo",
        },
        "headline": {
            "accumulateSuccessRate4wPct": accumulate.get("successRatePct"),
            "accumulateEvaluated4w": accumulate.get("evaluated", 0),
            "sellWatchMedianAvoidedLoss4wPct": sell_watch.get("medianAvoidedLossPct"),
            "sellWatchEvaluated4w": sell_watch.get("evaluated", 0),
            "forecastInsideQ20Q80Pct": calibration.get("insidePct"),
            "forecastsEvaluated": calibration.get("evaluated", 0),
        },
        "calls": calls_table,
        "forecasts": forecasts_table,
    }
    db[TRACK_RECORD_SUMMARY_COLLECTION].replace_one({"_id": "latest"}, summary, upsert=True)
    return summary


# ---------------------------------------------------------------------------
# 4. Impronta giornaliera
# ---------------------------------------------------------------------------

def _day_digest(db: Database, day: datetime) -> Dict[str, Any]:
    day_str = f"{day:%Y-%m-%d}"
    calls = db[TRACK_RECORD_CALLS_COLLECTION].find({"calledAt": {"$gte": day, "$lt": day + timedelta(days=1)}})
    forecasts = db[TRACK_RECORD_FORECASTS_COLLECTION].find({"asOfDay": day_str})
    return {"calls": digest_lines(calls), "forecasts": digest_lines(forecasts)}


def update_digests(db: Database, now: datetime) -> Tuple[int, List[str]]:
    coll = db[TRACK_RECORD_DIGESTS_COLLECTION]
    mismatches: List[str] = []

    # Verifica: tutto il passato ricalcolato deve dare la stessa impronta.
    for stored in coll.find({}):
        day = datetime.strptime(stored["_id"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        current = _day_digest(db, day)
        if current["calls"]["sha256"] != stored["callsSha256"] or current["forecasts"]["sha256"] != stored["forecastsSha256"]:
            mismatches.append(stored["_id"])

    last = coll.find_one({}, sort=[("_id", -1)])
    if last:
        day = datetime.strptime(last["_id"], "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        prev_chain = last["chainSha256"]
    else:
        first = db[TRACK_RECORD_CALLS_COLLECTION].find_one({}, {"calledAt": 1}, sort=[("calledAt", 1)])
        if not first:
            return (0, mismatches)
        day = _day_start(first["calledAt"])
        prev_chain = None

    written = 0
    today = _day_start(now)
    while day < today:  # solo giorni chiusi
        d = _day_digest(db, day)
        day_str = f"{day:%Y-%m-%d}"
        chain = chain_hash(prev_chain, day_str, d["calls"]["sha256"], d["forecasts"]["sha256"])
        written += insert_append_only(coll, [{
            "_id": day_str,
            "callsCount": d["calls"]["count"],
            "callsSha256": d["calls"]["sha256"],
            "forecastsCount": d["forecasts"]["count"],
            "forecastsSha256": d["forecasts"]["sha256"],
            "prevChainSha256": prev_chain,
            "chainSha256": chain,
            "createdAt": now,
        }])
        prev_chain = chain
        day += timedelta(days=1)
    return (written, mismatches)


def write_digest_file(db: Database, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["day", "callsCount", "callsSha256", "forecastsCount", "forecastsSha256", "chainSha256"])
        for d in db[TRACK_RECORD_DIGESTS_COLLECTION].find({}, sort=[("_id", 1)]):
            writer.writerow([d["_id"], d["callsCount"], d["callsSha256"], d["forecastsCount"], d["forecastsSha256"], d["chainSha256"]])


def run(db: Database) -> str:
    now = datetime.now(timezone.utc)
    ensure_indexes(db)
    item_types = {c["id"]: c.get("type") for c in db["Cards"].find({}, {"id": 1, "type": 1}) if isinstance(c.get("id"), str)}

    calls_written = evaluate_calls(db, now, item_types)
    forecasts_written = evaluate_forecasts(db, now)
    summary = build_summary(db, now)
    digests_written, mismatches = update_digests(db, now)

    digest_file = os.getenv("TRACKRECORD_DIGEST_FILE")
    if digest_file:
        write_digest_file(db, digest_file)

    report = (
        f"Esiti chiamate scritti: {calls_written}\n"
        f"Esiti previsioni scritti: {forecasts_written}\n"
        f"Impronte giornaliere scritte: {digests_written}\n"
        f"Headline: {summary['headline']}\n"
    )
    if mismatches:
        report += f"\nATTENZIONE: impronta non corrispondente per i giorni {', '.join(mismatches)}\n"
    return report


if __name__ == "__main__":
    load_dotenv(".env.local")
    load_dotenv()
    GAME = os.getenv("GAME") or "N/A"
    try:
        start_time = time.time()
        settings = load_settings()
        client = MongoClient(settings.mongodb_uri, tz_aware=True)
        report = run(client[settings.mongodb_db])
        elapsed = time.time() - start_time
        print(report)
        print(f"Tempo totale: {elapsed:.1f} secondi")
        subject = "[WORKFLOW] Track Record: " + GAME
        send_email(("🚫 " if "ATTENZIONE" in report else "✅ ") + subject, report + f"\nDurata: {elapsed / 60.0:.1f} minuti")
    except Exception:
        send_email("🚫 [WORKFLOW] Track Record: " + GAME, traceback.format_exc())
        raise
