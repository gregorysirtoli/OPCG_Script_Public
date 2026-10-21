"""
Track record pubblico: registro delle chiamate (stamp) e delle previsioni a 28 giorni.
Si verifica solo lo stamp; il pattern (buy tier) attivo resta come contesto in buyTierAtCall.

Regole del registro (vedi docs/trackrecord.md):
  - solo in aggiunta: si scrive con insert, mai update/delete. L'_id e' deterministico, cosi'
    un secondo insert dello stesso evento fallisce invece di sovrascriverlo;
  - un evento solo quando lo stamp di una carta cambia, non ogni giorno (cooldown di 14 giorni
    sullo stesso stamp, vedi REENTRY_COOLDOWN_DAYS);
  - ogni evento porta la versione delle regole che l'ha prodotto: quando le regole cambiano si
    alza la versione, lo storico delle versioni precedenti resta com'e' (nessun reset);
  - gli esiti a 4 e 8 settimane stanno in TrackRecordOutcomes, scritti dal job di valutazione:
    la riga dell'evento non si modifica mai.

CardsStampChanges NON e' il registro: il backfill delle regole la riscrive con le regole attuali.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pymongo.database import Database
from pymongo.errors import BulkWriteError

TRACK_RECORD_CALLS_COLLECTION = "TrackRecordCalls"
TRACK_RECORD_FORECASTS_COLLECTION = "TrackRecordForecasts"
TRACK_RECORD_OUTCOMES_COLLECTION = "TrackRecordOutcomes"
TRACK_RECORD_FORECAST_OUTCOMES_COLLECTION = "TrackRecordForecastOutcomes"
TRACK_RECORD_BENCHMARKS_COLLECTION = "TrackRecordBenchmarks"
TRACK_RECORD_SUMMARY_COLLECTION = "TrackRecordSummary"
TRACK_RECORD_DIGESTS_COLLECTION = "TrackRecordDigests"

# Versione delle regole di stamp (src/metrics/getMarketData.py). Va alzata a ogni modifica di
# soglie o logica di _classify_stamp / _compute_stamp_inputs / _classify_price_band: le chiamate
# gia' registrate restano con la loro versione. Il primo giorno di una nuova versione non si
# registra nulla (vedi update_cards_market_data): i cambi di quel giorno vengono dalle regole.
#   v1: soglie su variazioni di un solo giorno.
#   v2 (ottobre 2026): prezzi su mediane di 3 giorni, isteresi in uscita, soglie vicine allo
#       zero rese proporzionate alla fascia, Sell Watch = prezzo in calo con offerta in aumento.
#       -64% di cambi di stamp; Sell Watch da 48% a 67% di chiamate giuste (docs/trackrecord.md).
SIGNAL_RULES_VERSION = "v2"

# Sotto questo prezzo (USD) commissione e spedizione superano il movimento del prezzo: le
# chiamate non entrano nel registro e le carte non entrano nel benchmark di mercato.
TRACK_RECORD_MIN_PRICE_USD = 5.0

# Prezzo usato per stamp e valutazione: priceRedLine (mediana delle fonti della riga di Prices), USD.
PRICE_SOURCE = "priceRedLine"
PRICE_CURRENCY = "USD"

DUPLICATE_KEY_ERROR_CODE = 11000

# Uno stamp che oscilla (es. Buy Setup -> Hold -> Buy Setup in pochi giorni) non deve contare come
# tante chiamate quasi uguali: se la carta ha gia' una chiamata registrata con lo stesso segnale negli
# ultimi REENTRY_COOLDOWN_DAYS giorni, il nuovo ingresso non viene registrato. La finestra parte
# dall'ultima chiamata REGISTRATA, non da quella saltata: uno stamp che oscilla per mesi viene
# registrato al massimo una volta ogni 14 giorni.
REENTRY_COOLDOWN_DAYS = 14

CallKey = Tuple[str, str, str]  # (itemId, signalType, toKey)


def recent_calls_index(db: Database, item_ids: List[str], now: datetime) -> Dict[CallKey, datetime]:
    """Ultima chiamata registrata per (carta, tipo, segnale) dentro la finestra di cooldown."""
    since = now - timedelta(days=REENTRY_COOLDOWN_DAYS)
    index: Dict[CallKey, datetime] = {}
    for doc in db[TRACK_RECORD_CALLS_COLLECTION].find(
        {"itemId": {"$in": item_ids}, "calledAt": {"$gte": since}},
        {"itemId": 1, "signalType": 1, "toKey": 1, "calledAt": 1},
    ):
        key = (doc["itemId"], doc["signalType"], doc["toKey"])
        if key not in index or doc["calledAt"] > index[key]:
            index[key] = doc["calledAt"]
    return index


def accept_call(index: Dict[CallKey, datetime], event: Dict[str, Any]) -> bool:
    """True se l'evento va registrato (e aggiorna l'indice); False se cade nel cooldown."""
    key = (event["itemId"], event["signalType"], event["toKey"])
    last = index.get(key)
    if last is not None and event["calledAt"] - last < timedelta(days=REENTRY_COOLDOWN_DAYS):
        return False
    index[key] = event["calledAt"]
    return True


def liquidity_level(liquidity_score: Optional[float], listings: Optional[float] = None) -> str:
    # Stesse soglie del badge di liquidita' sul sito (OPCG/components/Card.tsx).
    if liquidity_score is not None and liquidity_score > 0:
        for threshold, level in ((85, "VERY_HIGH"), (67, "HIGH"), (34, "MEDIUM"), (15, "LOW")):
            if liquidity_score >= threshold:
                return level
        return "VERY_LOW"
    if listings is not None:
        for threshold, level in ((50, "VERY_HIGH"), (20, "HIGH"), (8, "MEDIUM"), (2, "LOW")):
            if listings >= threshold:
                return level
        return "VERY_LOW"
    return "UNKNOWN"


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_call_event(
    *,
    item_id: str,
    signal_type: str,
    from_key: Optional[str],
    to_signal: Dict[str, Any],
    called_at: datetime,
    base_card: Dict[str, Any],
    md: Dict[str, Any],
) -> Dict[str, Any]:
    """
    signal_type: "stamp" o "buyTier". to_signal e' il dict dello stamp/buy tier appena
    calcolato (key, label, kind/side). md e' il marketData della carta al momento della chiamata.
    """
    price_band = md.get("priceBand") if isinstance(md.get("priceBand"), dict) else {}
    to_key = to_signal.get("key")
    return {
        "_id": f"{item_id}|{signal_type}|{called_at.strftime('%Y%m%dT%H%M%S')}",
        "calledAt": called_at,
        "itemId": item_id,
        "name": base_card.get("name"),
        "setId": base_card.get("setId"),
        "itemType": base_card.get("type"),
        "condition": "RAW",
        "signalType": signal_type,
        "fromKey": from_key or None,
        "toKey": to_key,
        "label": to_signal.get("label"),
        "kind": to_signal.get("kind") or to_signal.get("side"),
        "rulesVersion": SIGNAL_RULES_VERSION,
        "price": {
            "value": md.get("price"),
            "source": PRICE_SOURCE,
            "currency": PRICE_CURRENCY,
            "priceConfidenceScore": md.get("priceConfidenceScore"),
        },
        "liquidity": {
            "score": md.get("liquidityScore"),
            "level": liquidity_level(md.get("liquidityScore"), md.get("listings")),
            "sellers": md.get("sellers"),
            "listings": md.get("listings"),
            # STIME dal calo dei listing, non vendite reali (vedi _compute_sales_estimate)
            "sales7dEstimate": md.get("sales7d"),
            "sales30dEstimate": md.get("sales30d"),
        },
        "priceBand": price_band.get("key") or "UNKNOWN",
        # Variazioni su mediane di 3 giorni usate dallo stamp (regole v2).
        "inputs": dict(md.get("stampInputs") or {}),
        "stampAtCall": (md.get("stamp") or {}).get("key"),
        "buyTierAtCall": (md.get("buyTier") or {}).get("key"),
    }


def insert_append_only(collection: Any, docs: List[Dict[str, Any]]) -> int:
    """Insert senza sovrascrivere: i duplicati (stesso _id) vengono ignorati. Ritorna gli inseriti."""
    if not docs:
        return 0
    try:
        res = collection.insert_many(docs, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as bwe:
        details = bwe.details or {}
        errors = details.get("writeErrors", [])
        if errors and all(e.get("code") == DUPLICATE_KEY_ERROR_CODE for e in errors):
            return int(details.get("nInserted") or 0)
        raise


# ---------------------------------------------------------------------------
# Impronta giornaliera (SHA-256), verificabile da fuori
# ---------------------------------------------------------------------------

def _canonical_number(value: float) -> str:
    # Regola riproducibile anche in JavaScript (toFixed(6) senza zeri finali): json.dumps
    # scriverebbe 10.0 e 1e-05, che JSON.stringify scrive 10 e 0.00001.
    if value != value or value in (float("inf"), float("-inf")):
        return "null"
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def _canonical(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _canonical_number(value)
    if isinstance(value, datetime):
        return json.dumps(_iso_utc(value))
    if isinstance(value, date):
        return json.dumps(value.isoformat())
    if isinstance(value, dict):
        items = sorted((str(k), v) for k, v in value.items())
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _canonical(v) for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical(v) for v in value) + "]"
    return json.dumps(str(value), ensure_ascii=False)


def canonical_line(doc: Dict[str, Any]) -> str:
    """Una riga JSON canonica: chiavi ordinate, nessuno spazio, date ISO UTC al secondo con Z,
    numeri con al massimo 6 decimali senza zeri finali. L'export del sito (lib/trackRecord.ts)
    produce esattamente queste righe, cosi' chiunque puo' rifare l'hash."""
    return _canonical(doc)


def digest_lines(docs: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    lines = sorted(canonical_line(d) for d in docs)
    payload = "\n".join(lines).encode("utf-8")
    return {"count": len(lines), "sha256": hashlib.sha256(payload).hexdigest()}


def chain_hash(prev_chain: Optional[str], day: str, calls_sha: str, forecasts_sha: str) -> str:
    raw = f"{prev_chain or ''}|{day}|{calls_sha}|{forecasts_sha}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def ensure_indexes(db: Database) -> None:
    db[TRACK_RECORD_CALLS_COLLECTION].create_index([("calledAt", 1)])
    db[TRACK_RECORD_CALLS_COLLECTION].create_index([("itemId", 1), ("calledAt", 1)])
    db[TRACK_RECORD_FORECASTS_COLLECTION].create_index([("asOfDay", 1)])
    db[TRACK_RECORD_OUTCOMES_COLLECTION].create_index([("eventId", 1)])
    db[TRACK_RECORD_FORECAST_OUTCOMES_COLLECTION].create_index([("forecastId", 1)])
