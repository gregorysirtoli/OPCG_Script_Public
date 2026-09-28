"""
Script one-off: ricostruisce CardsStampChanges per gli ultimi N giorni usando la
logica di classificazione CORRENTE (price redline a mediana, prezzo unificato,
baseline ancorati alla data dell'ultima osservazione invece che al wall-clock).

Necessario perché le modifiche fatte a src/metrics/getMarketData.py in questa sessione
(vedi docs/marketData.md) cambiano quando/come scattano priceBand/buyTier/stamp: lo
storico di CardsStampChanges accumulato con la logica vecchia non riflette più le
transizioni reali (es. non conteneva l'Hard Drop del 19 settembre per alcune carte).

Uso:
    py -m src.scripts.backfill_stamp_changes            # dry-run (nessuna scrittura)
    py -m src.scripts.backfill_stamp_changes --commit    # cancella+riscrive per davvero

Da eseguire una tantum, poi puoi cancellare questo file.
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pymongo import InsertOne, MongoClient
from pymongo.errors import AutoReconnect, PyMongoError

from src.metrics.getMarketData import (
    _calc_pct_change,
    _classify_buy_tier,
    _classify_price_band,
    _classify_stamp,
    _compute_listing_pct_changes,
    _compute_price_redline,
    _get_closest_around,
    _round2,
    _to_number,
)
# ONE PIECE
#MONGODB_URI = "mongodb+srv://gregorysirtoli-test:SYXPiV184NvToXIB@tcg.r74ywaw.mongodb.net"
#MONGODB_DB = "TCG"
#MONGODB_SALES_URI = "mongodb+srv://gregorysirtoli_db_user:49WrKttESKcBIn0h@sales-history.hzxnrt2.mongodb.net/"
#MONGODB_SALES_DB = "OPCG"

# LORCANA
MONGODB_URI="mongodb+srv://gregorysirtoli_db_user:lWQ9NhePV5k5hf8P@live.vruk1wi.mongodb.net"
MONGODB_DB="LIVE"
MONGODB_SALES_URI="mongodb+srv://gregorysirtoli_db_user:ITmHxBuqhsKSSe6f@lrc.6zqiu0b.mongodb.net"
MONGODB_SALES_DB="LRC"

BACKFILL_DAYS = 90
# Storico necessario: finestra di backfill + 1 giorno seed + il baseline piu' lungo
# usato dai classificatori (pct90, tolleranza 14gg) con un margine di sicurezza.
LOOKBACK_DAYS = BACKFILL_DAYS + 1 + 90 + 20

# Un'unica query su tutto il catalogo (14k+ carte x 200gg = milioni di documenti) tende
# a farsi cancellare la connessione a meta' strada. Si spezza il fetch per blocchi di
# itemId: query piu' corte, meno memoria di picco, progresso visibile piu' spesso.
CHUNK_SIZE = 300
FETCH_RETRIES = 3
FETCH_RETRY_SLEEP_SECONDS = 5

STAMP_CHANGES_COLLECTION = "CardsStampChanges"
RETENTION_DAYS = 180


def _ensure_retention(db) -> None:
    """Imposta/aggiorna il TTL a RETENTION_DAYS su CardsStampChanges, sia che sia una
    collection time series (collMod su expireAfterSeconds) sia che sia una collection
    normale gia' esistente (indice TTL classico su changedAt) - 'expireAfterSeconds' via
    collMod e' supportato solo su collection time series/clustered by _id, quindi va
    distinto il caso per non rompersi su una collection normale gia' popolata."""
    info = db.command("listCollections", filter={"name": STAMP_CHANGES_COLLECTION})
    batch = info.get("cursor", {}).get("firstBatch", [])
    if not batch:
        return
    coll_info = batch[0]
    target_seconds = RETENTION_DAYS * 86400

    if coll_info.get("type") == "timeseries":
        current_seconds = (coll_info.get("options") or {}).get("expireAfterSeconds")
        if current_seconds == target_seconds:
            return
        db.command("collMod", STAMP_CHANGES_COLLECTION, expireAfterSeconds=target_seconds)
        print(f"Retention TTL impostata a {RETENTION_DAYS} giorni (time series) su "
              f"'{STAMP_CHANGES_COLLECTION}' (era: {current_seconds}).")
        return

    # Collection normale (non time series): retention via indice TTL su changedAt.
    coll = db[STAMP_CHANGES_COLLECTION]
    existing_ttl_index = next(
        (idx for idx in coll.list_indexes() if idx.get("key") == {"changedAt": 1} and "expireAfterSeconds" in idx),
        None,
    )
    if existing_ttl_index is not None:
        if existing_ttl_index["expireAfterSeconds"] == target_seconds:
            return
        db.command(
            "collMod",
            STAMP_CHANGES_COLLECTION,
            index={"keyPattern": {"changedAt": 1}, "expireAfterSeconds": target_seconds},
        )
        print(f"Retention TTL aggiornata a {RETENTION_DAYS} giorni (indice) su "
              f"'{STAMP_CHANGES_COLLECTION}' (era: {existing_ttl_index['expireAfterSeconds']}).")
    else:
        coll.create_index([("changedAt", 1)], expireAfterSeconds=target_seconds, name="changedAt_ttl")
        print(f"Indice TTL creato: retention a {RETENTION_DAYS} giorni su '{STAMP_CHANGES_COLLECTION}'.")


def ensure_stamp_changes_collection(db) -> None:
    """
    Crea CardsStampChanges come time series collection, solo se non esiste gia', con
    retention automatica a RETENTION_DAYS giorni. E' una collection append-only di
    eventi datati (un cambio di stamp/tier per carta), quindi si presta bene al formato
    time series: MongoDB raggruppa i documenti in "bucket" compressi invece di salvarli
    come documenti indipendenti, riducendo spazio su disco e velocizzando le query per
    intervallo di date.

    - timeField=changedAt: obbligatorio, la data dell'evento.
    - metaField=itemId: raggruppa nello stesso bucket tutti gli eventi della stessa
      carta (itemId non cambia mai per una data "serie" di eventi, e' esattamente cosa
      deve essere un metaField). Riduce anche il costo delle query per singola carta.
    - granularity="hours": gli eventi arrivano al massimo una volta al giorno per
      carta (spesso molto meno), quindi bucket ampi fino a 24h non perdono risoluzione
      utile e riducono l'overhead di storage rispetto a "minutes"/"seconds".
    - expireAfterSeconds=RETENTION_DAYS*86400: TTL automatico, MongoDB elimina da solo
      in background i documenti piu' vecchi di RETENTION_DAYS giorni.
    """
    if STAMP_CHANGES_COLLECTION in db.list_collection_names():
        _ensure_retention(db)
        return

    db.create_collection(
        STAMP_CHANGES_COLLECTION,
        timeseries={
            "timeField": "changedAt",
            "metaField": "itemId",
            "granularity": "hours",
        },
        expireAfterSeconds=RETENTION_DAYS * 86400,
    )
    # Indice di supporto per le query piu' comuni: cambi di una carta in un intervallo
    # di date (il metaField da solo non copre l'ordinamento/filtro su changedAt).
    db[STAMP_CHANGES_COLLECTION].create_index([("itemId", 1), ("changedAt", 1)])
    print(f"Collection '{STAMP_CHANGES_COLLECTION}' creata come time series "
          f"(timeField=changedAt, metaField=itemId, granularity=hours, retention={RETENTION_DAYS}gg) "
          "+ indice itemId+changedAt.")


PRICE_PROJECTION = {
    "itemId": 1,
    "createdAt": 1,
    "pricePrimary": 1,
    "cmPriceTrend": 1,
    "cmPriceLow": 1,
    "cmAvg30d": 1,
    "priceYuyuTei": 1,
    "pricePriceCharting": 1,
    "priceCardTrader": 1,
    "sellers": 1,
    "listings": 1,
    "ctSellers": 1,
    "ctListings": 1,
}


def _spread_from_latest(latest: Optional[Dict[str, Any]]) -> Optional[float]:
    cm_trend = _to_number((latest or {}).get("cmPriceTrend"))
    cm_low = _to_number((latest or {}).get("cmPriceLow"))
    if cm_trend is not None and cm_trend > 0 and cm_low is not None:
        return _round2(((cm_trend - cm_low) / cm_trend) * 100.0)
    return None


def classify_day(prices_desc_as_of: List[Dict[str, Any]], day_cutoff: datetime, set_id: Optional[str]):
    """Replica leggera di compute_market_data_for_item: solo priceBand/buyTier/stamp,
    niente avgGain4w/8w (O(n^2), non serve qui) ne' grading. prices_desc_as_of deve
    essere gia' filtrata a createdAt <= day_cutoff e ordinata createdAt discendente."""
    if not prices_desc_as_of:
        return None

    latest = prices_desc_as_of[0]
    price_now = _compute_price_redline(latest)
    if price_now is None:
        return None

    def redline_at(days_ago: int, max_days: float) -> Optional[float]:
        doc = _get_closest_around(prices_desc_as_of, day_cutoff - timedelta(days=days_ago), max_days=max_days)
        return _compute_price_redline(doc) if doc else None

    b1 = redline_at(1, 1.75)
    b7 = redline_at(7, 3.5)
    b30 = redline_at(30, 7)
    b90 = redline_at(90, 14)

    pct1 = _calc_pct_change(price_now, b1)
    pct7 = _calc_pct_change(price_now, b7)
    pct30 = _calc_pct_change(price_now, b30)
    pct90 = _calc_pct_change(price_now, b90)

    listings_pct7, listings_pct30 = _compute_listing_pct_changes(prices_desc_as_of, day_cutoff)
    spread_pct = _spread_from_latest(latest)

    redline_series = [_compute_price_redline(p) for p in prices_desc_as_of]
    redline_series = [v for v in redline_series if v is not None and v > 0]
    ath = max(redline_series) if redline_series else None
    near_ath = (ath is not None and price_now is not None and ath > 0 and ((ath - price_now) / ath * 100.0) <= 8.0)

    price_band = _classify_price_band(b30, price_now)
    band_key = price_band.get("key", "UNKNOWN")
    buy_tier = _classify_buy_tier(
        set_id, band_key, pct1, pct7, pct30, pct90, spread_pct, listings_pct7, listings_pct30, near_ath
    )
    stamp = _classify_stamp(band_key, pct1, pct7, pct30, listings_pct30, buy_tier)

    return price_band, buy_tier, stamp


def fetch_prices_chunk(db, chunk_ids: List[str], fetch_since: datetime) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch con retry: una query cursor troppo lunga su tutto il catalogo tende a farsi
    cancellare la connessione a meta' strada, quindi si limita a un blocco di itemId
    per volta e si ritenta se la connessione cade."""
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            per_item: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            cursor = db["Prices"].find(
                {"itemId": {"$in": chunk_ids}, "createdAt": {"$gte": fetch_since}},
                PRICE_PROJECTION,
            )
            for doc in cursor:
                per_item[doc["itemId"]].append(doc)
            for docs in per_item.values():
                docs.sort(key=lambda d: d["createdAt"], reverse=True)
            return per_item
        except (AutoReconnect, PyMongoError) as exc:
            if attempt == FETCH_RETRIES:
                raise
            print(f"    [retry {attempt}/{FETCH_RETRIES}] fetch fallito ({exc}), ritento tra {FETCH_RETRY_SLEEP_SECONDS}s...")
            time.sleep(FETCH_RETRY_SLEEP_SECONDS)
    return {}


def process_card(
    item_id: str,
    card: Dict[str, Any],
    prices_desc: List[Dict[str, Any]],
    days: List[datetime],
    seed_day: datetime,
    change_ops: List[InsertOne],
    examples: List[str],
) -> None:
    set_id = card.get("setId")
    prev_stamp_key: Optional[str] = None

    for day in days:
        day_cutoff = day.replace(hour=23, minute=59, second=59, microsecond=999999)
        prices_as_of = [p for p in prices_desc if p["createdAt"] <= day_cutoff]
        result = classify_day(prices_as_of, day_cutoff, set_id)
        if result is None:
            continue
        price_band, buy_tier, stamp = result
        curr_stamp_key = (stamp or {}).get("key") or "HOLD"

        is_seed = day == seed_day
        if not is_seed and prev_stamp_key is not None and curr_stamp_key != prev_stamp_key:
            change_ops.append(
                InsertOne(
                    {
                        "itemId": item_id,
                        "setId": set_id,
                        "itemType": card.get("type"),
                        "fromStamp": prev_stamp_key,
                        "toStamp": curr_stamp_key,
                        "changedAt": day_cutoff,
                        "priceBandAtCall": price_band,
                        "buyTierAtCall": buy_tier,
                    }
                )
            )
            if len(examples) < 15:
                examples.append(f"{item_id} ({card.get('type')}): {prev_stamp_key} -> {curr_stamp_key} il {day.date()}")

        prev_stamp_key = curr_stamp_key


def main() -> None:
    dry_run = "--commit" not in sys.argv

    client = MongoClient(MONGODB_URI, tz_aware=True)
    db = client[MONGODB_DB]

    if MONGODB_SALES_URI == MONGODB_URI:
        sales_client = client
    else:
        sales_client = MongoClient(MONGODB_SALES_URI, tz_aware=True)
    sales_db = sales_client[MONGODB_SALES_DB]
    ensure_stamp_changes_collection(sales_db)

    now_real = datetime.now(timezone.utc)

    # Retention: oltre al TTL automatico (che agisce in background, non subito), si
    # ripulisce esplicitamente e da subito quello che e' gia' piu' vecchio di
    # RETENTION_DAYS, cosi' non si aspetta il prossimo giro del TTL monitor di Mongo.
    retention_cutoff = now_real - timedelta(days=RETENTION_DAYS)
    coll_changes_retention = sales_db[STAMP_CHANGES_COLLECTION]
    old_count = coll_changes_retention.count_documents({"changedAt": {"$lt": retention_cutoff}})
    if old_count:
        if dry_run:
            print(f"[dry-run] {old_count:,} record piu' vecchi di {RETENTION_DAYS}gg "
                  f"(prima del {retention_cutoff.date()}) verrebbero eliminati (retention).")
        else:
            retention_result = coll_changes_retention.delete_many({"changedAt": {"$lt": retention_cutoff}})
            print(f"Retention: eliminati {retention_result.deleted_count:,} record "
                  f"piu' vecchi di {RETENTION_DAYS}gg (prima del {retention_cutoff.date()}).")

    window_start = (now_real - timedelta(days=BACKFILL_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    seed_day = window_start - timedelta(days=1)
    days = [seed_day + timedelta(days=i) for i in range(0, BACKFILL_DAYS + 1)]

    cards = list(db["Cards"].find({}, {"id": 1, "setId": 1, "type": 1}))
    cards_by_id = {c["id"]: c for c in cards if isinstance(c.get("id"), str)}
    all_ids = list(cards_by_id.keys())
    print(f"Carte totali: {len(all_ids):,}")

    fetch_since = now_real - timedelta(days=LOOKBACK_DAYS)
    print(f"Carico Prices dal {fetch_since.date()} (lookback {LOOKBACK_DAYS}gg) a blocchi di {CHUNK_SIZE}...")

    change_ops: List[InsertOne] = []
    examples: List[str] = []
    processed = 0
    n_docs_total = 0
    n_chunks = (len(all_ids) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx in range(0, len(all_ids), CHUNK_SIZE):
        chunk_ids = all_ids[chunk_idx : chunk_idx + CHUNK_SIZE]
        per_item = fetch_prices_chunk(db, chunk_ids, fetch_since)
        n_docs_total += sum(len(v) for v in per_item.values())

        for item_id, prices_desc in per_item.items():
            card = cards_by_id.get(item_id, {})
            process_card(item_id, card, prices_desc, days, seed_day, change_ops, examples)
            processed += 1

        chunk_num = chunk_idx // CHUNK_SIZE + 1
        print(
            f"  ...blocco {chunk_num:,}/{n_chunks:,}: {processed:,}/{len(all_ids):,} carte, "
            f"{n_docs_total:,} documenti, {len(change_ops):,} transizioni finora",
            flush=True,
        )

    print()
    print(f"Carte elaborate: {processed:,}")
    print(f"Transizioni rilevate nella finestra {window_start.date()} -> {now_real.date()}: {len(change_ops):,}")
    print()
    print("Esempi:")
    for ex in examples:
        print(" ", ex)

    coll_changes = sales_db[STAMP_CHANGES_COLLECTION]
    existing_in_window = coll_changes.count_documents({"changedAt": {"$gte": window_start}})
    print()
    print(f"Record CardsStampChanges esistenti nella stessa finestra (DB {MONGODB_SALES_DB}): {existing_in_window:,}")

    if dry_run:
        print()
        print("DRY-RUN: nessuna scrittura effettuata. Rilancia con --commit per applicare.")
        return

    del_result = coll_changes.delete_many({"changedAt": {"$gte": window_start}})
    print(f"Eliminati {del_result.deleted_count:,} vecchi record.")

    if change_ops:
        for i in range(0, len(change_ops), 2000):
            batch = change_ops[i : i + 2000]
            coll_changes.bulk_write(batch, ordered=False)
        print(f"Inseriti {len(change_ops):,} nuovi record.")
    else:
        print("Nessuna transizione da inserire.")


if __name__ == "__main__":
    main()
