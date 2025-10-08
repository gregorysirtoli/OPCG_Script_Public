from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient

from src.core.config import ROME, load_settings
from src.core.emailer import send_email
from src.core.logging_setup import configure_logger
from src.core.utils import TokenBucket, eur_to_usd, get_fx_eur_usd

# =========================
# ENV & Logger
# =========================
load_dotenv(".env.local")
load_dotenv()
logger = configure_logger()

# =========================
# Rate limiter (soft)
# =========================
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "1.2"))
FX_API_URL = os.getenv("FX_API_URL")
BURST = int(os.getenv("BURST", "10"))
_bucket = TokenBucket(RATE_PER_SEC, BURST)


def now_rome() -> datetime:
    return datetime.now(ROME)


def load_provider_module(module_name: Optional[str]):
    """
    Carica dinamicamente i provider dal modulo indicato (repo privata o mock pubblico).
    Il modulo deve esportare una lista 'PROVIDERS' di istanze con:
      - name: str
      - fetch_primary_price(item_id) -> (price, sellers, listings)
      - fetch_secondary_breakdown(card_info) -> (details_map, updates_map)
    """
    if not module_name:
        from src.providers.mock import PROVIDERS  # fallback mock
        return PROVIDERS
    import importlib

    m = importlib.import_module(module_name)
    return getattr(m, "PROVIDERS")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--shard-total", type=int, default=1)
    return ap.parse_args()


def partition_ok(mongo_id: Any, shard_idx: int, shard_total: int) -> bool:
    # partizionamento semplice con hash dello _id in stringa
    h = hash(str(mongo_id))
    return (h % shard_total) == shard_idx


def main() -> int:
    settings = load_settings()
    args = parse_args()

    logger.info("=== Start Ingestor ===")

    # ===== Field mapping (override da ENV se necessario) =====
    ITEM_ID_FIELD = os.getenv("ITEM_ID_FIELD", "id") or "id"
    PRIMARY_ID_FIELD = os.getenv("PRIMARY_ID_FIELD")
    EXTERNAL_URI_FIELD = os.getenv("EXTERNAL_URI_FIELD")
    EXTERNAL_ID_FIELD = os.getenv("EXTERNAL_ID_FIELD")
    CM_ID_FIELD = os.getenv("CM_ID_FIELD")
    # toggle rapido per test senza partizionamento
    DISABLE_SHARDING = os.getenv("DISABLE_SHARDING", "false").lower() == "true"

    logger.info(
        "Field map -> item: %s | primary: %s | external: %s | extId: %s | cm: %s | sharding: %s",
        ITEM_ID_FIELD, PRIMARY_ID_FIELD, EXTERNAL_URI_FIELD, EXTERNAL_ID_FIELD, CM_ID_FIELD,
        "OFF" if DISABLE_SHARDING else f"{args.shard_index}/{args.shard_total}",
    )

    # ===== DB =====
    client = MongoClient(settings.mongodb_uri, tz_aware=True)
    db = client[settings.mongodb_db]
    coll_cards = db["Cards"]
    coll_prices = db["Prices_TEST"]
    coll_logs = db.get_collection("Logs")  # crea al primo insert, evita errori

    # ===== Providers (public/private) =====
    # ===== Provider bundle (public/private) =====
    try:
        providers = load_provider_module(settings.providers_module)
    except Exception as e:
        logger.error("Failed to load providers module '%s': %s", settings.providers_module, e)
        raise

    # Trova i provider per nome (puoi cambiare i nomi se nel bundle sono diversi)
    primary = next((p for p in providers if getattr(p, "name", "") == "primary"), None)
    secondary = next((p for p in providers if getattr(p, "name", "") == "secondary"), None)
    if not primary:
        logger.warning("Primary provider not found – proceeding without primary.")
    if not secondary:
        logger.warning("Secondary provider not found – proceeding without secondary.")

    # ===== Query Cards (proiezione minima) =====
    projection = {
        "_id": 1,
        ITEM_ID_FIELD: 1,
        PRIMARY_ID_FIELD: 1,
        EXTERNAL_URI_FIELD: 1,
        CM_ID_FIELD: 1,
    }

    shard_idx = getattr(args, "shard_index", 0)
    shard_total = getattr(args, "shard_total", 1)
    logger.info(
        "Field map -> item: %s | primary: %s | external: %s | cm: %s | sharding: %s/%s",
        ITEM_ID_FIELD, PRIMARY_ID_FIELD, EXTERNAL_URI_FIELD, CM_ID_FIELD, shard_idx, shard_total
    )

    # ===== FX per cambio EUR/USD =====
    if FX_API_URL:
        fx = get_fx_eur_usd(FX_API_URL)
    else:
        fx = float(os.getenv("FX_FIXED_RATE", "1.15"))

    # ===== Loop sui documenti Cards =====
    PAGE_SIZE = int(os.getenv("MONGO_PAGE_SIZE", "200"))
    BATCH = int(os.getenv("PRICES_BATCH", "500"))
    SAMPLE_LIMIT = int(os.getenv("SAMPLE_LIMIT", "0"))
    DISABLE_SHARDING = os.getenv("DISABLE_SHARDING", "false").lower() == "true"

    rows_batch: List[Dict[str, Any]] = []
    inserted = 0
    total = 0
    fetched = 0
    last_id = None
    reached_limit = False

    while True:
        if reached_limit:
            break

        #q = {} # nessun filtro
        q = {"setId": "OP01"} # filtra per setId
        if last_id is not None:
            q["_id"] = {"$gt": last_id}

        cur = (
            coll_cards.find(q, projection)
            .sort([("_id", 1)])
            .limit(PAGE_SIZE if SAMPLE_LIMIT == 0 else min(PAGE_SIZE, SAMPLE_LIMIT - fetched))
        )
        page = list(cur)
        if not page:
            break

        fetched += len(page)
        last_id = page[-1]["_id"]

        for doc in page:
            if SAMPLE_LIMIT and total >= SAMPLE_LIMIT:
                reached_limit = True
                break

            total += 1
            try:
                if (not DISABLE_SHARDING) and (not partition_ok(doc["_id"], shard_idx, shard_total)):
                    continue

                item_id = doc.get(ITEM_ID_FIELD)
                if not item_id:
                    continue

                primary_id = doc.get(PRIMARY_ID_FIELD)
                external_uri = (doc.get(EXTERNAL_URI_FIELD) or "") or None
                cm_id = doc.get(CM_ID_FIELD)

                # Documento base
                row: Dict[str, Any] = {
                    "createdAt": now_rome(),
                    "itemId": item_id,
                    "currency": "USD",
                }

                # ===== Primary =====
                if primary and primary_id:
                    try:
                        price, sellers, listings = primary.fetch_primary_price(primary_id)
                        if price is not None:
                            row["pricePrimary"] = float(price)
                        if sellers is not None:
                            row["sellers"] = int(sellers)
                        if listings is not None:
                            row["listings"] = int(listings)
                    except Exception as e:
                        logger.warning("Primary error itemId=%s id=%s: %s", item_id, primary_id, e)

                # ===== Secondary (breakdown grading + cardmarket) =====
                # Passiamo al provider le info utili: uri esterna, cm id, fx ecc.
                if secondary and (external_uri or cm_id):
                    card_info = {
                        "itemId": item_id,
                        "externalUri": external_uri,
                        "priceChartingUri": external_uri,
                        "cardMarketId": cm_id,
                        "eur_usd": fx,
                    }
                    try:
                        price_details_map, updates_map = secondary.fetch_secondary_breakdown(card_info)

                        # --- unisci i dettagli prezzo (PSA/BGS/CGC + price + cm*) ---
                        # Attesi (facoltativi):
                        #   priceUngraded, priceGrade7, priceGrade8, priceGrade9, priceGrade95,
                        #   psa10, sgc10, cgc10, cgc10pristine, bsg10, bsg10black, price,
                        #   cmPriceTrend, cmAvg30d, cmAvg7d, cmAvg1d, cmPriceAvg, cmPriceLow, cmFrom, cmAvailableItems
                        for k, v in (price_details_map or {}).items():
                            if v is not None:
                                row[k] = v

                        # --- eventuali update soft su Cards ---
                        if updates_map:
                            updates_clean = {}
                            if EXTERNAL_ID_FIELD in updates_map and updates_map[EXTERNAL_ID_FIELD] is not None:
                                updates_clean[EXTERNAL_ID_FIELD] = int(updates_map[EXTERNAL_ID_FIELD])
                            if EXTERNAL_URI_FIELD in updates_map and updates_map[EXTERNAL_URI_FIELD]:
                                # normalizza a lower se vuoi
                                updates_clean[EXTERNAL_URI_FIELD] = str(updates_map[EXTERNAL_URI_FIELD]).lower()
                            if updates_clean:
                                updates_clean["updatedAt"] = datetime.utcnow()
                                coll_cards.update_one({"_id": doc["_id"]}, {"$set": updates_clean})

                    except Exception as e:
                        logger.warning("Secondary error itemId=%s: %s", item_id, e)

                # Se non c’è nulla da inserire oltre a createdAt/itemId, salta
                if len(row.keys()) <= 3:
                    continue

                rows_batch.append(row)

                if len(rows_batch) >= BATCH:
                    try:
                        res = coll_prices.insert_many(rows_batch, ordered=False)
                        inserted += len(res.inserted_ids)
                        logger.info("Mongo batch insert: %d docs (tot=%d)", len(res.inserted_ids), inserted)
                    finally:
                        rows_batch.clear()

            except Exception as e:
                logger.error("Error on item _id=%s: %s\n%s", doc.get("_id"), e, traceback.format_exc())
                continue

        if SAMPLE_LIMIT and fetched >= SAMPLE_LIMIT:
            break

    if rows_batch:
        try:
            res = coll_prices.insert_many(rows_batch, ordered=False)
            inserted += len(res.inserted_ids)
            logger.info("Mongo final insert: %d docs (tot=%d)", len(res.inserted_ids), inserted)
        finally:
            rows_batch.clear()

    summary = f"Inserted: {inserted} / Scanned: {total}"
    logger.info(summary)
    send_email(os.getenv("MAIL_SUBJECT", "[Ingestor] Report"), summary)

    # log su collection Logs
    try:
        coll_logs.insert_one(
            {
                "type": "Ingestor",
                "description": f"Inserted: {inserted} / Scanned: {total}",
                "createdAt": datetime.now(timezone.utc),
            }
        )
    except Exception as e:
        logger.warning("Unable to write Logs entry: %s", e)

    logger.info("=== End Ingestor ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
