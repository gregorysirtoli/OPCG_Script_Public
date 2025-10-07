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
    ITEM_ID_FIELD = os.getenv("ITEM_ID_FIELD", "id")
    PRIMARY_ID_FIELD = os.getenv("PRIMARY_ID_FIELD", "tcgPlayerId")
    EXTERNAL_URI_FIELD = os.getenv("EXTERNAL_URI_FIELD", "priceChartingUri")
    # toggle rapido per test senza partizionamento
    DISABLE_SHARDING = os.getenv("DISABLE_SHARDING", "false").lower() == "true"

    logger.info(
        "Field map -> item: %s | primary: %s | external: %s | sharding: %s",
        ITEM_ID_FIELD,
        PRIMARY_ID_FIELD,
        EXTERNAL_URI_FIELD,
        "OFF" if DISABLE_SHARDING else f"{args.shard_index}/{args.shard_total}",
    )

    # ===== DB =====
    client = MongoClient(settings.mongodb_uri, tz_aware=True)
    db = client[settings.mongodb_db]
    coll_cards = db["Cards"]
    coll_prices = db["Prices_TEST"]
    coll_logs = db.get_collection("Logs")  # crea al primo insert, evita errori

    # ===== Providers (public/private) =====
    providers = load_provider_module(settings.providers_module)
    logger.info("Loaded providers: %s", ", ".join(p.name for p in providers))

    # ===== FX (se serve in futuro) =====
    fx = get_fx_eur_usd(settings.fx_api_url)  # disponibile per eventuali conversioni
    logger.debug("FX EUR->USD: %.6f", fx)

    total = 0
    inserted = 0
    skipped = 0
    rows_batch: List[Dict[str, Any]] = []

    # proiezione dinamica in base ai campi mappati
    projection = {"_id": 1, ITEM_ID_FIELD: 1, PRIMARY_ID_FIELD: 1, EXTERNAL_URI_FIELD: 1}

    fetched = 0
    last_id = None

    def flush_batch(batch: List[Dict[str, Any]]):
        nonlocal inserted
        if not batch:
            return
        try:
            res = coll_prices.insert_many(batch, ordered=False)
            inserted += len(res.inserted_ids)
            logger.info("Inserted %d docs (total=%d)", len(res.inserted_ids), inserted)
        except Exception as e:
            logger.warning("Bulk insert failed (%s). Fallback to single inserts.", e)
            ok = 0
            for r in batch:
                try:
                    coll_prices.insert_one(r)
                    ok += 1
                except Exception as ee:
                    logger.error("insert_one failed itemId=%s: %s", r.get("itemId"), ee)
            inserted += ok
        finally:
            batch.clear()

    # ===== Paginazione su Cards =====
    while True:
        q = {}
        if last_id is not None:
            q["_id"] = {"$gt": last_id}

        page_size = (
            settings.page_size
            if settings.sample_limit == 0
            else min(settings.page_size, max(0, settings.sample_limit - fetched))
        )
        if page_size <= 0:
            break

        cur = coll_cards.find(q, projection).sort([("_id", 1)]).limit(page_size)
        page = list(cur)
        if not page:
            break

        fetched += len(page)
        last_id = page[-1]["_id"]

        for doc in page:
            if settings.sample_limit and total >= settings.sample_limit:
                break

            total += 1

            # Sharding
            if not DISABLE_SHARDING and not partition_ok(doc["_id"], args.shard_index, args.shard_total):
                skipped += 1
                continue

            try:
                item_id = doc.get(ITEM_ID_FIELD)
                primary_id = doc.get(PRIMARY_ID_FIELD)
                external_uri_raw = doc.get(EXTERNAL_URI_FIELD)
                external_uri = (external_uri_raw or "").strip() or None

                if not (primary_id or external_uri):
                    skipped += 1
                    logger.debug(
                        "SKIP %s (no %s and no %s)",
                        item_id,
                        PRIMARY_ID_FIELD,
                        EXTERNAL_URI_FIELD,
                    )
                    continue

                # row base
                row: Dict[str, Any] = {
                    "createdAt": now_rome(),
                    "itemId": item_id,
                    "currency": "USD",
                }

                # ===== Fonte primaria (tipicamente JSON) =====
                price = sellers = listings = None
                for p in providers:
                    try:
                        _bucket.acquire()
                        pr, se, li = p.fetch_primary_price(primary_id)
                        price = price or pr
                        sellers = sellers or se
                        listings = listings or li
                    except Exception as e:
                        logger.debug("provider %s primary failed: %s", p.name, e)

                row["pricePrimary"] = price
                row["sellers"] = sellers
                row["listings"] = listings

                # ===== Fonte secondaria (tipicamente HTML, breakdown dettagli) =====
                details: Dict[str, Any] = {}
                updates: Dict[str, Any] = {}
                if external_uri:
                    for p in providers:
                        try:
                            _bucket.acquire()
                            d, u = p.fetch_secondary_breakdown(
                                {"id": item_id, "externalUri": external_uri}
                            )
                            if d:
                                details.update({k: v for k, v in d.items() if v is not None})
                            if u:
                                updates.update({k: v for k, v in u.items() if v is not None})
                        except Exception as e:
                            logger.debug("provider %s secondary failed: %s", p.name, e)

                row.update(details)

                # eventuale update parziale su Cards (es. externalId, externalUri normalizzata, updatedAt)
                if updates:
                    updates["updatedAt"] = datetime.now(timezone.utc)
                    coll_cards.update_one({ITEM_ID_FIELD: item_id}, {"$set": updates})

                rows_batch.append(row)

                if len(rows_batch) >= settings.batch_size:
                    flush_batch(rows_batch)

            except Exception as e:
                logger.error(
                    "Error on item %s: %s\n%s",
                    doc.get(ITEM_ID_FIELD),
                    e,
                    traceback.format_exc(),
                )
                continue

        if settings.sample_limit and fetched >= settings.sample_limit:
            break

    # flush finale
    flush_batch(rows_batch)

    summary = (
        f"=== Report Ingestor ===\n"
        f"Tot: {total}\n"
        f"Inserted: {inserted}\n"
        f"Skipped: {skipped}\n"
    )
    logger.info(summary)
    send_email(os.getenv("MAIL_SUBJECT", "[Ingestor] Report"), summary)

    # log su collection Logs
    try:
        coll_logs.insert_one(
            {
                "type": "Ingestor",
                "description": f"Inserted: {inserted}, Skipped: {skipped}",
                "createdAt": datetime.now(timezone.utc),
            }
        )
    except Exception as e:
        logger.warning("Unable to write Logs entry: %s", e)

    logger.info("=== End Ingestor ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
