from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# =========================
# ENV: deve stare PRIMA degli import di src.core.config
# perché config.py legge os.getenv() al momento dell'import
# =========================
load_dotenv(".env.local")
load_dotenv()

from pymongo import MongoClient

from src.core.config import ROME, load_settings
from src.core.emailer import send_email
from src.core.logging_setup import configure_logger
from src.core.utils import TokenBucket, get_fx_eur_usd
from src.metrics.getMarketData import _compute_price_redline
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

def execution_fingerprint(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M")

def load_provider_module(module_name: Optional[str]):
    """
    Carica dinamicamente i provider dal modulo indicato (repo privata o mock pubblico).
    Il modulo deve esportare una lista 'PROVIDERS' di istanze con:
      - name: str
      - fetch_primary_price(item_id) -> (price, sellers, listings)
      - fetch_secondary_breakdown(card_info) -> (details_map, updates_map)
    """
    if not module_name:
        from src.providers.mock import PROVIDERS # fallback mock
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
    start_time = time.time()
    start_dt = datetime.now()
    run_dt_rome = now_rome()
    run_fingerprint = execution_fingerprint(run_dt_rome)

    logger.info("=== Start Ingestor ===")
    logger.info("Execution fingerprint: %s", run_fingerprint)

    # ===== Field mapping (da ENV) =====
    ITEM_ID_FIELD = os.getenv("ITEM_ID_FIELD", "id") or "id"
    PRIMARY_ID_FIELD = os.getenv("PRIMARY_ID_FIELD")
    EXTERNAL_URI_FIELD = os.getenv("EXTERNAL_URI_FIELD")
    EXTERNAL_ID_FIELD = os.getenv("EXTERNAL_ID_FIELD")
    CM_ID_FIELD = os.getenv("CM_ID_FIELD")
    # toggle rapido per test senza partizionamento
    DISABLE_SHARDING = os.getenv("DISABLE_SHARDING", "false").lower() == "true"

    logger.info(
        "Field map loaded | item: %s | primary: %s | externalUri: %s | externalId: %s | cmId: %s | sharding: %s",
        ITEM_ID_FIELD,
        "set" if PRIMARY_ID_FIELD else "unset",
        "set" if EXTERNAL_URI_FIELD else "unset",
        "set" if EXTERNAL_ID_FIELD else "unset",
        "set" if CM_ID_FIELD else "unset",
        "OFF" if DISABLE_SHARDING else f"{args.shard_index}/{args.shard_total}",
    )

    # ===== DB =====
    client = MongoClient(settings.mongodb_uri, tz_aware=True)
    db = client[settings.mongodb_db]
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]
    coll_logs = db.get_collection("Logs")

    # ===== Providers (public/private) =====
    try:
        providers = load_provider_module(settings.providers_module)
    except Exception as e:
        logger.error("Failed to load providers module")
        raise

    # Trova i provider per nome (puoi cambiare i nomi se nel bundle sono diversi)
    primary = next((p for p in providers if getattr(p, "name", "") == "primary"), None)
    secondary = next((p for p in providers if getattr(p, "name", "") == "secondary"), None)
    third = next((p for p in providers if getattr(p, "name", "") == "third"), None)
    if not primary:
        logger.warning("Primary provider not found – proceeding without primary.")
    if not secondary:
        logger.warning("Secondary provider not found – proceeding without secondary.")

    if not third:
        logger.warning("Third provider not found - proceeding without third provider.")

    # ===== Query Cards (proiezione minima) =====
    projection = {
        "_id": 1,
        ITEM_ID_FIELD: 1,
        "name": 1,
        "localId": 1,
        "type": 1,
        "setId": 1,
        "yuyuteiId": 1,
        "yuyuteiLink": 1,
        PRIMARY_ID_FIELD: 1,
        EXTERNAL_URI_FIELD: 1,
        EXTERNAL_ID_FIELD: 1,
        CM_ID_FIELD: 1,
        "releaseDate": 1,
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
    secondary_alerts: List[Dict[str, Any]] = []
    inserted = 0
    total = 0
    fetched = 0
    last_id = None
    reached_limit = False

    while True:
        if reached_limit:
            break

        q = {} # nessun filtro in query
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

                # DEBUG: processa solo questa card
                #if item_id != "RED02XXOP01002PERRA277477":
                #    continue

                primary_id = doc.get(PRIMARY_ID_FIELD)
                external_uri = (doc.get(EXTERNAL_URI_FIELD) or "") or None
                external_id = doc.get(EXTERNAL_ID_FIELD)
                cm_id = doc.get(CM_ID_FIELD)

                # Documento base
                dt_now = now_rome()
                row: Dict[str, Any] = {
                    "createdAt": now_rome(),
                    "itemId": item_id,
                    "executionFingerprint": run_fingerprint,
                    #"currency": "USD",
                    "weekNumber": dt_now.isocalendar().week, 
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

                # ===== Secondary (breakdown grading + cm) =====
                # Passiamo al provider le info utili: uri esterna, cm id, fx ecc.
                if secondary and (external_uri or cm_id or external_id):
                    card_info = {
                        "itemId": item_id,
                        "externalUri": external_uri,
                        "priceChartingUri": external_uri,
                        "priceChartingId": external_id,
                        "cardMarketId": cm_id,
                        "eur_usd": fx,
                        "releaseDate": doc.get("releaseDate"),
                        "type": doc.get("type", ""),
                    }
                    try:
                        price_details_map, updates_map = secondary.fetch_secondary_breakdown(card_info)
                        print(f"[DEBUG] Card {item_id} - price_details_map: {price_details_map}")

                        # --- unisci i dettagli prezzo (PSA/BGS/CGC + price + cm*) ---
                        for k, v in (price_details_map or {}).items():
                            if v is not None:
                                print(f"[DEBUG] Card {item_id} - {k} = {v}")
                                row[k] = v

                        # --- eventuali update soft su Cards ---
                        if updates_map:
                            updates_clean = {}
                            external_id_value = None
                            if EXTERNAL_ID_FIELD and EXTERNAL_ID_FIELD in updates_map and updates_map[EXTERNAL_ID_FIELD] is not None:
                                external_id_value = updates_map[EXTERNAL_ID_FIELD]
                            elif updates_map.get("externalId") is not None:
                                external_id_value = updates_map["externalId"]
                            elif updates_map.get("priceChartingId") is not None:
                                external_id_value = updates_map["priceChartingId"]

                            if EXTERNAL_ID_FIELD and external_id_value is not None:
                                updates_clean[EXTERNAL_ID_FIELD] = int(external_id_value)

                            external_uri_value = None
                            if EXTERNAL_URI_FIELD and updates_map.get(EXTERNAL_URI_FIELD):
                                external_uri_value = updates_map[EXTERNAL_URI_FIELD]
                            elif updates_map.get("externalUri"):
                                external_uri_value = updates_map["externalUri"]
                            elif updates_map.get("priceChartingUri"):
                                external_uri_value = updates_map["priceChartingUri"]

                            if EXTERNAL_URI_FIELD and external_uri_value:
                                updates_clean[EXTERNAL_URI_FIELD] = str(external_uri_value).lower()

                            release_date_value = updates_map.get("releaseDate")
                            if release_date_value is not None:
                                updates_clean["releaseDate"] = release_date_value

                            alert_payload = updates_map.get("__secondary_alert__")
                            if isinstance(alert_payload, dict):
                                secondary_alerts.append(
                                    {
                                        "itemId": item_id,
                                        "cardId": str(doc.get("_id")),
                                        "name": doc.get("name") or "",
                                        "localId": doc.get("localId") or "",
                                        "currentUri": external_uri,
                                        **alert_payload,
                                    }
                                )

                            if updates_clean:
                                updates_clean["updatedAt"] = datetime.now(timezone.utc)
                                coll_cards.update_one({"_id": doc["_id"]}, {"$set": updates_clean})

                    except Exception as e:
                        logger.warning("Secondary error itemId=%s: %s", item_id, e)

                # Se non c’è nulla da inserire oltre a createdAt/itemId, salta
                if third:
                    try:
                        price_yuyutei = third.fetch_yuyutei_price(
                            {
                                "itemId": item_id,
                                "setId": doc.get("setId"),
                                "yuyuteiId": doc.get("yuyuteiId"),
                                "yuyuteiLink": doc.get("yuyuteiLink"),
                            }
                        )
                        if price_yuyutei is not None:
                            row["priceYuyuTei"] = price_yuyutei
                    except Exception as e:
                        logger.warning(
                            "Yuyutei error itemId=%s yuyuteiId=%s: %s",
                            item_id,
                            doc.get("yuyuteiId"),
                            e,
                        )

                if len(row.keys()) <= 3:
                    continue

                price_redline = _compute_price_redline(row)
                if price_redline is not None:
                    row["priceRedLine"] = price_redline

                rows_batch.append(row)

                if len(rows_batch) >= BATCH:
                    try:
                        res = coll_prices.insert_many(rows_batch, ordered=False)
                        inserted += len(res.inserted_ids)
                        logger.info("Batch insert: %d docs (tot=%d)", len(res.inserted_ids), inserted)
                    finally:
                        rows_batch.clear()

            except Exception as e:
                logger.error("Error on item _id=%s: %s", doc.get("_id"), e.__class__.__name__)
                logger.debug("Traceback:\n%s", traceback.format_exc())
                continue

        if SAMPLE_LIMIT and fetched >= SAMPLE_LIMIT:
            break

    if rows_batch:
        try:
            res = coll_prices.insert_many(rows_batch, ordered=False)
            inserted += len(res.inserted_ids)
            logger.info("Final insert: %d docs (tot=%d)", len(res.inserted_ids), inserted)
        finally:
            rows_batch.clear()

    end_time = time.time()
    end_dt = datetime.now()
    elapsed = end_time - start_time
    minutes = elapsed / 60.0

    summary = f"Inserted: {inserted} / Scanned: {total}"
    logger.info(summary)
    body = (
        f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
        f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
        f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
        f"Inserted: {inserted}\n"
        f"Scanned: {total}"
    )
    send_email("✅ [1/5][WORKFLOW] Prices Ingestor", body)

    if secondary_alerts:
        lines = ["<b>PriceCharting URL issues detected:</b><br><br>"]
        for alert in secondary_alerts:
            local_id = alert.get("localId") or ""
            card_link = f"http://localhost:3000/cards?s={local_id}" if local_id else "-"
            lines.append(f"<b>Name:</b> {alert.get('name') or '-'}")
            lines.append(f"<b>localId:</b> {alert.get('localId') or '-'}")
            lines.append(f"<b>Link:</b> <a href=\"{card_link}\">{card_link}</a>" if local_id else "<b>Link:</b> -")
            lines.append(f"<b>itemId:</b> {alert.get('itemId') or '-'}")
            lines.append(f"<b>cardId:</b> {alert.get('cardId') or '-'}")
            lines.append(f"<b>type:</b> {alert.get('type') or '-'}")
            lines.append(f"<b>reason:</b> {alert.get('reason') or '-'}")
            lines.append(f"<b>error:</b> {alert.get('error') or '-'}")
            lines.append(f"<b>currentUri:</b> {alert.get('currentUri') or '-'}")
            lines.append(f"<b>requestedUrl:</b> {alert.get('requestedUrl') or '-'}")
            lines.append(f"<b>finalUrl:</b> {alert.get('finalUrl') or '-'}")
            lines.append("<br><hr><br>")
        send_email("🚫 [1/5][WORKFLOW] PriceCharting URL issues detected", "<br>".join(lines))

    # log su collection Logs
    try:
        coll_logs.insert_one(
            {
                "type": "Prices: Ingestor",
                "description": f"Inserted: {inserted} / Scanned: {total}",
                "createdAt": datetime.now(timezone.utc),
            }
        )
    except Exception as e:
        logger.warning("Unable to write Logs entry: %s", e)

    logger.info("=== End Ingestor ===")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        send_email("🚫 [1/5][WORKFLOW] Prices Ingestor", traceback.format_exc())
        raise
