from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from pymongo import MongoClient

from src.core.config import ROME, load_settings
from src.core.emailer import send_email
from src.core.logging_setup import configure_logger
from src.core.utils import get_fx_eur_usd
from src.metrics.getMarketData import (
    GRADED_MAX_KEY,
    GRADED_REFERENCE_FIELDS,
    _cardmarket_has_no_sales,
    _compute_price_redline,
)
from src.ingest.main import DISABLE_GRADING, build_ebay_siblings
from src.core.prices_graded import (
    PRICES_GRADED_COLLECTION,
    ensure_prices_graded_collection,
    latest_cards_metrics_psa10,
    latest_graded_max,
    latest_pricecharting,
    split_graded_row,
)

logger = configure_logger()

BAD_URI_TAG_PREFIX = "pricecharting_uri_error"
FX_API_URL = os.getenv("FX_API_URL")
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "10000"))
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "10000"))
MONGO_SOCKET_TIMEOUT_MS = int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "30000"))
MONGO_FIND_MAX_TIME_MS = int(os.getenv("MONGO_FIND_MAX_TIME_MS", "120000"))


def now_rome() -> datetime:
    return datetime.now(ROME)


def execution_fingerprint(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M")


def load_provider_module(module_name: Optional[str]):
    if not module_name:
        from src.providers.mock import PROVIDERS

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
    h = hash(str(mongo_id))
    return (h % shard_total) == shard_idx


def _rome_day_bounds(reference_dt: datetime) -> tuple[datetime, datetime]:
    start_of_day = reference_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)
    return start_of_day, end_of_day


def _delete_latest_price_for_today(coll_prices, coll_prices_graded, item_id: str, reference_dt: datetime) -> bool:
    start_of_day, end_of_day = _rome_day_bounds(reference_dt)
    today = {
        "itemId": item_id,
        "createdAt": {"$gte": start_of_day, "$lt": end_of_day},
    }
    latest_doc = coll_prices.find_one(
        today,
        sort=[("createdAt", -1), ("_id", -1)],
        projection={"_id": 1},
    )
    if not latest_doc:
        return False

    res = coll_prices.delete_one({"_id": latest_doc["_id"]})
    # la riga PriceCharting collegata (stesso _id) va via con quella di Prices
    coll_prices_graded.delete_many({**today, "_id": latest_doc["_id"]})
    return res.deleted_count == 1


def _append_secondary_alert(
    secondary_alerts: List[Dict[str, Any]],
    updates_map: Dict[str, Any],
    item_id: str,
    doc: Dict[str, Any],
    current_uri: Optional[str],
) -> None:
    alert_payload = updates_map.get("__secondary_alert__")
    if isinstance(alert_payload, dict):
        secondary_alerts.append(
            {
                "itemId": item_id,
                "cardId": str(doc.get("_id")),
                "name": doc.get("name") or "",
                "localId": doc.get("localId") or "",
                "currentUri": current_uri,
                **alert_payload,
            }
        )


def main() -> int:
    if DISABLE_GRADING:
        logger.warning("Grading is DISABLED (DISABLE_GRADING=True in src/ingest/main.py): retry skipped.")
        return 0

    settings = load_settings()
    args = parse_args()
    start_time = time.time()
    start_dt = datetime.now()
    run_dt_rome = now_rome()
    run_fingerprint = execution_fingerprint(run_dt_rome)

    logger.info("=== Start Retry PriceCharting URI Errors Ingestor ===")
    logger.info("Execution fingerprint: %s", run_fingerprint)

    item_id_field = os.getenv("ITEM_ID_FIELD", "id") or "id"
    primary_id_field = os.getenv("PRIMARY_ID_FIELD")
    external_uri_field = os.getenv("EXTERNAL_URI_FIELD")
    external_id_field = os.getenv("EXTERNAL_ID_FIELD")
    cm_id_field = os.getenv("CM_ID_FIELD")
    disable_sharding = os.getenv("DISABLE_SHARDING", "false").lower() == "true"

    if not external_uri_field:
        raise RuntimeError("EXTERNAL_URI_FIELD must be configured for retry_pricecharting_uri_errors")

    logger.info(
        "Field map loaded | item: %s | primary: %s | externalUri: %s | externalId: %s | cmId: %s | sharding: %s",
        item_id_field,
        "set" if primary_id_field else "unset",
        external_uri_field,
        "set" if external_id_field else "unset",
        "set" if cm_id_field else "unset",
        "OFF" if disable_sharding else f"{args.shard_index}/{args.shard_total}",
    )

    logger.info("Connecting to MongoDB...")
    client = MongoClient(
        settings.mongodb_uri,
        tz_aware=True,
        serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
        connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
        socketTimeoutMS=MONGO_SOCKET_TIMEOUT_MS,
    )
    client.admin.command("ping")
    logger.info("MongoDB connection OK")

    db = client[settings.mongodb_db]
    # CardsMetrics (PSA 10) per _cardmarket_low_as_price; senza MONGODB_SALES_* e' il db principale
    sales_db = MongoClient(settings.mongodb_sales_uri, tz_aware=True)[settings.mongodb_sales_db]
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]
    # i campi PriceCharting vanno in PricesGraded, collegati alla riga di Prices dallo stesso _id
    ensure_prices_graded_collection(db)
    coll_prices_graded = db[PRICES_GRADED_COLLECTION]

    logger.info("Loading providers module: %s", settings.providers_module or "src.providers.mock")
    providers = load_provider_module(settings.providers_module)
    primary = next((p for p in providers if getattr(p, "name", "") == "primary"), None)
    secondary = next((p for p in providers if getattr(p, "name", "") == "secondary"), None)
    third = next((p for p in providers if getattr(p, "name", "") == "third"), None)
    cardtrader = next((p for p in providers if getattr(p, "name", "") == "cardtrader"), None)
    ebay = next((p for p in providers if getattr(p, "name", "") == "ebay"), None)
    logger.info(
        "Providers ready | primary=%s secondary=%s third=%s cardtrader=%s ebay=%s",
        "yes" if primary else "no",
        "yes" if secondary else "no",
        "yes" if third else "no",
        "yes" if cardtrader else "no",
        "yes" if ebay else "no",
    )

    projection = {
        "_id": 1,
        item_id_field: 1,
        "variant": 1,
        "variants": 1,
        "name": 1,
        "localId": 1,
        "type": 1,
        "setId": 1,
        "setName": 1,
        "yuyuteiId": 1,
        "yuyuteiLink": 1,
        "cardTraderId": 1,
        "language": 1,
        "releaseDate": 1,
    }
    for optional_field in (primary_id_field, external_uri_field, external_id_field, cm_id_field):
        if optional_field:
            projection[optional_field] = 1

    logger.info("Resolving EUR/USD rate...")
    if FX_API_URL:
        fx = get_fx_eur_usd(FX_API_URL)
    else:
        fx = float(os.getenv("FX_FIXED_RATE", "1.15"))
    logger.info("EUR/USD in use: %.4f", fx)

    page_size = int(os.getenv("MONGO_PAGE_SIZE", "200"))
    batch_size = int(os.getenv("PRICES_BATCH", "500"))
    sample_limit = int(os.getenv("SAMPLE_LIMIT", "0"))

    rows_batch: List[Dict[str, Any]] = []
    graded_batch: List[Dict[str, Any]] = []
    inserted_graded = 0
    secondary_alerts: List[Dict[str, Any]] = []
    inserted = 0
    deleted = 0
    total = 0
    fetched = 0
    last_id = None
    reached_limit = False

    # Printings of the same card, used by the eBay provider to tell them apart
    ebay_key_fn = getattr(ebay, "sibling_key", None) or (lambda card: card.get("localId") or None)
    siblings_by_local_id = build_ebay_siblings(coll_cards, item_id_field, ebay_key_fn) if ebay else {}

    logger.info(
        "Starting scan for cards where %s starts with '%s'",
        external_uri_field,
        BAD_URI_TAG_PREFIX,
    )

    while True:
        if reached_limit:
            break

        query: Dict[str, Any] = {
            external_uri_field: {"$regex": f"^{BAD_URI_TAG_PREFIX}"},
        }
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cur = (
            coll_cards.find(query, projection, max_time_ms=MONGO_FIND_MAX_TIME_MS)
            .sort([("_id", 1)])
            .limit(page_size if sample_limit == 0 else min(page_size, sample_limit - fetched))
        )
        page = list(cur)
        if not page:
            break

        fetched += len(page)
        last_id = page[-1]["_id"]
        logger.info("Fetched page of %d cards (matched so far=%d)", len(page), fetched)

        for doc in page:
            if sample_limit and total >= sample_limit:
                reached_limit = True
                break

            total += 1
            try:
                if (not disable_sharding) and (not partition_ok(doc["_id"], args.shard_index, args.shard_total)):
                    continue

                item_id = doc.get(item_id_field)
                if not item_id:
                    continue

                if _delete_latest_price_for_today(coll_prices, coll_prices_graded, item_id, run_dt_rome):
                    deleted += 1

                primary_id = doc.get(primary_id_field)
                card_variants = doc.get("variants") if isinstance(doc.get("variants"), list) else []
                card_variant = str(doc.get("variant") or "").strip()
                external_uri = (doc.get(external_uri_field) or "") or None
                external_id = doc.get(external_id_field)
                cm_id = doc.get(cm_id_field)

                dt_now = now_rome()
                row: Dict[str, Any] = {
                    "createdAt": now_rome(),
                    "itemId": item_id,
                    "executionFingerprint": run_fingerprint,
                    "weekNumber": dt_now.isocalendar().week,
                }

                if primary and primary_id:
                    try:
                        price, sellers, listings, tcg_lowest_price, tcg_median_price = primary.fetch_primary_price(primary_id)
                        if price is not None:
                            row["pricePrimary"] = float(price)
                        if sellers is not None:
                            row["sellers"] = int(sellers)
                        if listings is not None:
                            row["listings"] = int(listings)
                        if tcg_lowest_price is not None:
                            row["tcgLowestPrice"] = float(tcg_lowest_price)
                        if tcg_median_price is not None:
                            row["tcgMedianPrice"] = float(tcg_median_price)

                        if len(card_variants) >= 2 and hasattr(primary, "fetch_primary_variant_prices"):
                            variant_prices = primary.fetch_primary_variant_prices(
                                primary_id,
                                card_variants,
                                current_variant=card_variant,
                            ) or {}
                            for field_name, field_value in variant_prices.items():
                                if field_name == "pricePrimary":
                                    row[field_name] = float(field_value)
                            if variant_prices:
                                print(
                                    f"[DEBUG] Primary variants | itemId={item_id} | primaryId={primary_id} | variant={card_variant} | variants={card_variants} | fields={variant_prices}"
                                )
                    except Exception as exc:
                        logger.warning("Primary error itemId=%s id=%s: %s", item_id, primary_id, exc)

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
                        
                        for key, value in (price_details_map or {}).items():
                            if value is not None:
                                print(f"[DEBUG] Card {item_id} - {key} = {value}")
                                row[key] = value

                        if updates_map:
                            updates_clean: Dict[str, Any] = {}
                            external_id_value = None
                            if external_id_field and external_id_field in updates_map and updates_map[external_id_field] is not None:
                                external_id_value = updates_map[external_id_field]
                            elif updates_map.get("externalId") is not None:
                                external_id_value = updates_map["externalId"]
                            elif updates_map.get("priceChartingId") is not None:
                                external_id_value = updates_map["priceChartingId"]

                            if external_id_field and external_id_value is not None:
                                updates_clean[external_id_field] = int(external_id_value)

                            external_uri_value = None
                            if external_uri_field and updates_map.get(external_uri_field):
                                external_uri_value = updates_map[external_uri_field]
                            elif updates_map.get("externalUri"):
                                external_uri_value = updates_map["externalUri"]
                            elif updates_map.get("priceChartingUri"):
                                external_uri_value = updates_map["priceChartingUri"]

                            if external_uri_field and external_uri_value:
                                updates_clean[external_uri_field] = str(external_uri_value).lower()

                            release_date_value = updates_map.get("releaseDate")
                            if release_date_value is not None:
                                updates_clean["releaseDate"] = release_date_value

                            _append_secondary_alert(secondary_alerts, updates_map, item_id, doc, external_uri)

                            if updates_clean:
                                updates_clean["updatedAt"] = datetime.now(timezone.utc)
                                coll_cards.update_one({"_id": doc["_id"]}, {"$set": updates_clean})
                    except Exception as exc:
                        logger.warning("Secondary error itemId=%s: %s", item_id, exc)

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
                    except Exception as exc:
                        logger.warning(
                            "Yuyutei error itemId=%s yuyuteiId=%s: %s",
                            item_id,
                            doc.get("yuyuteiId"),
                            exc,
                        )

                if cardtrader:
                    card_trader_id = doc.get("cardTraderId")
                    language = doc.get("language", "en")
                    if card_trader_id:
                        try:
                            price_cardtrader, ct_listings, ct_sellers = cardtrader.fetch_cardtrader_price(
                                {
                                    "itemId": item_id,
                                    "cardTraderId": card_trader_id,
                                    "language": language,
                                    "variant": card_variant,
                                    "variants": card_variants,
                                    "eur_usd": fx,
                                }
                            )
                            if price_cardtrader is not None:
                                row["priceCardTrader"] = price_cardtrader
                            if ct_listings is not None:
                                row["ctListings"] = ct_listings
                            if ct_sellers is not None:
                                row["ctSellers"] = ct_sellers
                        except Exception as exc:
                            logger.warning(
                                "CardTrader error itemId=%s cardTraderId=%s: %s",
                                item_id,
                                card_trader_id,
                                exc,
                            )

                if ebay:
                    try:
                        ebay_siblings = siblings_by_local_id.get(ebay_key_fn(doc), [])
                        ebay_fields = ebay.fetch_ebay_prices(
                            {
                                "itemId": item_id,
                                "localId": doc.get("localId"),
                                "name": doc.get("name"),
                                "language": doc.get("language", "en"),
                                "variant": card_variant,
                                "variants": card_variants,
                                "type": doc.get("type", ""),
                                "setId": doc.get("setId"),
                                "setName": doc.get("setName"),
                                "linkedName": next(
                                    (s.get("linkedName") for s in ebay_siblings if s.get("itemId") == item_id),
                                    None,
                                ),
                                "siblings": ebay_siblings,
                            }
                        ) or {}
                        for field_name, field_value in ebay_fields.items():
                            if field_value is not None:
                                row[field_name] = field_value
                    except Exception as exc:
                        logger.warning("eBay error itemId=%s: %s", item_id, exc)

                if len(row.keys()) <= 3:
                    continue

                # senza PriceCharting in questa esecuzione si usa l'ultimo tracciato (solo per il calcolo:
                # la riga di PricesGraded resta quella dello scraping)
                redline_row = row
                if "pricePriceCharting" not in row:
                    last_pc = latest_pricecharting(db, item_id)
                    if last_pc is not None:
                        redline_row = {**row, "pricePriceCharting": last_pc}
                # gradati per _cardmarket_low_as_price: servono solo con Cardmarket senza vendite
                if _cardmarket_has_no_sales(row) and not any(row.get(f) for f in GRADED_REFERENCE_FIELDS):
                    graded = [latest_graded_max(db, item_id, GRADED_REFERENCE_FIELDS),
                              latest_cards_metrics_psa10(sales_db, item_id)]
                    graded_max = max((g for g in graded if g), default=None)
                    if graded_max is not None:
                        redline_row = {**redline_row, GRADED_MAX_KEY: graded_max}
                price_redline = _compute_price_redline(redline_row)
                if price_redline is not None:
                    row["priceRedLine"] = price_redline

                graded_row = split_graded_row(row)
                if graded_row:
                    graded_batch.append(graded_row)
                rows_batch.append(row)

                if len(rows_batch) >= batch_size:
                    try:
                        res = coll_prices.insert_many(rows_batch, ordered=False)
                        inserted += len(res.inserted_ids)
                        if graded_batch:
                            res_graded = coll_prices_graded.insert_many(graded_batch, ordered=False)
                            inserted_graded += len(res_graded.inserted_ids)
                        logger.info(
                            "Batch insert: %d docs (tot=%d) | graded tot=%d",
                            len(res.inserted_ids), inserted, inserted_graded,
                        )
                    finally:
                        rows_batch.clear()
                        graded_batch.clear()

            except Exception:
                logger.error("Error on item _id=%s: %s", doc.get("_id"), traceback.format_exc())
                continue

        if sample_limit and fetched >= sample_limit:
            break

    if rows_batch:
        try:
            res = coll_prices.insert_many(rows_batch, ordered=False)
            inserted += len(res.inserted_ids)
            if graded_batch:
                res_graded = coll_prices_graded.insert_many(graded_batch, ordered=False)
                inserted_graded += len(res_graded.inserted_ids)
            logger.info(
                "Final insert: %d docs (tot=%d) | graded tot=%d",
                len(res.inserted_ids), inserted, inserted_graded,
            )
        finally:
            rows_batch.clear()
            graded_batch.clear()

    end_time = time.time()
    end_dt = datetime.now()
    elapsed = end_time - start_time
    minutes = elapsed / 60.0

    summary = f"Inserted: {inserted} / Deleted latest-today: {deleted} / Scanned: {total}"
    logger.info(summary)
    body = (
        f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
        f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
        f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
        f"Inserted: {inserted}\n"
        f"Deleted latest-today: {deleted}\n"
        f"Scanned: {total}"
    )
    send_email("✅ [1/5][WORKFLOW] Prices Ingestor Retry PriceCharting URI Errors", body)

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
            lines.append(f"<b>fallbackUrl:</b> {alert.get('fallbackUrl') or '-'}")
            lines.append(f"<b>priceChartingId:</b> {alert.get('priceChartingId') or '-'}")
            lines.append("<br><hr><br>")
        send_email("🚫 [1/5][WORKFLOW] Retry PriceCharting URL issues detected", "<br>".join(lines))

    logger.info("=== End Retry PriceCharting URI Errors Ingestor ===")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        send_email("🚫 [1/5][WORKFLOW] Prices Retry PriceCharting URI Errors", traceback.format_exc())
        raise
