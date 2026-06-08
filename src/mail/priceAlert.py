import asyncio
import decimal
import os
import random
import re
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

from src.core.notifications import enqueue_notification, ensure_notification_indexes

# =============================================================================
# Environment & constants
# =============================================================================

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

CREDIT_COST = 10
MAX_RANDOM_DELAY_HOURS = 12
MAX_RANDOM_DELAY_MINUTES = MAX_RANDOM_DELAY_HOURS * 60
MIN_DELTA_HOURS = MAX_RANDOM_DELAY_HOURS

PRICE_FIELDS = (
    "priceRedLine",
    "pricePriceCharting",
    "pricePrimary",
    "cmAvg7d",
    "cmPriceAvg",
    "cmPriceTrend",
)

decimal.getcontext().prec = 10


# =============================================================================
# Helpers
# =============================================================================

def D(x) -> decimal.Decimal:
    try:
        return decimal.Decimal(str(x))
    except Exception:
        return decimal.Decimal("0")


def format_money(value: decimal.Decimal) -> str:
    return f"${value:.2f}"


def normalize_dt(dt):
    if dt and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def enqueue_mail(
    db,
    subject: str,
    body: str,
    to: str,
    user_id,
    alert_id: ObjectId | None = None,
    alert_ids: list[ObjectId] | None = None,
    scheduled_at: datetime | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    db.Mail.insert_one({
        "subject": subject,
        "body": body,
        "to": to,
        "status": "queued",
        "createdAt": now,
        "scheduledAt": scheduled_at or now,
        "userId": user_id,
        "lockedAt": None,
        "lockedBy": None,
        "lastError": None,
        "retries": 0,
        "alertId": alert_id,
        "reportType": "priceAlert",
    })


def random_scheduled_at(now_utc: datetime) -> datetime:
    delay_minutes = random.randint(0, MAX_RANDOM_DELAY_MINUTES)
    return now_utc + timedelta(minutes=delay_minutes)


def clean_card_name(card: dict) -> tuple[str, str, str]:
    clean_name = re.sub(r"\s*\[.*?\]\s*", "", (card.get("name") or "")).strip()
    local_id = (card.get("localId") or "").strip()
    card_name = clean_name or f"Card {card.get('id', '')}".strip()
    if local_id:
        card_name += f" #{local_id}"
    return clean_name, local_id, card_name


def build_card_links(card: dict, clean_name: str, local_id: str) -> dict[str, str]:
    encoded_query = quote_plus(f"{clean_name} {local_id}".strip())
    ebay_query = quote_plus(f"{clean_name} {local_id}".strip())
    links = {
        "cardmarket": (
            "https://www.cardmarket.com/it/OnePiece/Products/Search"
            f"?referrer=Yovacca&mode=gallery&searchString={encoded_query}"
        ),
        "ebay": (
            "https://www.ebay.com/sch/i.html?_nkw="
            + ebay_query
            + "&mkcid=1&mkrid=711-53200-19255-0&siteid=0&campid=5339118630"
            + "&customid=redlinecards&toolid=10001&mkevt=1"
        ),
        "cardtrader": "https://www.cardtrader.com/invite/ivory-swamp-389",
    }
    tcg_id = card.get("tcgPlayerId")
    if isinstance(tcg_id, int):
        links["tcgplayer"] = f"https://www.tcgplayer.com/product/{tcg_id}"
    return links


# =============================================================================
# Alert evaluation
# =============================================================================

def build_price_candidates(price_doc: dict) -> list[tuple[str, decimal.Decimal]]:
    candidates: list[tuple[str, decimal.Decimal]] = []

    for field in PRICE_FIELDS:
        value = price_doc.get(field)
        if value is not None:
            candidates.append((field, D(value)))

    if price_doc.get("cmAvg7d") is None and price_doc.get("cmAvg1d") is not None:
        candidates.append(("cmAvg1d", D(price_doc.get("cmAvg1d"))))

    return candidates


def load_latest_prices_by_item_id(db, item_ids: list) -> dict:
    if not item_ids:
        return {}

    rows = db.Prices.aggregate([
        {"$match": {"itemId": {"$in": item_ids}}},
        {"$sort": {"itemId": 1, "createdAt": -1}},
        {
            "$group": {
                "_id": "$itemId",
                "latest": {"$first": "$$ROOT"},
            }
        },
    ])
    return {row["_id"]: row["latest"] for row in rows}


def price_item_id(alert_doc: dict):
    return alert_doc.get("cardId")


def any_price_hits_conditions(
    price_candidates: list[tuple[str, decimal.Decimal]],
    base_price: decimal.Decimal,
    target_price: decimal.Decimal,
    direction: str,
    change_raw,
) -> tuple[bool, bool, tuple[str, decimal.Decimal] | None]:
    hit_target = False
    hit_change = False
    first_hit: tuple[str, decimal.Decimal] | None = None

    direction = (direction or "lte").lower()
    change_p = D(change_raw) if change_raw is not None else None
    base_ok = base_price is not None and base_price > 0

    for label, current in price_candidates:
        if current <= 0:
            continue

        if target_price and direction == "lte" and current <= target_price:
            hit_target = True
            first_hit = first_hit or (label, current)
        elif target_price and direction == "gte" and current >= target_price:
            hit_target = True
            first_hit = first_hit or (label, current)

        if change_p is not None and base_ok:
            delta_pct = (current - base_price) / base_price * 100
            if change_p < 0 and delta_pct <= change_p:
                hit_change = True
                first_hit = first_hit or (label, current)
            elif change_p > 0 and delta_pct >= change_p:
                hit_change = True
                first_hit = first_hit or (label, current)

    return hit_target, hit_change, first_hit


def is_throttled(alert_doc: dict, now_utc: datetime) -> bool:
    if not alert_doc.get("recurrent"):
        return False

    last = normalize_dt(alert_doc.get("lastNotifiedAt"))
    if not last:
        return False

    return (now_utc - last).total_seconds() < MIN_DELTA_HOURS * 3600


def build_triggered_item(alert_doc: dict, now_utc: datetime) -> dict:
    card = alert_doc["cardData"]
    latest_price = alert_doc["latestPriceData"]
    clean_name, local_id, card_name = clean_card_name(card)
    created_at = normalize_dt(alert_doc.get("createdAt"))
    created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S UTC") if created_at else "ERROR"

    candidates = build_price_candidates(latest_price)
    base = D(alert_doc.get("basePrice"))
    target = D(alert_doc.get("priceTarget") or "0")
    direction = (alert_doc.get("direction") or alert_doc.get("targetCondition") or "lte").lower()

    hit_target, hit_change, first_hit = any_price_hits_conditions(
        price_candidates=candidates,
        base_price=base,
        target_price=target,
        direction=direction,
        change_raw=alert_doc.get("priceChange"),
    )

    if not (hit_target or hit_change) or not first_hit:
        return {}

    hit_label, hit_price = first_hit
    return {
        "alert": alert_doc,
        "alertId": alert_doc["_id"],
        "card": card,
        "cardName": card_name,
        "cleanName": clean_name,
        "localId": local_id,
        "createdAtStr": created_at_str,
        "hitLabel": hit_label,
        "hitPrice": hit_price,
        "latestPriceCreatedAt": normalize_dt(latest_price.get("createdAt")),
        "links": build_card_links(card, clean_name, local_id),
    }


def build_single_body(to_email: str, item: dict, now_utc: datetime) -> str:
    links = item["links"]
    tcg_link = links.get("tcgplayer")

    return (
        "Hi,<br>"
        "we have good news for you! <br><br>"
        "One of the cards you have been looking for has reached your price conditions.<br><br>"
        f"<b>{item['cleanName']} #{item['localId']}</b> card is currently at "
        f"<b>{format_money(item['hitPrice'])}</b> ({item['hitLabel']}).<br><br>"
        "Don't miss this opportunity!<br>"
        "<ul>"
        f"<li>Check it out on <a href='{links['cardmarket']}'>Cardmarket</a></li>"
        + (f"<li>Check it out on <a href='{tcg_link}'>TCGPlayer</a></li>" if tcg_link else "")
        + f"<li>Check it out on <a href='{links['ebay']}'>Ebay</a></li>"
        + f"<li>Check it out on <a href='{links['cardtrader']}'>CardTrader</a></li>"
        "</ul>"
        f"<br>This email was sent to {to_email} because on date {item['createdAtStr']} "
        f"you have set up an alert for the card {item['cardName']} on RED LINE "
        "(https://redline.cards/).<br>"
        "If you wish to stop receiving notifications, please log in to your account and update "
        "your mail alert preferences in the <a href='https://redline.cards/account'>settings</a>.<br>"
        "This is a free notification service of the RED LINE website (https://redline.cards/).<br><br>"
        "______<br><br>"
        "<i>This e-mail may contain confidential and/or privileged information.<br>"
        "If you are not the intended recipient or have received this e-mail in error, please notify "
        "the sender immediately and delete this e-mail.<br>"
        "Any unauthorized copying, disclosure, or distribution of the material contained in this "
        "e-mail is strictly prohibited</i>.<br><br>"
        f"### This is an automatically generated message on UTC "
        f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} ###<br><br>"
    )


def build_notification_text(item: dict) -> str:
    return (
        f"{item['cleanName']} #{item['localId']} has reached your price alert! Actual price is "
        f"{format_money(item['hitPrice'])}"
    )


# =============================================================================
# Main
# =============================================================================

async def main() -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGODB_DB]
    ensure_notification_indexes(db)

    alerts = list(db.PricesAlert.aggregate([
        {
            "$match": {
                "$or": [
                    {"recurrent": False, "notified": False},
                    {"recurrent": True},
                ]
            }
        },
        {
            "$lookup": {
                "from": "Cards",
                "localField": "itemId",
                "foreignField": "_id",
                "as": "cardData",
            }
        },
        {"$unwind": "$cardData"},
    ]))

    if not alerts:
        print("[INFO] No active alerts.")
        return

    unique_item_ids = list({price_item_id(alert) for alert in alerts if price_item_id(alert)})
    latest_prices_by_item_id = load_latest_prices_by_item_id(db, unique_item_ids)
    if not latest_prices_by_item_id:
        print("[INFO] No active alerts with available prices.")
        return

    now = datetime.now(timezone.utc)
    skipped_for_credit = 0
    queued_count = 0
    notified_alert_ids: list[ObjectId] = []
    alerts_by_id: dict[ObjectId, dict] = {}

    for alert in alerts:
        if is_throttled(alert, now):
            continue
        if not alert.get("recurrent") and alert.get("notified"):
            continue

        to_email = (alert.get("userEmail") or "").strip()
        user_id = alert.get("userId")
        if not to_email or not user_id:
            continue

        latest_price = latest_prices_by_item_id.get(price_item_id(alert))
        if not latest_price:
            continue
        alert["latestPriceData"] = latest_price

        item = build_triggered_item(alert, now)
        if not item:
            continue

        credit_res = db.Users.update_one(
            {"_id": user_id, "credit": {"$gte": CREDIT_COST}},
            {"$inc": {"credit": -CREDIT_COST, "creditSpent": CREDIT_COST}},
        )
        if credit_res.matched_count == 0:
            skipped_for_credit += 1
            print(f"[SKIP] Not enough credit for user {user_id}.")
            continue

        scheduled_at = random_scheduled_at(now)
        subject = f"[RED LINE] 🔔 {item['cleanName']} #{item['localId']} has a new price!"
        body = build_single_body(to_email, item, now)
        enqueue_mail(db, subject, body, to_email, user_id, item["alertId"], scheduled_at=scheduled_at)
        enqueue_notification(db, user_id, build_notification_text(item))
        queued_count += 1
        notified_alert_ids.append(item["alertId"])
        alerts_by_id[item["alertId"]] = item["alert"]
        print(
            f"[MAIL] Queued alert -> {to_email} "
            f"(scheduledAt={scheduled_at.strftime('%Y-%m-%d %H:%M:%S UTC')})"
        )

    for item_ids in (notified_alert_ids[i:i + 100] for i in range(0, len(notified_alert_ids), 100)):
        if not item_ids:
            continue

        non_recurrent_ids = [
            alert_id
            for alert_id in item_ids
            if not alerts_by_id[alert_id].get("recurrent")
        ]
        recurrent_ids = [alert_id for alert_id in item_ids if alert_id not in non_recurrent_ids]

        if recurrent_ids:
            db.PricesAlert.update_many(
                {"_id": {"$in": recurrent_ids}},
                {"$set": {"lastNotifiedAt": now}},
            )
        if non_recurrent_ids:
            db.PricesAlert.update_many(
                {"_id": {"$in": non_recurrent_ids}},
                {"$set": {"lastNotifiedAt": now, "notified": True}},
            )

    print(
        f"[END] Script completed. Queued emails: {queued_count}. "
        f"Triggered alerts: {len(notified_alert_ids)}. Credit skips: {skipped_for_credit}."
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
