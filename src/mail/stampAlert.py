import os
import re
import traceback
from datetime import datetime, timezone

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
MONGODB_ACCOUNT_URI = os.environ["MONGODB_ACCOUNT_URI"]
MONGODB_ACCOUNT_DB = os.environ["MONGODB_ACCOUNT_DB"]

CARD_LINK_BASE = "http://redline.cards/cards"


# =============================================================================
# Helpers
# =============================================================================

def clean_card_name(card: dict) -> str:
    clean_name = re.sub(r"\s*\[.*?\]\s*", "", (card.get("name") or "")).strip()
    return clean_name or f"Card {card.get('id', '')}".strip()


def format_price(card: dict) -> str:
    price = (card.get("marketData") or {}).get("price")
    if price is None:
        return "N/A"
    try:
        return f"US${float(price):.2f}"
    except (TypeError, ValueError):
        return "N/A"


def card_link(item_id) -> str:
    return f"{CARD_LINK_BASE}/{item_id}"


def unsubscribe_link(user_id, alert_id, item_id) -> str:
    return f"https://redline.cards/api/alerts/stamp/unsubscribe?u={user_id}&_id={alert_id}&i={item_id}"


def normalize_dt(dt):
    if dt and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def enqueue_mail(account_db, subject: str, body: str, to: str, user_id, alert_id=None) -> None:
    now = datetime.now(timezone.utc)
    account_db.Mail.insert_one({
        "subject": subject,
        "body": body,
        "to": to,
        "status": "queued",
        "createdAt": now,
        "scheduledAt": now,
        "userId": user_id,
        "lockedAt": None,
        "lockedBy": None,
        "lastError": None,
        "retries": 0,
        "alertId": alert_id,
        "reportType": "stampAlert",
    })


def build_body(
    to_email: str,
    card_name: str,
    set_id: str,
    local_id: str,
    price_str: str,
    old_stamp: str,
    new_stamp: str,
    link: str,
    created_at_str: str,
    now_utc: datetime,
    unsubscribe_url: str,
) -> str:
    return (
        "Hi,<br>we have good news for you! <br><br>"
        "One of your cards changed verdict in this morning's run. Worth a look!<br><br>"
        f"<b>{card_name}</b><br>"
        f"{set_id} &middot; {local_id} &middot; {price_str}<br>"
        f"{old_stamp} &rarr; {new_stamp}<br><br>"
        f"Here's the link <a href='{link}'>{link}</a><br>"
        "<br>Price alerts and stamps are observations from our market data, not financial advice.<br>"
        f"This email was sent to {to_email} because on date {created_at_str} "
        f"you have set up an alert for the card {card_name} on Red Line "
        "(https://redline.cards/).<br>"
        "If you wish to stop receiving alerts and notifications, you can "
        f"<a href='{unsubscribe_url}'>unsubscribe</a> any time.<br>"
        "This is a free notification service of the Red Line website (https://redline.cards/).<br><br>"
        "______<br><br>"
        "<i>This e-mail may contain confidential and/or privileged information.<br>"
        "If you are not the intended recipient or have received this e-mail in error, please notify "
        "the sender immediately and delete this e-mail.<br>"
        "Any unauthorized copying, disclosure, or distribution of the material contained in this "
        "e-mail is strictly prohibited</i>.<br><br>"
        f"### This is an automatically generated message on UTC "
        f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} ###<br><br>"
    )


def build_notification_text(card_name: str, local_id: str, old_stamp: str, new_stamp: str) -> str:
    return f"{card_name} #{local_id} changed stamp: {old_stamp} -> {new_stamp}"


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    market_client = MongoClient(MONGO_URI)
    market_db = market_client[MONGODB_DB]
    account_client = MongoClient(MONGODB_ACCOUNT_URI)
    account_db = account_client[MONGODB_ACCOUNT_DB]
    ensure_notification_indexes(account_db)

    alerts = list(market_db.StampAlert.find({}))
    if not alerts:
        print("[INFO] No stamp alerts to check.")
        return

    card_ids = list({a["cardId"] for a in alerts if a.get("cardId")})
    item_ids = list({a["itemId"] for a in alerts if a.get("itemId")})

    stamps_by_card_id = {
        s["itemId"]: s
        for s in market_db.CardsStamps.find({"itemId": {"$in": card_ids}})
    }
    cards_by_item_id = {
        c["_id"]: c
        for c in market_db.Cards.find({"_id": {"$in": item_ids}})
    }

    now = datetime.now(timezone.utc)
    queued = 0

    for alert in alerts:
        card_id = alert.get("cardId")
        item_id = alert.get("itemId")
        to_email = (alert.get("userEmail") or "").strip()
        user_id = alert.get("userId")
        if not card_id or not item_id or not to_email or not user_id:
            continue

        current_doc = stamps_by_card_id.get(card_id)
        if not current_doc:
            continue
        current_stamp = current_doc.get("stamp") or {}
        current_key = current_stamp.get("key")
        current_label = current_stamp.get("label")
        current_call_date = current_stamp.get("callDate")

        old_key = alert.get("stampKey")
        if not current_key or current_key == old_key:
            continue  # nessun cambiamento di stamp dall'ultimo controllo

        card = cards_by_item_id.get(item_id)
        if not card:
            continue

        card_name = clean_card_name(card)
        set_id = card.get("setId") or ""
        local_id = card.get("localId") or ""
        price_str = format_price(card)
        link = card_link(item_id)
        created_at = normalize_dt(alert.get("createdAt"))
        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S UTC") if created_at else "—"

        subject = f"[RED LINE] {card_name} #{local_id} stamp changed to {current_key}!"
        body = build_body(
            to_email, card_name, set_id, local_id, price_str,
            old_key or "—", current_key, link, created_at_str, now,
            unsubscribe_link(user_id, alert["_id"], item_id),
        )
        enqueue_mail(account_db, subject, body, to_email, user_id, alert["_id"])
        enqueue_notification(account_db, user_id, build_notification_text(card_name, local_id, old_key or "—", current_key))

        market_db.StampAlert.update_one(
            {"_id": alert["_id"]},
            {"$set": {
                "stampKey": current_key,
                "stampLabel": current_label,
                "stampCallDate": current_call_date,
                "notified": True,
                "lastNotifiedAt": now,
            }},
        )
        queued += 1
        print(f"[MAIL] Queued stamp alert -> {to_email} ({card_id}: {old_key} -> {current_key})")

    print(f"[END] Script completed. Queued emails: {queued}.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
