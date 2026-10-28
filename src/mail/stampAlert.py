import html
import os
import re
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

from src.core.notifications import enqueue_notification, ensure_notification_indexes
from src.mail.emailTemplates import SITE_URL, common_values, load_email_template, render_email_template

# =============================================================================
# Environment & constants
# =============================================================================

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]
MONGODB_ACCOUNT_URI = os.environ["MONGODB_ACCOUNT_URI"]
MONGODB_ACCOUNT_DB = os.environ["MONGODB_ACCOUNT_DB"]

CARD_LINK_BASE = f"{SITE_URL}/cards"
EMAIL_TEMPLATE = "stamp-alert"

# Etichette degli stamp come sul sito (OPCG lib/ledger-stamps.ts, STAMP_LABELS).
STAMP_LABELS = {
    "ACCUMULATE": "Accumulate",
    "HOLD": "Neutral",
    "SELL_WATCH": "Sell Watch",
    "REBOUND_WATCH": "Speculative",  # "Rebound Watch"
    "BUY_SETUP": "Buy Setup",
}

# Colori dei badge come sul sito in tema chiaro (lib/ledger-stamps.ts STAMP_KEY_CLASS e STAMP_STYLE,
# misurati con i CSS del sito): (testo, sfondo, bordo).
STAMP_BADGE_COLORS = {
    "ACCUMULATE": ("#22c55e", "#d1e7dd", "#e6e6e6"),
    "HOLD": ("#141824", "#e2e3e5", "#e6e6e6"),
    "SELL_WATCH": ("#ef4444", "#f8d7da", "#e6e6e6"),
    "REBOUND_WATCH": ("#7c3aed", "#ede9fe", "#c4b5fd"),
    "BUY_SETUP": ("#3b82f6", "#cff4fc", "#e6e6e6"),
}


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
    return f"{SITE_URL}/api/alerts/stamp/unsubscribe?u={user_id}&_id={alert_id}&i={item_id}"


def stamp_label(stamp_key: str | None) -> str:
    return STAMP_LABELS.get(stamp_key or "") or stamp_key or "—"


def stamp_badge_html(stamp_key: str | None) -> str:
    """Badge dello stamp come sul sito; senza stamp precedente solo un trattino."""
    colors = STAMP_BADGE_COLORS.get(stamp_key or "")
    label = stamp_label(stamp_key)
    if not colors:
        return html.escape(label)
    color, background, border = colors
    return (
        f"<span style='display:inline-block;padding:4px 9px;border:1px solid {border};border-radius:4px;"
        f"background-color:{background};color:{color};font-size:12px;font-weight:700;line-height:1.2;"
        f"white-space:nowrap;vertical-align:middle'>{html.escape(label)}</span>"
    )


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
    return render_email_template(EMAIL_TEMPLATE, {
        **common_values(now_utc),
        "email": to_email,
        "cardName": card_name,
        "setId": set_id,
        "localId": local_id,
        "price": price_str,
        "oldStamp": stamp_label(old_stamp),
        "newStamp": stamp_label(new_stamp),
        "oldStampHtml": stamp_badge_html(old_stamp),
        "newStampHtml": stamp_badge_html(new_stamp),
        "cardUrl": link,
        "alertCreatedAt": created_at_str,
        "unsubscribeUrl": unsubscribe_url,
    })


def build_notification_text(card_name: str, local_id: str, old_stamp: str, new_stamp: str) -> str:
    return f"{card_name} #{local_id} changed stamp: {stamp_label(old_stamp)} -> {stamp_label(new_stamp)}"


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    market_client = MongoClient(MONGO_URI)
    market_db = market_client[MONGODB_DB]
    account_client = MongoClient(MONGODB_ACCOUNT_URI)
    account_db = account_client[MONGODB_ACCOUNT_DB]
    ensure_notification_indexes(account_db)
    load_email_template(EMAIL_TEMPLATE)  # se il sito non risponde ci si ferma prima di accodare

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

        subject = f"[RED LINE] {card_name} #{local_id} stamp changed to {stamp_label(current_key)}!"
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
