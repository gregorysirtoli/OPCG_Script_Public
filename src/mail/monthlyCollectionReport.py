import os
import traceback
from datetime import datetime, timezone
from typing import Any

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

TOP_SETS = 5
TOP_NEWEST = 5
TOP_ITEMS = 10
TOP_PRODUCTS = 10
SNAPSHOT_COLLECTION = "PortfolioSnapshots"
PORTFOLIO_URL = "https://redline.cards/account/portfolio"
CARD_URL_BASE = "https://redline.cards/cards"


# =============================================================================
# Helpers
# =============================================================================


def to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def normalize_dt(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_object_id(value: Any) -> ObjectId | None:
    if isinstance(value, ObjectId):
        return value
    if isinstance(value, dict) and "$oid" in value:
        try:
            return ObjectId(str(value["$oid"]))
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return ObjectId(text)
        except Exception:
            return None
    return None


def add_months(dt: datetime, months: int) -> datetime:
    month_index = (dt.year * 12 + (dt.month - 1)) + months
    year = month_index // 12
    month = (month_index % 12) + 1

    # clamp day for month length
    if month == 2:
        leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        max_day = 29 if leap else 28
    elif month in (4, 6, 9, 11):
        max_day = 30
    else:
        max_day = 31

    day = min(dt.day, max_day)
    return dt.replace(year=year, month=month, day=day)


def money_html(value: float) -> str:
    color = "#059669" if value > 0 else ("#dc2626" if value < 0 else "#374151")
    sign = "+" if value > 0 else ""
    return f"<span style='color:{color};font-weight:700'>{sign}${value:,.2f}</span>"


def money_bold_plain(value: float) -> str:
    return f"<b>${value:,.2f}</b>"


def pct_html(value: float | None) -> str:
    if value is None:
        return "<span style='color:#6b7280'>N/A</span>"
    color = "#059669" if value > 0 else ("#dc2626" if value < 0 else "#374151")
    sign = "+" if value > 0 else ""
    pct_text = f"{abs(value):.2f}".replace(".", ",")
    sign_text = "+" if value > 0 else ("-" if value < 0 else "")
    return f"<span style='color:{color};font-weight:700'>{sign_text}{pct_text}%</span>"


def pick_unit_price(card_doc: dict[str, Any]) -> float:
    md = card_doc.get("marketData") or {}
    for key in ("pricePrimary", "priceTrend", "priceLow"):
        v = md.get(key)
        if v is not None:
            p = to_float(v)
            if p > 0:
                return p
    return 0.0


def get_quantity(raw: dict[str, Any]) -> float:
    for k in ("quantity", "qty", "count", "copies"):
        if k in raw:
            q = to_float(raw.get(k))
            if q > 0:
                return q
    return 1.0


def get_added_at(raw: dict[str, Any]) -> datetime | None:
    for k in ("addedAt", "acquiredAt", "createdAt", "updatedAt"):
        d = normalize_dt(raw.get(k))
        if d:
            return d
    return None


def get_display_name(card_doc: dict[str, Any]) -> str:
    name = (card_doc.get("name") or "").strip()
    local_id = (card_doc.get("localId") or "").strip()
    set_id = (card_doc.get("setId") or "").strip()

    if local_id:
        base = f"{name} #{local_id}" if name else f"#{local_id}"
    else:
        base = name or str(card_doc.get("id") or card_doc.get("_id") or "Unknown")

    if set_id:
        return f"{base} ({set_id})"
    return base


def get_set_key(card_doc: dict[str, Any]) -> str:
    set_id = card_doc.get("setId")
    set_name = (card_doc.get("setName") or "").strip()
    set_code = (card_doc.get("setCode") or "").strip()

    if set_name:
        return set_name
    if set_code:
        return set_code
    if set_id:
        return str(set_id)
    return "Unknown Set"


def is_subscribed(user_doc: dict[str, Any]) -> bool:
    val = user_doc.get("monthlyReport")
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in {"true", "subscribed", "yes", "1"}
    if isinstance(val, (int, float)):
        return val != 0
    return False


def get_registration_date(user_doc: dict[str, Any]) -> datetime | None:
    return normalize_dt(user_doc.get("createdAt"))


def enqueue_mail(db, subject: str, body: str, to: str, user_id: ObjectId) -> None:
    db.Mail.insert_one(
        {
            "subject": subject,
            "body": body,
            "to": to,
            "status": "queued",  # queued, locked, sent, failed
            "createdAt": datetime.now(timezone.utc),
            "lockedAt": None,
            "lockedBy": None,
            "lastError": None,
            "retries": 0,
            "userId": user_id,
            "reportType": "monthlyCollection",
        }
    )


def build_notification_text(month_label: str) -> str:
    return f"Your monthly collection report for {month_label} is ready. Check your email for details!"


def get_user_holdings(db, user_doc: dict[str, Any]) -> list[dict[str, Any]]:
    holdings: list[dict[str, Any]] = []

    # Source of truth: dedicated Portfolio collection
    uid = user_doc.get("_id")
    if uid is None:
        return holdings

    # Primary schema: Portfolios -> PortfolioItems references
    portfolio_docs = list(
        db.Portfolios.find(
            {
                "$or": [
                    {"userId": uid},
                    {"userId": str(uid)},
                ]
            }
        )
    )

    for pdoc in portfolio_docs:
        refs = pdoc.get("items") if isinstance(pdoc.get("items"), list) else []
        ref_ids = [oid for oid in (normalize_object_id(r) for r in refs) if oid]

        if ref_ids:
            pitems = list(db.PortfolioItems.find({"_id": {"$in": ref_ids}}))
            for pi in pitems:
                item_id = pi.get("itemId") or pi.get("cardId") or pi.get("id") or pi.get("_id")
                oid_item = normalize_object_id(item_id)
                if oid_item:
                    item_id = oid_item
                if not item_id:
                    continue

                holdings.append(
                    {
                        "itemId": item_id,
                        "quantity": get_quantity(pi),
                        "addedAt": get_added_at(pi) or normalize_dt(pdoc.get("createdAt")),
                    }
                )

    if holdings:
        return holdings

    # Legacy fallback schema: Portfolio collection
    portfolio_docs = list(
        db.Portfolio.find(
            {
                "$or": [
                    {"userId": uid},
                    {"userId": str(uid)},
                ]
            }
        )
    )
    for pdoc in portfolio_docs:
        if isinstance(pdoc.get("items"), list):
            for row in pdoc["items"]:
                oid_item = normalize_object_id(row)
                if oid_item:
                    holdings.append(
                        {
                            "itemId": oid_item,
                            "quantity": 1.0,
                            "addedAt": normalize_dt(pdoc.get("createdAt")),
                        }
                    )
                    continue

                if not isinstance(row, dict):
                    continue
                item_id = row.get("itemId") or row.get("cardId") or row.get("id") or row.get("_id") or row.get("$oid")
                oid_item = normalize_object_id(item_id)
                if oid_item:
                    item_id = oid_item
                if item_id:
                    holdings.append(
                        {
                            "itemId": item_id,
                            "quantity": get_quantity(row),
                            "addedAt": get_added_at(row) or normalize_dt(pdoc.get("createdAt")),
                        }
                    )
        else:
            item_id = pdoc.get("itemId") or pdoc.get("cardId") or pdoc.get("id") or pdoc.get("_id") or pdoc.get("$oid")
            oid_item = normalize_object_id(item_id)
            if oid_item:
                item_id = oid_item
            if item_id:
                holdings.append(
                    {
                        "itemId": item_id,
                        "quantity": get_quantity(pdoc),
                        "addedAt": get_added_at(pdoc),
                    }
                )

    return holdings


def pick_snapshot_value(db, user_id: ObjectId, cutoff: datetime) -> float | None:
    snap = db[SNAPSHOT_COLLECTION].find_one(
        {"userId": user_id, "asOf": {"$lte": cutoff}},
        sort=[("asOf", -1)],
    )
    if not snap:
        return None
    return to_float(snap.get("totalValue"))


def get_last_monthly_report_run_at(db, user_id: ObjectId) -> datetime | None:
    last_mail = db.Mail.find_one(
        {
            "userId": user_id,
            "reportType": "monthlyCollection",
        },
        sort=[("createdAt", -1)],
    )
    if not last_mail:
        return None

    created_at = normalize_dt(last_mail.get("createdAt"))
    if created_at:
        return created_at
    return normalize_dt(last_mail.get("sentAt"))


def build_report_html(
    now: datetime,
    user_name: str,
    user_email: str,
    current_value: float,
    delta_month: float | None,
    delta_year: float | None,
    top_sets: list[dict[str, Any]],
    newest: list[dict[str, Any]],
    top_items: list[dict[str, Any]],
    top_products: list[dict[str, Any]],
) -> str:
    delta_month_pct = (delta_month / (current_value - delta_month) * 100.0) if delta_month is not None and (current_value - delta_month) > 0 else None
    delta_year_pct = (delta_year / (current_value - delta_year) * 100.0) if delta_year is not None and (current_value - delta_year) > 0 else None

    def render_value_line(v: float | None) -> str:
        if v is None:
            return "<span style='color:#6b7280'>N/A</span>"
        return money_html(v)

    def linked_item_name(row: dict[str, Any]) -> str:
        item_id = row.get("id")
        if item_id is None:
            return str(row.get("name") or "Unknown")
        return f"<a href='{CARD_URL_BASE}/{str(item_id)}'>{row.get('name') or 'Unknown'}</a>"

    lines_sets = "".join(
        f"<li>{s['name']}: {money_bold_plain(s['value'])}</li>" for s in top_sets
    ) or "<li><span style='color:#6b7280'>No data</span></li>"

    def fmt_added_date(row: dict[str, Any]) -> str:
        d = row.get("addedAt")
        if isinstance(d, datetime):
            return f"{d.strftime('%B')} {d.day}, {d.year}"
        return "N/A"

    lines_newest = "".join(
        f"<li>{linked_item_name(x)} - {fmt_added_date(x)} - {money_bold_plain(x['value'])}</li>" for x in newest
    ) or "<li><span style='color:#6b7280'>No recent additions available</span></li>"

    lines_items = "".join(
        f"<li>{linked_item_name(x)} - {money_bold_plain(x['value'])}</li>" for x in top_items
    ) or "<li><span style='color:#6b7280'>No card items</span></li>"

    lines_products = "".join(
        f"<li>{linked_item_name(x)} - {money_bold_plain(x['value'])}</li>" for x in top_products
    ) or "<li><span style='color:#6b7280'>No non-card products</span></li>"

    return (
        f"Hi {user_name},<br>"
        "here is your monthly collection report by Red Line.<br><br>"
        "<h3>Collection Summary</h3>"
        "<ul>"
        f"<li><b>Current Value:</b> {money_html(current_value)}</li>"
        f"<li><b>Change From Last Month:</b> {render_value_line(delta_month)} ({pct_html(delta_month_pct)})</li>"
        f"<li><b>Change From Last Year:</b> {render_value_line(delta_year)} ({pct_html(delta_year_pct)})</li>"
        "</ul>"
        f"<a href='{PORTFOLIO_URL}'>Open your portfolio</a><br><br>"
        "<h3>Most Valuable Sets</h3>"
        f"<ul>{lines_sets}</ul>"
        "<h3>Newest Additions</h3>"
        f"<ul>{lines_newest}</ul>"
        "<h3>Most Valuable Items (Cards)</h3>"
        f"<ul>{lines_items}</ul>"
        "<h3>Most Valuable Products (Non-Cards)</h3>"
        f"<ul>{lines_products}</ul>"
        f"<br><br><a href='{PORTFOLIO_URL}'>Open your portfolio</a><br><br>"
        f"<br>This email was sent to {user_email} because you created a collection on RED LINE (https://redline.cards/)."
        "<br>You can <a href='https://redline.cards/account'>unsubscribe</a> from these emails if you no longer want to receive them..<br>"
        "______<br><br>"
        "<i>This e-mail may contain confidential and/or privileged information.<br>"
        "If you are not the intended recipient or have received this e-mail in error, please notify the sender immediately and delete this e-mail.<br>"
        "Any unauthorized copying, disclosure, or distribution of the material contained in this e-mail is strictly prohibited.</i><br><br>"
        f"### This is an automatically generated message on UTC {now.strftime('%Y-%m-%d %H:%M:%S')} ###<br><br>"
    )


def should_send_report(db, user_doc: dict[str, Any], user_id: ObjectId, now: datetime) -> bool:
    if not is_subscribed(user_doc):
        return False

    registered_at = get_registration_date(user_doc)
    if not registered_at:
        return False

    last_report_at = get_last_monthly_report_run_at(db, user_id)

    first_due = add_months(registered_at, 1)
    if now < first_due:
        return False

    if not last_report_at:
        return True

    next_due = add_months(last_report_at, 1)
    return now >= next_due


def main() -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGODB_DB]
    ensure_notification_indexes(db)

    now = datetime.now(timezone.utc)

    users = list(
        db.Users.find(
            {
                "monthlyReport": "subscribed",
                "$or": [{"email": {"$exists": True}}, {"userEmail": {"$exists": True}}],
            }
        )
    )

    if not users:
        print("[INFO] No subscribed users for monthly report.")
        return

    queued = 0

    for user_doc in users:
        user_ref = str(user_doc.get("_id") or "unknown")
        user_id = user_doc.get("_id")
        if not isinstance(user_id, ObjectId):
            continue

        if not should_send_report(db, user_doc, user_id, now):
            print(f"[SKIP] User {user_ref}: not due/subscribed.")
            continue

        to_email = (user_doc.get("email") or user_doc.get("userEmail") or "").strip()
        if not to_email:
            print(f"[SKIP] User {user_ref}: missing email.")
            continue

        holdings = get_user_holdings(db, user_doc)
        if not holdings:
            print(f"[SKIP] User {user_ref}: no portfolio holdings.")
            continue

        item_ids_raw = [h["itemId"] for h in holdings if h.get("itemId")]
        item_ids_oid = [x for x in (normalize_object_id(v) for v in item_ids_raw) if x]
        item_ids_str = [str(v) for v in item_ids_raw if not normalize_object_id(v)]

        card_match: dict[str, Any] = {"_id": {"$in": item_ids_oid}} if item_ids_oid else {"_id": {"$in": []}}
        if item_ids_str:
            card_match = {
                "$or": [
                    {"_id": {"$in": item_ids_oid}} if item_ids_oid else {"_id": {"$in": []}},
                    {"id": {"$in": item_ids_str}},
                ]
            }

        cards = list(db.Cards.find(card_match))
        by_id = {c.get("_id"): c for c in cards}
        by_legacy_id = {str(c.get("id")): c for c in cards if c.get("id") is not None}

        rows: list[dict[str, Any]] = []
        for h in holdings:
            hid = h["itemId"]
            card = by_id.get(hid)
            if not card:
                oid_hid = normalize_object_id(hid)
                if oid_hid:
                    card = by_id.get(oid_hid)
            if not card and isinstance(hid, str):
                card = by_legacy_id.get(hid)
            if not card:
                continue

            qty = max(0.0, h.get("quantity") or 0.0)
            if qty <= 0:
                continue

            unit_price = pick_unit_price(card)
            total_value = unit_price * qty
            if total_value <= 0:
                continue

            item_type = (card.get("type") or "").strip() or "Unknown"
            rows.append(
                {
                    "id": card.get("_id"),
                    "name": get_display_name(card),
                    "type": item_type,
                    "qty": qty,
                    "unitPrice": unit_price,
                    "value": total_value,
                    "set": get_set_key(card),
                    "addedAt": h.get("addedAt"),
                }
            )

        if not rows:
            print(f"[SKIP] User {user_ref}: no priced rows from holdings={len(holdings)} cards={len(cards)}.")
            continue

        current_value = sum(r["value"] for r in rows)

        month_cutoff = add_months(now, -1)
        year_cutoff = add_months(now, -12)
        last_month_value = pick_snapshot_value(db, user_id, month_cutoff)
        last_year_value = pick_snapshot_value(db, user_id, year_cutoff)

        delta_month = (current_value - last_month_value) if last_month_value is not None else None
        delta_year = (current_value - last_year_value) if last_year_value is not None else None

        sets_map: dict[str, float] = {}
        for r in rows:
            key = r["set"]
            sets_map[key] = sets_map.get(key, 0.0) + r["value"]
        top_sets = [
            {"name": k, "value": v}
            for k, v in sorted(sets_map.items(), key=lambda x: x[1], reverse=True)[:TOP_SETS]
        ]

        newest = sorted(
            [r for r in rows if isinstance(r.get("addedAt"), datetime)],
            key=lambda x: x["addedAt"],
            reverse=True,
        )[:TOP_NEWEST]

        top_items = sorted(
            [r for r in rows if str(r["type"]).lower() == "cards"],
            key=lambda x: x["value"],
            reverse=True,
        )[:TOP_ITEMS]

        top_products = sorted(
            [r for r in rows if str(r["type"]).lower() != "cards"],
            key=lambda x: x["value"],
            reverse=True,
        )[:TOP_PRODUCTS]

        user_name = (user_doc.get("name") or user_doc.get("username") or "there").strip()

        body = build_report_html(
            now=now,
            user_name=user_name,
            user_email=to_email,
            current_value=current_value,
            delta_month=delta_month,
            delta_year=delta_year,
            top_sets=top_sets,
            newest=newest,
            top_items=top_items,
            top_products=top_products,
        )

        month_label = now.strftime("%B %Y")
        subject = f"[RED LINE] 🔔 Monthly Collection Report - {month_label}"

        enqueue_mail(db=db, subject=subject, body=body, to=to_email, user_id=user_id)
        enqueue_notification(db, user_id, build_notification_text(month_label))

        db[SNAPSHOT_COLLECTION].update_one(
            {"userId": user_id, "asOf": now},
            {
                "$set": {
                    "userId": user_id,
                    "asOf": now,
                    "totalValue": round(current_value, 2),
                    "updatedAt": now,
                }
            },
            upsert=True,
        )

        queued += 1
        print(f"[MAIL] Queued monthly report for: {user_id}")

    print(f"[END] Script completed. Queued reports: {queued}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
