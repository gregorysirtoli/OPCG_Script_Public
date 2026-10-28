import html
import os
import traceback
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

from src.core.notifications import enqueue_notification, ensure_notification_indexes
from src.mail.emailTemplates import (
    BORDER,
    LINK,
    MUTED,
    NEGATIVE,
    POSITIVE,
    SITE_URL,
    TEXT,
    common_values,
    load_email_template,
    render_email_template,
)

# =============================================================================
# Environment & constants
# =============================================================================

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]
MONGODB_ACCOUNT_URI = os.environ["MONGODB_ACCOUNT_URI"]
MONGODB_ACCOUNT_DB = os.environ["MONGODB_ACCOUNT_DB"]

TOP_SETS = 5
TOP_NEWEST = 5
TOP_ITEMS = 10
TOP_PRODUCTS = 10
SNAPSHOT_COLLECTION = "PortfolioSnapshots"
PORTFOLIO_URL = f"{SITE_URL}/account/portfolio"
CARD_URL_BASE = f"{SITE_URL}/cards"
ACCOUNT_URL = f"{SITE_URL}/account"
EMAIL_TEMPLATE = "monthly-report"


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


def money_text(value: float) -> str:
    return f"${value:,.2f}"


def delta_html(value: float | None, pct: float | None) -> str:
    """Variazione in dollari e %, verde se positiva e rossa se negativa; N/A senza storico."""
    if value is None:
        return f"<span style='color:{MUTED}'>N/A</span>"
    color = POSITIVE if value > 0 else (NEGATIVE if value < 0 else TEXT)
    sign = "+" if value > 0 else ("-" if value < 0 else "")
    text = f"{sign}${abs(value):,.2f}"
    if pct is not None:
        text += f" ({sign}{abs(pct):.2f}%)"
    return f"<strong style='color:{color}'>{text}</strong>"


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


def enqueue_mail(account_db, subject: str, body: str, to: str, user_id: ObjectId) -> None:
    account_db.Mail.insert_one(
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


def get_last_monthly_report_run_at(account_db, user_id: ObjectId) -> datetime | None:
    last_mail = account_db.Mail.find_one(
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

    def item_name_html(row: dict[str, Any]) -> str:
        name = html.escape(str(row.get("name") or "Unknown"))
        item_id = row.get("id")
        if item_id is None:
            return name
        href = html.escape(f"{CARD_URL_BASE}/{item_id}", quote=True)
        return f"<a href='{href}' style='color:{TEXT};text-decoration:none'>{name}</a>"

    def fmt_added_date(row: dict[str, Any]) -> str:
        d = row.get("addedAt")
        if isinstance(d, datetime):
            return f"{d.strftime('%B')} {d.day}, {d.year}"
        return "N/A"

    def row_html(name_html: str, value: float, note: str = "") -> str:
        note_html = f"<br><span style='font-size:12px;color:{MUTED}'>{html.escape(note)}</span>" if note else ""
        return (
            f"<tr><td style='padding:9px 0;border-top:1px solid {BORDER};font-size:14px;line-height:1.5;color:{TEXT}'>"
            f"{name_html}{note_html}</td>"
            f"<td align='right' style='padding:9px 0 9px 12px;border-top:1px solid {BORDER};font-size:14px;"
            f"font-weight:700;white-space:nowrap;color:{TEXT}'>{money_text(value)}</td></tr>"
        )

    def section_html(title: str, rows_html: str, empty: str) -> str:
        body = rows_html or (
            f"<tr><td style='padding:9px 0;border-top:1px solid {BORDER};font-size:14px;color:{MUTED}'>{empty}</td></tr>"
        )
        return (
            f"<h2 style='margin:28px 0 8px 0;font-size:16px;line-height:1.4;font-weight:700;color:{TEXT}'>{title}</h2>"
            f"<table role='presentation' cellpadding='0' cellspacing='0' border='0' width='100%'>{body}</table>"
        )

    sections = "".join([
        section_html("Most valuable sets", "".join(row_html(html.escape(s["name"]), s["value"]) for s in top_sets), "No data"),
        section_html(
            "Newest additions",
            "".join(row_html(item_name_html(x), x["value"], f"Added {fmt_added_date(x)}") for x in newest),
            "No recent additions available",
        ),
        section_html("Most valuable cards", "".join(row_html(item_name_html(x), x["value"]) for x in top_items), "No card items"),
        section_html(
            "Most valuable products",
            "".join(row_html(item_name_html(x), x["value"]) for x in top_products),
            "No non-card products",
        ),
    ])
    sections += (
        f"<p style='margin:20px 0 0 0;font-size:14px;line-height:1.6'>"
        f"<a href='{PORTFOLIO_URL}' style='color:{LINK}'>See your full portfolio</a></p>"
    )

    return render_email_template(EMAIL_TEMPLATE, {
        **common_values(now),
        "email": user_email,
        "userName": user_name,
        "monthLabel": now.strftime("%B %Y"),
        "currentValue": money_text(current_value),
        "deltaMonthHtml": delta_html(delta_month, delta_month_pct),
        "deltaYearHtml": delta_html(delta_year, delta_year_pct),
        "portfolioUrl": PORTFOLIO_URL,
        "sectionsHtml": sections,
        "unsubscribeUrl": ACCOUNT_URL,
    })


def should_send_report(account_db, user_doc: dict[str, Any], user_id: ObjectId, now: datetime) -> bool:
    if not is_subscribed(user_doc):
        return False

    registered_at = get_registration_date(user_doc)
    if not registered_at:
        return False

    last_report_at = get_last_monthly_report_run_at(account_db, user_id)

    first_due = add_months(registered_at, 1)
    if now < first_due:
        return False

    if not last_report_at:
        return True

    next_due = add_months(last_report_at, 1)
    return now >= next_due


def main() -> None:
    market_client = MongoClient(MONGO_URI)
    market_db = market_client[MONGODB_DB]
    account_client = MongoClient(MONGODB_ACCOUNT_URI)
    account_db = account_client[MONGODB_ACCOUNT_DB]
    ensure_notification_indexes(account_db)
    load_email_template(EMAIL_TEMPLATE)  # se il sito non risponde ci si ferma prima di accodare

    now = datetime.now(timezone.utc)

    users = list(
        account_db.Users.find(
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

        if not should_send_report(account_db, user_doc, user_id, now):
            print(f"[SKIP] User {user_ref}: not due/subscribed.")
            continue

        to_email = (user_doc.get("email") or user_doc.get("userEmail") or "").strip()
        if not to_email:
            print(f"[SKIP] User {user_ref}: missing email.")
            continue

        holdings = get_user_holdings(market_db, user_doc)
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

        cards = list(market_db.Cards.find(card_match))
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
        last_month_value = pick_snapshot_value(market_db, user_id, month_cutoff)
        last_year_value = pick_snapshot_value(market_db, user_id, year_cutoff)

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

        enqueue_mail(account_db=account_db, subject=subject, body=body, to=to_email, user_id=user_id)
        enqueue_notification(account_db, user_id, build_notification_text(month_label))

        market_db[SNAPSHOT_COLLECTION].update_one(
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
