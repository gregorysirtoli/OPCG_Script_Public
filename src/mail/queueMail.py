import os
import asyncio
import aiohttp
import decimal
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus
from bson import ObjectId

# =============================================================================
# Environment & constants
# =============================================================================

load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

TCG_CLIENT_ID = os.environ["TCG_CLIENT_ID"]
TCG_CLIENT_SECRET = os.environ["TCG_CLIENT_SECRET"]
if not TCG_CLIENT_ID or not TCG_CLIENT_SECRET:
    raise RuntimeError("Missing TCG credentials (TCG_CLIENT_ID / TCG_CLIENT_SECRET).")

API_TOKEN_URL = "https://api.tcgplayer.com/token"
API_PRICING_URL_TPL = "https://api.tcgplayer.com/pricing/product/{product_ids_csv}"

CREDIT_COST = 10
MIN_DELTA_HOURS = 6

TCG_CHUNK_SIZE = 50 # number of productIds per pricing request
HTTP_TIMEOUT_SECONDS = 30 # total request timeout

decimal.getcontext().prec = 10

# =============================================================================
# Helpers
# =============================================================================

# Decimal conversion
def D(x) -> decimal.Decimal:
    try:
        return decimal.Decimal(str(x))
    except Exception:
        return decimal.Decimal("0")

# Yield list chunks of size n
def chunks(lst: list[int], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Queue an email in MongoDB for later delivery by a dedicated worker.
def enqueue_mail(db, subject: str, body: str, to: str, alert_id: ObjectId) -> None:
    db.Mail.insert_one({
        "subject": subject,
        "body": body,
        "to": to,
        "status": "queued", # queued, locked, sent, failed
        "createdAt": datetime.now(timezone.utc),
        "lockedAt": None,
        "lockedBy": None,
        "lastError": None,
        "retries": 0,
         "alertId": alert_id,
    })

# =============================================================================
# TCGplayer auth & pricing
# =============================================================================

#  Client-credentials bearer token with in-memory caching.
class TcgAuth:
    def __init__(self):
        self._token: Optional[str] = None
        self._expires_at: float = 0.0 # epoch seconds

    async def get_bearer(self, session: aiohttp.ClientSession) -> str:
        # Reuse token if still valid (60s safety buffer)
        if self._token and time.time() < (self._expires_at - 60):
            return self._token

        data = {
            "grant_type": "client_credentials",
            "client_id": TCG_CLIENT_ID,
            "client_secret": TCG_CLIENT_SECRET,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with session.post(
            API_TOKEN_URL,
            data=data,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
        ) as resp:
            resp.raise_for_status()
            js = await resp.json()

        token = js["access_token"]
        expires_in = int(js.get("expires_in", 0))
        self._token = token
        self._expires_at = time.time() + expires_in
        return token


AUTH = TcgAuth()

# Call TCGplayer official pricing endpoint and group results by productId
async def fetch_tcg_pricing_grouped(
    session: aiohttp.ClientSession,
    product_ids: list[int],
) -> dict[int, list[dict]]:

    if not product_ids:
        return {}

    bearer = await AUTH.get_bearer(session)
    ids_csv = ",".join(str(i) for i in product_ids)
    url = API_PRICING_URL_TPL.format(product_ids_csv=ids_csv)

    headers = {
        "Accept": "application/json",
        "Authorization": f"bearer {bearer}",
    }

    async with session.get(
        url,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
    ) as resp:
        resp.raise_for_status()
        js = await resp.json()

    grouped: dict[int, list[dict]] = {}
    for row in js.get("results", []) or []:
        pid = row.get("productId")
        if isinstance(pid, int):
            grouped.setdefault(pid, []).append(row)

    return grouped


def pick_market_price_by_printing(rows: list[dict], printing: str) -> decimal.Decimal:
    """
    Rule B (DB-consistent subtype selection):

    - If Cards.printing == "Foil"   -> pick subTypeName == "Foil"
    - Otherwise                     -> pick subTypeName == "Normal"
    - If marketPrice is null        -> fallback to lowPrice (same subtype)
    - If nothing usable             -> fallback to first available marketPrice
    - Else                          -> 0
    """
    printing_norm = (printing or "").strip().lower()
    wanted_subtype = "Foil" if printing_norm == "foil" else "Normal"

    # 1) marketPrice for the wanted subtype
    for r in rows:
        if r.get("subTypeName") == wanted_subtype and r.get("marketPrice") is not None:
            return D(r["marketPrice"])

    # 2) lowPrice for the wanted subtype
    for r in rows:
        if r.get("subTypeName") == wanted_subtype and r.get("lowPrice") is not None:
            return D(r["lowPrice"])

    # 3) any marketPrice as last resort
    for r in rows:
        if r.get("marketPrice") is not None:
            return D(r["marketPrice"])

    return decimal.Decimal("0")


# =============================================================================
# Alert evaluation
# =============================================================================

# Evaluate alert conditions across multiple candidate prices.
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

    # Prevent division by zero for percent change checks
    base_ok = base_price is not None and base_price > 0

    for label, current in price_candidates:
        if current <= 0:
            continue

        # Target-based condition
        if target_price and direction == "lte" and current <= target_price:
            hit_target = True
            first_hit = first_hit or (label, current)
        elif target_price and direction == "gte" and current >= target_price:
            hit_target = True
            first_hit = first_hit or (label, current)

        # Percent change condition (directional)
        if change_p is not None and base_ok:
            delta_pct = (current - base_price) / base_price * 100
            if change_p < 0 and delta_pct <= change_p:
                hit_change = True
                first_hit = first_hit or (label, current)
            elif change_p > 0 and delta_pct >= change_p:
                hit_change = True
                first_hit = first_hit or (label, current)

    return hit_target, hit_change, first_hit

#  Throttle recurrent alerts so we don't spam users. Non-recurrent alerts are expected to be filtered upstream.
def is_throttled(alert_doc: dict, now_utc: datetime) -> bool:
    if not alert_doc.get("recurrent"):
        return False

    last = alert_doc.get("lastNotifiedAt")
    if not last:
        return False

    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)

    return (now_utc - last).total_seconds() < MIN_DELTA_HOURS * 3600


# =============================================================================
# Main
# =============================================================================

async def main() -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGODB_DB]

    # Load active alerts and join Cards by itemId -> Cards._id
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

    # Collect unique tcgPlayerId values
    tcg_ids: set[int] = set()
    for a in alerts:
        tcg_id = a.get("cardData", {}).get("tcgPlayerId")
        if isinstance(tcg_id, int) and tcg_id > 0:
            tcg_ids.add(tcg_id)

    # Fetch TCGplayer pricing grouped rows in chunks
    tcg_rows: dict[int, list[dict]] = {}
    if tcg_ids:
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(fetch_tcg_pricing_grouped(session, chunk))
                for chunk in chunks(list(tcg_ids), TCG_CHUNK_SIZE)
            ]
            results = await asyncio.gather(*tasks)
            for block in results:
                for pid, rows in block.items():
                    tcg_rows.setdefault(pid, []).extend(rows)

    now = datetime.now(timezone.utc)

    for a in alerts:
        # Skip if throttled (recurrent) or already notified (non-recurrent)
        if is_throttled(a, now):
            continue
        if not a.get("recurrent") and a.get("notified"):
            continue

        card = a["cardData"]

        created_at = a.get("createdAt")
        if created_at and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S UTC") if created_at else "ERROR"


        base = D(a.get("basePrice"))
        target = D(a.get("priceTarget") or "0")
        direction = (a.get("direction") or a.get("targetCondition") or "lte").lower()

        # Build price candidates (NO Prices collection)
        candidates: list[tuple[str, decimal.Decimal]] = []

        # Candidate from marketData: pricePrimary -> priceTrend -> priceLow (first available only)
        md = card.get("marketData") or {}
        if md.get("pricePrimary") is not None:
            candidates.append(("pricePrimary", D(md.get("pricePrimary"))))
        elif md.get("priceTrend") is not None:
            candidates.append(("priceTrend", D(md.get("priceTrend"))))
        elif md.get("priceLow") is not None:
            candidates.append(("priceLow", D(md.get("priceLow"))))

        # Candidate from TCGplayer pricing (Rule B via Cards.printing)
        tcg_id = card.get("tcgPlayerId")
        if isinstance(tcg_id, int) and tcg_id in tcg_rows:
            printing = card.get("printing", "Normal")
            tcg_price = pick_market_price_by_printing(tcg_rows[tcg_id], printing)
            if tcg_price > 0:
                candidates.append((f"tcgplayer:{printing}", tcg_price))

        if not candidates:
            continue

        hit_target, hit_change, first_hit = any_price_hits_conditions(
            price_candidates=candidates,
            base_price=base,
            target_price=target,
            direction=direction,
            change_raw=a.get("priceChange"),
        )
        if not (hit_target or hit_change):
            continue

        # Atomically decrement credit only if sufficient
        user_id = a.get("userId")
        if not user_id:
            continue

        credit_res = db.Users.update_one(
            {"_id": user_id, "credit": {"$gte": CREDIT_COST}},
            {"$inc": {"credit": -CREDIT_COST, "creditSpent": CREDIT_COST}},
        )
        if credit_res.matched_count == 0:
            print(f"[SKIP] Not enough credit for user {user_id}.")
            continue

        # Build email content
        card_name = card.get("name") or f"Card {card.get('id', '')}"
        if card.get("localId"):
            card_name += f" #{card['localId']}"

        hit_label, hit_price = first_hit if first_hit else candidates[0]

        tcg_url = f"https://www.tcgplayer.com/product/{tcg_id}" if isinstance(tcg_id, int) else ""
        ebay_url = (
            "https://www.ebay.com/sch/i.html?_nkw="
            + quote_plus(card_name)
            + "&mkcid=1&mkrid=711-53200-19255-0&siteid=0&campid=5339118630"
            + "&customid=redlinecards&toolid=10001&mkevt=1"
        )

        clean_name = re.sub(r"\s*\[.*?\]\s*", "", (card.get("name") or "")).strip()
        local_id = (card.get("localId") or "").strip()
        encoded_query = quote_plus(f"{clean_name} {local_id}".strip())

        cardmarket_url = (
            "https://www.cardmarket.com/it/OnePiece/Products/Search"
            f"?referrer=Yovacca&mode=gallery&searchString={encoded_query}"
        )
        cardtrader_url = "https://www.cardtrader.com/invite/ivory-swamp-389"

        body = (
            "Hi,<br>"
            "we have good news for you! <br><br>One of the cards you have been looking for has reached your price conditions.<br><br>"
            f"<b>{clean_name} #{local_id}</b> card is currently at <b>${hit_price:.2f}</b>.<br><br>"
            "Don't miss this opportunity!<br>"
            "<ul>"
            f"<li>Check it out on <a href='{cardmarket_url}'>Cardmarket</a></li>"
            + (f"<li>Check it out on <a href='{tcg_url}'>TCGPlayer</a></li>" if tcg_url else "")
            + f"<li>Check it out on <a href='{ebay_url}'>Ebay</a></li>"
            f"<li>Check it out on <a href='{cardtrader_url}'>CardTrader</a></li>"
            "</ul>"
            f"<br>You are receiving this notification because on date {created_at_str} you have set up an alert for the card {card_name}.<br>"
            "If you wish to stop receiving notifications, please log in to your account and update your mail alert preferences in the settings.<br>"
            "This is a free notification service of the RED LINE website (https://redline.cards/).<br><br>"
            "______<br><br>"
            "<i>This e-mail may contain confidential and/or privileged information.<br>"
            "If you are not the intended recipient or have received this e-mail in error, please notify the sender immediately and delete this e-mail.<br>"
            "Any unauthorized copying, disclosure, or distribution of the material contained in this e-mail is strictly prohibited</i>.<br><br>"
            f"### This is an automatically generated message on UTC {now.strftime('%Y-%m-%d %H:%M:%S')} ###<br><br>"
        )

        to_email = a.get("userEmail")
        if not to_email:
            continue

        # Queue email in DB
        enqueue_mail(
            db=db,
            subject=f"[RED LINE] 🔔 {clean_name} #{local_id} has a new price!",
            body=body,
            to=to_email,
            alert_id=a["_id"],
        )
        print(f"[MAIL] Queued: {card_name} -> {to_email}")

        # Update alert notification fields
        update = {"$set": {"lastNotifiedAt": now}}
        if not a.get("recurrent"):
            update["$set"]["notified"] = True
        db.PricesAlert.update_one({"_id": a["_id"]}, update)

    print("[END] Script completed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc()
