from datetime import datetime, timezone
from pymongo import MongoClient
import statistics
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
import traceback
import csv
from typing import Optional, Dict, List, Any, Tuple

# ============================================================
# ENV
# ============================================================
load_dotenv(".env.local")
load_dotenv()

REQUIRED = ["MONGODB_URI", "MONGODB_DB", "EMAIL_ADDRESS", "EMAIL_PASSWORD", "EMAIL_TO"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

# ============================================================
# EMAIL CONFIG
# ============================================================
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.environ["EMAIL_ADDRESS"]
SMTP_PASSWORD = os.environ["EMAIL_PASSWORD"]
EMAIL_TO = os.environ["EMAIL_TO"]


def send_mail(esito: str, messaggio: str, allegati: Optional[List[str]] = None):
    subject = f"[PYTHON] - {esito}"
    body = f"{messaggio}\n\nOra: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"

    email = MIMEMultipart()
    email["From"] = SMTP_USER
    email["To"] = EMAIL_TO
    email["Subject"] = subject
    email.attach(MIMEText(body, "plain"))

    if allegati:
        for path in allegati:
            if path and os.path.exists(path):
                with open(path, "rb") as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(path))
                    part["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
                    email.attach(part)

    server = None
    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(email)
        print("✅ [MAIL] Mail inviata con successo.")
    except Exception as e:
        print(f"❌ [MAIL] Errore durante l'invio della mail: {e}")
    finally:
        if server:
            server.quit()


# ============================================================
# METRICHE
# ============================================================
def calcola_ev(prezzi: List[float]) -> float:
    return statistics.mean(prezzi) if prezzi else 0.0


def calcola_icv(prezzi_oltre_10: List[float]) -> float:
    if not prezzi_oltre_10:
        return 1.0
    top3 = sorted(prezzi_oltre_10, reverse=True)[:3]
    tot = sum(prezzi_oltre_10)
    return (sum(top3) / tot) if tot > 0 else 1.0


# ============================================================
# PREZZO CARTE: da Cards.marketData.*
# ============================================================
def pick_price_from_card(card: Dict[str, Any]) -> Optional[float]:
    md = card.get("marketData")
    if not isinstance(md, dict):
        return None

    for k in ("price", "priceTrend", "price7d", "price30d", "price90d", "price1d", "priceSecondary", "pricePrimary"):
        v = md.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)

    return None

def get_box_cost(db, sealed_id: Optional[str], default_cost: float = 200.0) -> float:
    """
    Box cost ricavato dalla collection Cards usando sealedId come Cards.id
    e leggendo il prezzo da marketData con la stessa logica delle carte.
    Fallback: default_cost
    """
    if not sealed_id:
        return default_cost

    doc = db.Cards.find_one({"id": sealed_id}, {"marketData": 1})
    price = pick_price_from_card(doc) if doc else None
    return price if price is not None else default_cost


# ============================================================
# TIER SOLO SU test_ratio
# ============================================================
def tier_by_test_ratio(test_ratio: float) -> str:
    if test_ratio >= 0.30:
        return "S"
    if test_ratio >= 0.20:
        return "A+"
    if test_ratio >= 0.12:
        return "A"
    if test_ratio >= 0.06:
        return "B"
    return "C"


# ============================================================
# REPORT CSV
# ============================================================
def dump_csv(rows: List[dict], path: str):
    if not rows:
        return
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# ============================================================
# CORE BUILDER (tiers + report_rows)
# ============================================================
def build_tierlist(
    db,
    set_ids: List[str],
    market: str = "en",
    only_verified_visible: bool = True
) -> Tuple[dict, List[dict]]:
    tiers = {"S": [], "A+": [], "A": [], "B": [], "C": []}
    report_rows: List[dict] = []

    # buffer print ordinato
    print_buffer = {"S": [], "A+": [], "A": [], "B": [], "C": []}

    sets = list(
        db.Sets.find(
            {"id": {"$in": set_ids}},
            {"id": 1, "name": 1, "sealedId": 1}
        )
    )

    cards_col = db.Cards

    for s in sets:
        set_id = s.get("id")
        set_name = s.get("name")
        sealed_id = s.get("sealedId")
        if not set_id:
            continue

        # Costo box (da Prices, default 200)
        box_cost = get_box_cost(db, sealed_id, default_cost=200.0)

        # Query carte
        q = {"setId": set_id, "type": "Cards"}
        if only_verified_visible:
            q["visible"] = True
            q["verified"] = True

        card_docs = list(cards_col.find(q, {"id": 1, "marketData": 1}))

        total_cards = len(card_docs)
        if total_cards == 0:
            report_rows.append({
                "setId": set_id,
                "setName": set_name,
                "market": market,
                "tier": "n/a",
                "total_cards": 0,
                "priced_cards": 0,
                "missing_price": 0,
                "box_cost": box_cost,
                "note": "No cards found"
            })
            continue

        prezzi: List[float] = []
        for c in card_docs:
            p = pick_price_from_card(c)
            if p is not None:
                prezzi.append(p)

        priced_cards = len(prezzi)
        missing_price = total_cards - priced_cards

        if priced_cards == 0:
            report_rows.append({
                "setId": set_id,
                "setName": set_name,
                "market": market,
                "tier": "n/a",
                "total_cards": total_cards,
                "priced_cards": 0,
                "missing_price": missing_price,
                "box_cost": box_cost,
                "note": "No priced cards"
            })
            continue

        prezzi_sorted = sorted(prezzi)
        sum_prices = sum(prezzi_sorted)
        ev = calcola_ev(prezzi_sorted)
        median = statistics.median(prezzi_sorted)
        stdev = statistics.pstdev(prezzi_sorted) if priced_cards > 1 else 0.0

        prezzi_oltre_10 = [p for p in prezzi_sorted if p > 10]
        pct10 = (len(prezzi_oltre_10) / priced_cards) * 100.0
        sum_gt10 = sum(prezzi_oltre_10)
        top3_gt10 = sorted(prezzi_oltre_10, reverse=True)[:3]
        icv = calcola_icv(prezzi_oltre_10)

        # Tier SOLO da test_ratio
        test_ratio = (ev / box_cost) if (box_cost and box_cost > 0) else 0.0
        tier = tier_by_test_ratio(test_ratio)

        # tiers in output finale (Mongo)
        tiers[tier].append({"name": set_name, "id": set_id})

        # report dettagliato
        row = {
            "setId": set_id,
            "setName": set_name,
            "market": market,
            "tier": tier,
            "sealedId": sealed_id,
            "box_cost": round(box_cost, 2) if box_cost is not None else None,
            "total_cards": total_cards,
            "priced_cards": priced_cards,
            "missing_price": missing_price,
            "sum_prices": round(sum_prices, 2),
            "ev_mean": round(ev, 4),
            "median": round(median, 4),
            "stdev": round(stdev, 4),
            "pct_gt10": round(pct10, 2),
            "count_gt10": len(prezzi_oltre_10),
            "sum_gt10": round(sum_gt10, 2),
            "top1_gt10": round(top3_gt10[0], 2) if len(top3_gt10) > 0 else None,
            "top2_gt10": round(top3_gt10[1], 2) if len(top3_gt10) > 1 else None,
            "top3_gt10": round(top3_gt10[2], 2) if len(top3_gt10) > 2 else None,
            "icv": round(icv, 4),
            "test_ratio": round(test_ratio, 6),
        }
        report_rows.append(row)

        # salva la riga di output nel buffer del suo tier (NON stampare qui)
        print_buffer[tier].append(
            f"[{market.upper()}] {set_id} | cards={total_cards} priced={priced_cards} miss={missing_price} | "
            f"sum_prices={sum_prices:.2f} EV={ev:.2f} median={median:.2f} pct>10={pct10:.1f}% ICV={icv:.2f} "
            f"box={box_cost:.0f} test_ratio={test_ratio:.4f}| tier={tier}"
        )

    # ✅ stampa UNA SOLA VOLTA, a fine loop, in ordine tier
    for t in ["S", "A+", "A", "B", "C"]:
        if print_buffer[t]:
            print(f"\n--- TIER {t} ---")
            for line in print_buffer[t]:
                print(line)

    return tiers, report_rows


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGODB_DB]

        # ---------------- EN ----------------
        print("\n✅ [TIERLIST] GLOBAL (EN)")
        OP_IDS_EN = [
            "OP01", "OP02", "OP03", "OP04", "OP05", "OP06",
            "OP07", "OP08", "OP09", "OP10", "OP11", "OP12",
            "OP13", "OP14", "EB01", "EB02", "PRB01", "PRB02"
        ]

        tiers_en, report_en = build_tierlist(db, OP_IDS_EN, market="en", only_verified_visible=True)

        # ✅ SALVATAGGIO con output identico al JSON che vuoi
        db.Tierlist.delete_many({"language": "en"})
        db.Tierlist.insert_one({
            "date": datetime.now(timezone.utc),
            "language": "en",
            "tiers": tiers_en
        })

        # ---------------- JP ----------------
        print("\n✅ [TIERLIST] JAPANESE (JP)")
        OP_IDS_JP = [
            "OP01JP", "OP02JP", "OP03JP", "OP04JP", "OP05JP", "OP06JP",
            "OP07JP", "OP08JP", "OP09JP", "OP10JP", "OP11JP", "OP12JP",
            "OP13JP", "OP14JP", "EB01JP", "EB02JP", "EB03JP", "PRB01JP", "PRB02JP"
        ]

        tiers_jp, report_jp = build_tierlist(db, OP_IDS_JP, market="jp", only_verified_visible=True)

        # ✅ SALVATAGGIO con output identico al JSON che vuoi
        db.Tierlist.delete_many({"language": "jp"})
        db.Tierlist.insert_one({
            "date": datetime.now(timezone.utc),
            "language": "jp",
            "tiers": tiers_jp
        })

        print("\n✅ [END] Fine processo creazione tierlist.")
        send_mail(
            "✅ TIERLIST COMPLETATA",
            "Tierlist EN/JP create con successo. In allegato i report CSV."
        )

    except Exception:
        send_mail("❌ ERRORE TIERLIST", traceback.format_exc())
        raise
