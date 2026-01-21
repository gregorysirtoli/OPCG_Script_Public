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
from typing import Optional, Dict, List, Any

# Carica variabili da ambiente (.env o .env.local)
load_dotenv(".env.local")
load_dotenv()

# === CHECK ENV  ===
REQUIRED = ["MONGODB_URI", "MONGODB_DB", "EMAIL_ADDRESS", "EMAIL_PASSWORD", "EMAIL_TO"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

# === CONFIG ===
MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

# === VARIABILI DI CONFIGURAZIONE EMAIL ===
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.environ["EMAIL_ADDRESS"]
SMTP_PASSWORD = os.environ["EMAIL_PASSWORD"]
EMAIL_TO = os.environ["EMAIL_TO"]

# -------------------- Email helper --------------------
def send_mail(esito: str, messaggio: str, allegato_path: Optional[str] = None):
    subject = f"[PYTHON] - {esito}"
    body = f"{messaggio}\n\nOra: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    email = MIMEMultipart()
    email["From"] = SMTP_USER
    email["To"] = EMAIL_TO
    email["Subject"] = subject
    email.attach(MIMEText(body, "plain"))
    if allegato_path and os.path.exists(allegato_path):
        with open(allegato_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(allegato_path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(allegato_path)}"'
            email.attach(part)
    server = None
    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(email)
        print("✅ [MAIL] Mail inviata con successo.")
    except Exception as e:
        # Evita di stampare credenziali
        print(f"❌ [MAIL] Errore durante l'invio della mail: {e}")
    finally:
        if server:
            server.quit()

# -------------------- Metriche --------------------
def calcola_ev(prezzi: List[float]) -> float:
    """Media aritmetica dei prezzi carta."""
    return statistics.mean(prezzi) if prezzi else 0.0

def calcola_icv(prezzi_oltre_10: List[float]) -> float:
    """
    Indice di concentrazione del valore: somma delle top-3 / somma totale (sulle carte >10).
    Valore basso => valore distribuito; alto => valore concentrato.
    """
    if not prezzi_oltre_10:
        return 1.0
    top3 = sorted(prezzi_oltre_10, reverse=True)[:3]
    return sum(top3) / sum(prezzi_oltre_10)

def determina_tier(ev: float, icv: float, pct10: float, box_cost: Optional[float] = None) -> str:
    # Rapporto qualità/prezzo (fallback neutro se manca il box)
    ratio = (ev / box_cost) if (box_cost and box_cost > 0) else (ev / 200.0)

    # === S ===
    if ratio >= 0.26 and pct10 >= 25 and icv <= 0.55:       # S profondo (es. EB02)
        return "S"
    if ev >= 45 and ratio >= 0.14 and icv >= 0.80:          # S "chase" (es. OP05)
        return "S"

    # === A+ ===
    if ratio >= 0.24 and ev >= 30 and icv <= 0.90:          # OP09 / OP11
        return "A+"
    if ev >= 25 and pct10 >= 28 and icv <= 0.45:            # PRB01
        return "A+"
    if ev >= 20 and pct10 >= 15 and icv <= 0.60 and ratio >= 0.035:  # OP01
        return "A+"

    # === A ===
    if ratio >= 0.16 and pct10 >= 15 and icv <= 0.80:       # OP12
        return "A"
    if ratio >= 0.095 and pct10 >= 14 and icv <= 0.70:      # OP06 (OP07/OP10 restano fuori)
        return "A"
    if ratio >= 0.05 and ev >= 10 and pct10 >= 12 and icv <= 0.85:   # OP02, EB01, ecc.
        return "A"

    # === B ===
    if (ratio >= 0.06 and pct10 >= 13) or (ratio >= 0.045 and ev >= 8 and icv <= 0.70):
        return "B"

    # === C ===
    return "C"

def determina_tier_jp(ev: float, icv: float, pct10: float, box_cost: float | None = None) -> str:
    """
    Soglie tarate per JP dove i box costano meno e l'EV è più basso:
    - privilegio al ratio EV/Box
    - pct10 e icv restano correttivi (profondità e concentrazione)
    """

    # ratio solido solo se ho un box; se manca, usa un fallback prudente per non “boostare” set senza sealed
    ratio = (ev / box_cost) if (box_cost and box_cost > 0) else (ev / 120.0)

    # === S ===
    # S "profondo": EB02JP (ratio ~0.41, pct10 ~30%, icv ~0.49)
    if ratio >= 0.38 and pct10 >= 22 and icv <= 0.60:
        return "S"
    # NOTA: evito di fare S solo da ratio “estremo”, così OP13JP (~1.06) resta A+ e non S.

    # === A+ ===
    # A+ forte da ratio (OP09JP ~0.61), oppure ratio buono + EV decente (OP11JP ~0.43, EV~21)
    if ratio >= 0.60:
        return "A+"
    if ratio >= 0.40 and ev >= 18:
        return "A+"
    # A+ bilanciato (un po' di profondità e icv non eccessivo)
    if ev >= 22 and pct10 >= 12 and icv <= 0.80:
        return "A+"

    # === A ===
    # ratio buono ma non top (OP12JP ~0.30)
    if ratio >= 0.25:
        return "A"
    # alternative per set “onesti”
    if ev >= 12 and pct10 >= 10 and icv <= 0.85:
        return "A"

    # === B ===
    # dignitosi: ratio minimo o EV/pct10 minimi (OP01JP finisce qui)
    if ratio >= 0.12 or (ev >= 6 and pct10 >= 8):
        return "B"

    # === C ===
    return "C"

# -------------------- Prezzi --------------------
def pick_best_price(snap: Dict[str, Any] | None) -> Optional[float]:
    """Sceglie il miglior prezzo dallo snapshot Prices."""
    if not isinstance(snap, dict):
        return None
    for k in ("cmAvg7d", "cmAvg1d", "cmPriceTrend", "priceTcg", "priceUngraded"):
        v = snap.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None

# -------------------- Core builder --------------------
def build_tierlist(db, set_ids: list[str], market: str = "en") -> dict:
    tiers = {"S": [], "A+": [], "A": [], "B": [], "C": []}

    sets = list(
        db.Sets.find(
            {"id": {"$in": set_ids}},
            {"id": 1, "name": 1, "sealedId": 1}
        )
    )

    cards = db.Cards
    prices = db.Prices

    for s in sets:
        set_id = s.get("id")
        sealed_id = s.get("sealedId")
        if not set_id:
            continue

        # Costo box
        box_cost = None
        if sealed_id:
            latest_box = prices.find_one({"itemId": sealed_id}, sort=[("createdAt", -1)])
            box_cost = pick_best_price(latest_box)

        print(f"\n✅ [SET] {set_id}, sealedId: {sealed_id}")

        # Tutte le carte (non filtriamo più per rarità)
        card_list = list(cards.find({"setId": set_id, "type": "Cards"}, {"id": 1}))
        if not card_list:
            continue

        # Prezzi carte
        prezzi_pullabili: List[float] = []
        for card in card_list:
            cid = card["id"]
            latest = prices.find_one({"itemId": cid}, sort=[("createdAt", -1)])
            p = pick_best_price(latest)
            if p is not None:
                prezzi_pullabili.append(p)

        if not prezzi_pullabili:
            continue

        prezzi_oltre_10 = [p for p in prezzi_pullabili if p > 10]

        ev = calcola_ev(prezzi_pullabili)
        icv = calcola_icv(prezzi_oltre_10)
        pct10 = (len(prezzi_oltre_10) / len(prezzi_pullabili)) * 100.0
        ev_per_box = (ev / box_cost) if (box_cost and box_cost > 0) else None

        print(
            f"🔄 [SET] {set_id} - [EV] {ev:.2f} | [P10] {pct10:.2f} | [ICV] {icv:.2f} | "
            f"[Box] {box_cost if box_cost is not None else 'n/a'}"
            f"{' | [EV/Box] ' + f'{ev_per_box:.4f}' if ev_per_box is not None else ''}"
        )

        if market == "jp":
            tier = determina_tier_jp(ev, icv, pct10, box_cost)
        else:
            tier = determina_tier(ev, icv, pct10, box_cost)
            
        tiers[tier].append({"name": s["name"], "id": set_id})

    return tiers

# -------------------- Main --------------------
if __name__ == "__main__":
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGODB_DB]

        # --- Tierlist EN ---
        print(f"\n✅ [TIERLIST] GLOBAL")
        OP_IDS_EN = [
            "OP01", "OP02", "OP03", "OP04", "OP05", "OP06",
            "OP07", "OP08", "OP09", "OP10", "OP11", "OP12", 
            "OP13","OP14", "EB01", "EB02", "PRB01", "PRB02"
        ]
        tiers_en = build_tierlist(db, OP_IDS_EN, market= "en")
        db.Tierlist.delete_many({"language": "en"})
        db.Tierlist.insert_one({
            "date": datetime.now(timezone.utc),
            "language": "en",
            "tiers": tiers_en
        })

        # --- Tierlist JP ---
        print(f"\n✅ [TIERLIST] JAPANASE")
        OP_IDS_JP = [
            "OP01JP", "OP02JP", "OP03JP", "OP04JP", "OP05JP", "OP06JP",
            "OP07JP", "OP08JP", "OP09JP", "OP10JP", "OP11JP", "OP12JP",
            "OP13JP", "OP14JP","EB01JP", "EB02JP", "PRB01JP", "PRB02JP", "EB03JP"
        ]
        tiers_jp = build_tierlist(db, OP_IDS_JP, market= "jp")
        db.Tierlist.delete_many({"language": "jp"})
        db.Tierlist.insert_one({
            "date": datetime.now(timezone.utc),
            "language": "jp",
            "tiers": tiers_jp
        })

        print("\n✅ [END] Fine processo creazione tierlist.")
        send_mail("✅ TIERLIST COMPLETATA", "Tierlist EN/JP create con successo.")
    except Exception as e:
        send_mail("❌ ERRORE TIERLIST", traceback.format_exc())
