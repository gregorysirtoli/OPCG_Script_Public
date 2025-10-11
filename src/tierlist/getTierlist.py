from datetime import datetime, timezone
from pymongo import MongoClient
from datetime import datetime, UTC
import statistics
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
import traceback

# Carica variabili da ambiente (.env o .env.local)
load_dotenv(".env.local")
load_dotenv()

# === CHECK ENV  ===
REQUIRED = ["MONGODB_URI", "MONGODB_DB", "EMAIL_ADDRESS", "EMAIL_PASSWORD", "EMAIL_TO"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

# === CONFIG ===
MONGO_URI = os.environ['MONGODB_URI'] 
MONGODB_DB = os.environ["MONGODB_DB"]
RARITY_PULLABLE = {"Rare","R","SR","SEC","L","S","DON!!"}

# === VARIABILI DI CONFIGURAZIONE EMAIL ===
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.environ['EMAIL_ADDRESS']
SMTP_PASSWORD = os.environ['EMAIL_PASSWORD']
EMAIL_TO = os.environ['EMAIL_TO']

# Funzione per invio e-mail
def send_mail(esito, messaggio, allegato_path=None):
    subject = f"[PYTHON] - {esito}"
    body = f"{messaggio}\n\nOra: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    email = MIMEMultipart()
    email["From"] = SMTP_USER
    email["To"] = EMAIL_TO
    email["Subject"] = subject
    email.attach(MIMEText(body, "plain"))
    if allegato_path and os.path.exists(allegato_path):
        with open(allegato_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(allegato_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(allegato_path)}"'
            email.attach(part)
    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(email)

        print(f"\n✅ [MAIL] Mail inviata con successo.")

    except Exception as e:
        print(f"\n❌ {SMTP_USER} - {SMTP_PASSWORD} [MAIL] Errore durante l'invio della mail: {e}")
    finally:
        server.quit()

# Calcolo Expected Value (EV)
def calcola_ev(prezzi):
    # Media artimetica semplice dei prezzi delle carte
    return statistics.mean(prezzi) if prezzi else 0.0

# Calcolo indice di Concentrazione del Valore (ICV)
def calcola_icv(prezzi_oltre_10):
    # Misura se il valore è concentrato in poche carte: se le 3 carte più care valgono il 75%+ del totale, è rischioso per un investitore.
    # Valore molto concentrato in poche carte; valore più basso significa che il valore è più distribuito tra molte carte.
    if not prezzi_oltre_10:
        return 1.0
    top3 = sorted(prezzi_oltre_10, reverse=True)[:3]
    return sum(top3) / sum(prezzi_oltre_10)

# Assegnazione tier
def determina_tier(ev, icv, pct10):
    # S
    if (ev >= 60 and icv <= 0.50 and pct10 >= 30): # S1: set ricco e profondo (es. EB02)
        return "S"
    if (ev >= 85 and pct10 >= 20 and icv >= 0.80): # S2: valore enorme ma concentrato (es. OP05)
        return "S"
    # A+
    if (ev >= 40 and pct10 >= 25 and icv <= 0.80): # A+1: ricco, profondo, non troppo concentrato
        return "A+"
    if (ev >= 60 and pct10 >= 25): # A+2: molto ricco, profondità ok (ignora ICV)
        return "A+"
    # A
    if (20 <= ev < 40 and pct10 >= 20): # A1: EV medio con buona profondità
        return "A"
    if (25 <= ev < 50 and 0.70 <= icv <= 0.90): # A2: EV medio-alto ma abbastanza concentrato
        return "A"
    # B
    if (12 <= ev < 25 and pct10 >= 20): # B1: EV moderato ma con un po' di profondità
        return "B"
    # C ---
    return "C"

# Seleziona il miglior prezzo disponibile da uno snapshot di prezzi
def pick_best_price(snap: dict) -> float | None:
    # Ordine di preferenza
    if not isinstance(snap, dict):
        return None
    for k in ("cmAvg7d", "cmAvg1d", "priceTcg", "priceUngraded"):
        v = snap.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None

# === AVVIO ===
try:
    client = MongoClient(MONGO_URI)
    db = client[MONGODB_DB]

    # Selezione dei set validi
    sets = list(db.Sets.find({ "tierList": { "$ne": False } }, { "id": 1, "name": 1, "logo": 1 }))
    cards = db.Cards
    prices = db.Prices

    tiers = { "S": [], "A+": [], "A": [], "B": [], "C": [] }

    for s in sets:
        setId = s.get("id")
        if not setId:
            continue

        print(f"\n✅ [SET] {setId}")

        # Filtra solo le carte dei set validi
        card_list = list(cards.find({ "setId": setId, "rarityId": { "$in": list(RARITY_PULLABLE) } }, { "id": 1 }))
        if not card_list:
            continue

        prezzi_pullabili = []
        for card in card_list:
            card_id = card["id"]

            # Recupero prezzi raw più recenti
            latest_price = prices.find_one({ "itemId": card_id }, sort=[("createdAt", -1)])
            prezzo = pick_best_price(latest_price)
            if prezzo is not None:
                prezzi_pullabili.append(prezzo)

        if not prezzi_pullabili:
            continue

        prezzi_oltre_10 = [p for p in prezzi_pullabili if p > 10]

        # Calcolo Expected Value (EV)
        ev = calcola_ev(prezzi_pullabili)
        print(f"🔄 [SET] {setId} - [EV] {ev}")

        # Calcolo indice di Concentrazione del Valore (ICV)
        icv = calcola_icv(prezzi_oltre_10)
        print(f"🔄 [SET] {setId} - [ICV] {icv}")

        # Calcolo percentuale delle carte sopra i 10.00 € (considerate pull)
        perc_10 = (len(prezzi_oltre_10) / len(prezzi_pullabili)) * 100
        print(f"🔄 [SET] {setId} - [%] {perc_10}")

        # Assegnazione tier
        tier = determina_tier(ev, icv, perc_10)

        tiers[tier].append({
            "name": s["name"],
            "id": s["id"],
            "logo": s.get("logo")
        })

    # === Salvataggio Tierlist ===
    db.Tierlist.delete_many({})
    db.Tierlist.insert_one({
        "date": datetime.now(timezone.utc),
        "tiers": tiers
    })

    print(f"\n✅ [END] Fine processo creazione tierlist.")

    send_mail("✅ TIERLIST COMPLETATA (MONGODB)", "Tierlist completata con successo.")
except Exception as e:
    send_mail("❌ ERRORE TIERLIST (MONGODB)", traceback.format_exc())