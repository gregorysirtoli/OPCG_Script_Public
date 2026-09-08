from datetime import datetime, timezone
from pymongo import MongoClient
import statistics
from dotenv import load_dotenv
import os
import traceback
from typing import Optional, Dict, List, Any, Tuple
from src.core.emailer import send_email

# ============================================================
# ENV
# ============================================================
load_dotenv(".env.local")
load_dotenv()

REQUIRED = ["MONGODB_URI", "MONGODB_DB"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

MONGO_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ["MONGODB_DB"]

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

    for k in (
        "price",
        "priceTrend",
        "price7d",
        "price30d",
        "price90d",
        "price1d",
        "priceSecondary",
        "pricePrimary",
        "priceCardTrader",
    ):
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


def get_set_image(
    db,
    set_doc: Dict[str, Any],
    fallback_sealed_id: Optional[str],
    sealed_image_cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """
    Restituisce l'immagine del set con priorita:
    1) Sets.image
    2) Cards.localImage cercando la card con id == sealedId
    """
    set_image = str(set_doc.get("image") or "").strip()
    if set_image:
        return set_image

    if not fallback_sealed_id:
        return None

    if fallback_sealed_id in sealed_image_cache:
        return sealed_image_cache[fallback_sealed_id]

    card_doc = db.Cards.find_one({"id": fallback_sealed_id}, {"localImage": 1})
    local_image = str((card_doc or {}).get("localImage") or "").strip()
    value = local_image or None
    sealed_image_cache[fallback_sealed_id] = value
    return value


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


def rebalance_tiers_min_one(
    tier_entries: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Prova a garantire almeno 1 elemento per tier quando il numero totale di set lo consente.
    Sposta set dai tier con piu elementi verso tier vuoti scegliendo il candidato
    con test_ratio piu vicino al centro della fascia target.
    """
    order = ["S", "A+", "A", "B", "C"]
    centers = {
        "S": 0.35,
        "A+": 0.25,
        "A": 0.16,
        "B": 0.09,
        "C": 0.03,
    }

    total_items = sum(len(tier_entries.get(t, [])) for t in order)
    if total_items < len(order):
        return {
            t: [row["item"] for row in tier_entries.get(t, [])]
            for t in order
        }

    for target_tier in order:
        if tier_entries.get(target_tier):
            continue

        best_donor_tier: Optional[str] = None
        best_idx = -1
        best_distance = float("inf")
        target_center = centers[target_tier]

        for donor_tier in order:
            donor_rows = tier_entries.get(donor_tier, [])
            if len(donor_rows) <= 1:
                continue

            for idx, row in enumerate(donor_rows):
                ratio = float(row.get("test_ratio") or 0.0)
                distance = abs(ratio - target_center)
                if distance < best_distance:
                    best_distance = distance
                    best_donor_tier = donor_tier
                    best_idx = idx

        if best_donor_tier is None or best_idx < 0:
            continue

        moved = tier_entries[best_donor_tier].pop(best_idx)
        tier_entries[target_tier].append(moved)

    return {
        t: [row["item"] for row in tier_entries.get(t, [])]
        for t in order
    }


def detect_market_from_label(label: str) -> str:
    """
    Estrae il market dal label della tierlist, es. "STARTER DECK (EN)" -> "en".
    Fallback: "global".
    """
    s = (label or "").strip()
    if "(" in s and ")" in s and s.rfind("(") < s.rfind(")"):
        market = s[s.rfind("(") + 1 : s.rfind(")")].strip().lower()
        if market:
            return market
    return "global"


def get_set_ids_for_tierlist(db, tierlist_id: Any) -> List[str]:
    """
    Recupera dinamicamente tutti i set associati alla tierlist tramite Sets.tierListIds._id.
    """
    docs = list(
        db.Sets.find(
            {"tierListIds._id": tierlist_id},
            {"id": 1},
        )
    )
    out: List[str] = []
    for d in docs:
        sid = str(d.get("id") or "").strip()
        if sid:
            out.append(sid)
    return out


# ============================================================
# CORE BUILDER (tiers + report_rows)
# ============================================================
def build_tierlist(
    db,
    set_ids: List[str],
    market: str = "en",
    only_visible: bool = True
) -> Tuple[dict, List[dict]]:
    tiers = {"S": [], "A+": [], "A": [], "B": [], "C": []}
    tier_entries: Dict[str, List[Dict[str, Any]]] = {"S": [], "A+": [], "A": [], "B": [], "C": []}
    report_rows: List[dict] = []

    # buffer print ordinato
    print_buffer = {"S": [], "A+": [], "A": [], "B": [], "C": []}

    sets = list(
        db.Sets.find(
            {"id": {"$in": set_ids}},
            {"id": 1, "name": 1, "sealedId": 1, "image": 1}
        )
    )
    sets_by_id = {
        str(s.get("id") or ""): s
        for s in sets
        if str(s.get("id") or "")
    }

    cards_col = db.Cards
    sealed_image_cache: Dict[str, Optional[str]] = {}

    for s in sets:
        set_id = s.get("id")
        set_name = s.get("name")
        if not set_id:
            continue

        sealed_id = s.get("sealedId")

        # Costo box (da Prices, default 200)
        box_cost = get_box_cost(db, sealed_id, default_cost=200.0)

        # Query carte
        q = {"setId": set_id, "type": "Cards"}
        if only_visible:
            q["visible"] = True

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
        tier_item = {"name": set_name, "id": set_id}
        set_image = get_set_image(db, s, sealed_id, sealed_image_cache)
        if set_image:
            tier_item["image"] = set_image
        tier_entries[tier].append({
            "item": tier_item,
            "test_ratio": test_ratio,
        })

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

    tiers = rebalance_tiers_min_one(tier_entries)
    return tiers, report_rows


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    try:
        GAME = os.getenv("GAME") or "N/A"
        client = MongoClient(MONGO_URI)
        db = client[MONGODB_DB]

        tierlists = list(
            db.Tierlist.find(
                {},
                {"_id": 1, "label": 1},
            )
        )

        if not tierlists:
            print("\n[WARN] Nessuna tierlist trovata in collection Tierlist.")

        for tl in tierlists:
            tl_id = tl.get("_id")
            label = str(tl.get("label") or "").strip()
            market = detect_market_from_label(label)

            if not tl_id:
                continue

            set_ids = get_set_ids_for_tierlist(db, tl_id)

            if not set_ids:
                empty_tiers = {"S": [], "A+": [], "A": [], "B": [], "C": []}
                db.Tierlist.update_one(
                    {"_id": tl_id},
                    {
                        "$set": {
                            "tiers": empty_tiers,
                            "updatedAt": datetime.now(timezone.utc),
                        }
                    },
                )
                print(f"\n[WARN] Tierlist '{label}' senza set associati. Salvata tiers vuota.")
                continue

            tiers, _report = build_tierlist(db, set_ids, market=market, only_visible=True)

            # Salvataggio: tiers come elemento del documento anagrafico Tierlist.
            db.Tierlist.update_one(
                {"_id": tl_id},
                {
                    "$set": {
                        "tiers": tiers,
                        "updatedAt": datetime.now(timezone.utc),
                    }
                },
            )

            print(f"\n[OK] Tierlist aggiornata: {label} | sets={len(set_ids)}")

        print("\n✅ [END] Fine processo creazione tierlist.")
        send_email("✅ [3/5][WORKFLOW] Tierlist " + GAME, "", "")

    except Exception:
        send_email("🚫 [3/5][WORKFLOW] Tierlist " + GAME, traceback.format_exc(), "")
        raise
