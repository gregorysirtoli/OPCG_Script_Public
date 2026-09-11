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
# TIER: SEMPRE QUANTILE
# ============================================================
QUANTILE_THRESHOLDS: Tuple[float, float, float, float] = (0.10, 0.25, 0.45, 0.75)


def tier_by_rank_quantile(rank_idx: int, total: int, thresholds: Tuple[float, float, float, float]) -> str:
    if total <= 0:
        return "C"

    s_q, ap_q, a_q, b_q = thresholds
    pct = (rank_idx + 1) / total
    if pct <= s_q:
        return "S"
    if pct <= ap_q:
        return "A+"
    if pct <= a_q:
        return "A"
    if pct <= b_q:
        return "B"
    return "C"


def rebalance_tiers_min_one(
    tier_entries: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Prova a garantire almeno 1 elemento per tier minimizzando salti incoerenti.
    Usa movimenti adiacenti a passi (es. B->A, poi A->A+, poi A+->S),
    evitando spostamenti diretti lunghi come B->S.
    """
    order = ["S", "A+", "A", "B", "C"]
    centers = {
        "S": 0.35,
        "A+": 0.25,
        "A": 0.16,
        "B": 0.09,
        "C": 0.03,
    }

    def pop_best_row(rows: List[Dict[str, Any]], target_center: float) -> Dict[str, Any]:
        best_idx = 0
        best_distance = float("inf")
        for idx, row in enumerate(rows):
            ratio = float(row.get("test_ratio") or 0.0)
            distance = abs(ratio - target_center)
            if distance < best_distance:
                best_distance = distance
                best_idx = idx
        return rows.pop(best_idx)

    # Copia difensiva della struttura
    work: Dict[str, List[Dict[str, Any]]] = {
        t: list(tier_entries.get(t, []))
        for t in order
    }

    # Finche esistono tier vuoti, prova a riempirli con passi adiacenti.
    # Esegue sia passata top-down che bottom-up per gestire vuoti in qualsiasi posizione.
    max_iters = 500
    for _ in range(max_iters):
        empty_exists = any(len(work[t]) == 0 for t in order)
        if not empty_exists:
            break

        moved_any = False

        # Top-down: porta massa verso l'alto con passi adiacenti.
        for target_idx in range(0, len(order) - 1):
            target_tier = order[target_idx]
            if work[target_tier]:
                continue

            donor_idx = None
            for j in range(target_idx + 1, len(order)):
                if len(work[order[j]]) > 1:
                    donor_idx = j
                    break
            if donor_idx is None:
                continue

            donor_tier = order[donor_idx]
            receiver_tier = order[donor_idx - 1]
            row = pop_best_row(work[donor_tier], centers[receiver_tier])
            work[receiver_tier].append(row)
            moved_any = True

        # Bottom-up: porta massa verso il basso con passi adiacenti.
        for target_idx in range(len(order) - 1, 0, -1):
            target_tier = order[target_idx]
            if work[target_tier]:
                continue

            donor_idx = None
            for j in range(target_idx - 1, -1, -1):
                if len(work[order[j]]) > 1:
                    donor_idx = j
                    break
            if donor_idx is None:
                continue

            donor_tier = order[donor_idx]
            receiver_tier = order[donor_idx + 1]
            row = pop_best_row(work[donor_tier], centers[receiver_tier])
            work[receiver_tier].append(row)
            moved_any = True

        if not moved_any:
            break

    out: Dict[str, List[Dict[str, Any]]] = {
        t: [row["item"] for row in work.get(t, [])]
        for t in order
    }

    return out


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
    only_visible: bool = True,
) -> Tuple[dict, List[dict]]:
    tiers = {"S": [], "A+": [], "A": [], "B": [], "C": []}
    tier_entries: Dict[str, List[Dict[str, Any]]] = {"S": [], "A+": [], "A": [], "B": [], "C": []}
    report_rows: List[dict] = []
    scored_entries: List[Dict[str, Any]] = []

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

        # Tier calcolato in seconda fase: quantile.
        test_ratio = (ev / box_cost) if (box_cost and box_cost > 0) else 0.0

        tier_item = {"name": set_name, "id": set_id}
        set_image = get_set_image(db, s, sealed_id, sealed_image_cache)
        if set_image:
            tier_item["image"] = set_image

        # report dettagliato
        row = {
            "setId": set_id,
            "setName": set_name,
            "market": market,
            "tier": "",
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

        scored_entries.append(
            {
                "tier_item": tier_item,
                "test_ratio": test_ratio,
                "row": row,
                "print_data": {
                    "set_id": set_id,
                    "total_cards": total_cards,
                    "priced_cards": priced_cards,
                    "missing_price": missing_price,
                    "sum_prices": sum_prices,
                    "ev": ev,
                    "median": median,
                    "pct10": pct10,
                    "icv": icv,
                    "box_cost": box_cost,
                    "test_ratio": test_ratio,
                },
            }
        )

    if scored_entries:
        ranked = sorted(scored_entries, key=lambda x: float(x["test_ratio"]), reverse=True)
        for idx, entry in enumerate(ranked):
            entry["tier"] = tier_by_rank_quantile(idx, len(ranked), QUANTILE_THRESHOLDS)

    for entry in scored_entries:
        tier = str(entry["tier"])
        tier_entries[tier].append(
            {
                "item": entry["tier_item"],
                "test_ratio": entry["test_ratio"],
            }
        )

        row = entry["row"]
        row["tier"] = tier
        report_rows.append(row)

        pdata = entry["print_data"]
        print_buffer[tier].append(
            f"[{market.upper()}] {pdata['set_id']} | cards={pdata['total_cards']} priced={pdata['priced_cards']} miss={pdata['missing_price']} | "
            f"sum_prices={pdata['sum_prices']:.2f} EV={pdata['ev']:.2f} median={pdata['median']:.2f} pct>10={pdata['pct10']:.1f}% ICV={pdata['icv']:.2f} "
            f"box={pdata['box_cost']:.0f} test_ratio={pdata['test_ratio']:.4f}| tier={tier}"
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

            tiers, _report = build_tierlist(
                db,
                set_ids,
                market=market,
                only_visible=True,
            )

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
