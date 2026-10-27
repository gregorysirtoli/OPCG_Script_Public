"""
PricesGraded: time series con i campi PriceCharting (grading + prezzi PriceCharting),
tenuti fuori da Prices.

Ogni riga di PricesGraded ha lo stesso _id, createdAt e itemId (= Cards.id) della riga di
Prices scritta nello stesso momento: chi legge Prices e ha bisogno di quei campi li
riattacca con attach_graded/attach_graded_df, ottenendo esattamente i documenti di prima.
Le righe di PricesGraded senza una riga di Prices corrispondente (ad esempio perche' quella
di Prices e' gia' stata archiviata) vengono ignorate.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from bson import ObjectId
from pymongo.database import Database

PRICES_GRADED_COLLECTION = "PricesGraded"

# Campi scritti in Prices da PriceCharting (SECONDARY_LABEL_MAP + prezzo principale).
PRICECHARTING_FIELDS: List[str] = [
    "pricePriceCharting",
    "priceUngraded",
    "priceGrade1", "priceGrade2", "priceGrade3", "priceGrade4", "priceGrade5",
    "priceGrade6", "priceGrade7", "priceGrade8", "priceGrade9", "priceGrade95",
    "psa10",
    "bsg10", "bsg10black",
    "cgc10", "cgc10pristine",
    "sgc10",
    "tag10",
    "ace10",
]

# Campi della riga di Prices copiati anche nella riga di PricesGraded, per collegarle.
GRADED_LINK_FIELDS: List[str] = ["_id", "createdAt", "itemId", "executionFingerprint", "weekNumber"]


def ensure_prices_graded_collection(db: Database) -> None:
    """Crea PricesGraded come time series se non esiste (un insert la creerebbe normale)."""
    if PRICES_GRADED_COLLECTION in db.list_collection_names():
        return
    db.create_collection(
        PRICES_GRADED_COLLECTION,
        timeseries={"timeField": "createdAt", "metaField": "itemId", "granularity": "hours"},
    )


def split_graded_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Toglie da row i campi PriceCharting e li restituisce come riga di PricesGraded
    (None se row non ne ha). Assegna a row un _id, se manca, cosi' le due righe lo condividono.
    """
    graded_values = {field: row.pop(field) for field in PRICECHARTING_FIELDS if field in row}
    if not graded_values:
        return None
    row.setdefault("_id", ObjectId())
    graded = {field: row[field] for field in GRADED_LINK_FIELDS if field in row}
    graded.update(graded_values)
    return graded


def _requested_fields(fields: Optional[Iterable[str]]) -> List[str]:
    return [f for f in (fields or PRICECHARTING_FIELDS) if f in PRICECHARTING_FIELDS]


def _day_key(item_id: Any, created_at: Any) -> Optional[tuple]:
    if item_id is None or not hasattr(created_at, "date"):
        return None
    return (item_id, created_at.date())


def _seconds_apart(a: Any, b: Any) -> float:
    try:
        return abs((a - b).total_seconds())
    except Exception:
        return float("inf")


def _match_graded(
    prices_rows: List[Dict[str, Any]],
    graded_rows: List[Dict[str, Any]],
    default_item_id: Any = None,
) -> Dict[Any, Dict[str, Any]]:
    """
    Abbina le righe di PricesGraded alle righe di Prices; restituisce {_id riga Prices: riga graded}.
    1. stesso _id (righe scritte dall'ingest insieme alla riga di Prices);
    2. per le righe graded senza riga di Prices con lo stesso _id (scritte da altri script):
       la riga di Prices della stessa carta nello stesso giorno (UTC) con createdAt piu' vicino,
       se quella riga non ha gia' la sua riga graded per _id. Con piu' righe graded nello stesso
       giorno vince la piu' recente.
    prices_rows: dict con _id, createdAt e itemId (se manca si usa default_item_id).
    """
    prices_ids = {row["_id"] for row in prices_rows if "_id" in row}
    matched: Dict[Any, Dict[str, Any]] = {}
    unlinked: List[Dict[str, Any]] = []
    for graded in graded_rows:
        if graded.get("_id") in prices_ids:
            matched[graded["_id"]] = graded
        else:
            unlinked.append(graded)
    if not unlinked:
        return matched

    by_day: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in prices_rows:
        if "_id" not in row or row["_id"] in matched:
            continue
        key = _day_key(row.get("itemId", default_item_id), row.get("createdAt"))
        if key is not None:
            by_day.setdefault(key, []).append(row)

    fallback: Dict[Any, Dict[str, Any]] = {}
    for graded in sorted(unlinked, key=lambda g: (g.get("createdAt") is None, g.get("createdAt"))):
        candidates = by_day.get(_day_key(graded.get("itemId"), graded.get("createdAt")))
        if not candidates:
            continue
        target = min(candidates, key=lambda row: _seconds_apart(row.get("createdAt"), graded.get("createdAt")))
        fallback[target["_id"]] = graded  # in ordine di createdAt: la piu' recente sovrascrive
    matched.update(fallback)
    return matched


def latest_pricecharting(db: Database, item_id: Any) -> Optional[float]:
    """
    Ultimo pricePriceCharting > 0 della carta in PricesGraded (None se non c'e'). All'ingest serve
    a riportarlo nella riga del giorno quando lo scraping non l'ha preso, come fa
    _carry_forward_pricecharting di getMarketData, cosi' Prices.priceRedLine lo considera.
    """
    doc = db[PRICES_GRADED_COLLECTION].find_one(
        {"itemId": item_id, "pricePriceCharting": {"$gt": 0}},
        {"pricePriceCharting": 1},
        sort=[("createdAt", -1)],
    )
    return doc.get("pricePriceCharting") if doc else None


def _load_graded(db: Database, query: Dict[str, Any], wanted: List[str]) -> List[Dict[str, Any]]:
    projection = {field: 1 for field in wanted}
    projection.update({"itemId": 1, "createdAt": 1})
    return list(db[PRICES_GRADED_COLLECTION].find(query, projection))


def attach_graded(
    db: Database,
    docs: List[Dict[str, Any]],
    query: Dict[str, Any],
    fields: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Riattacca ai documenti di Prices (letti con `query`) i campi PriceCharting di PricesGraded
    (abbinamento: vedi _match_graded). `query` e' lo stesso filtro usato su Prices
    (itemId/createdAt): su PricesGraded usa l'indice metaField+timeField.
    I documenti devono contenere _id e createdAt. Modifica docs in place.
    """
    wanted = _requested_fields(fields)
    if not docs or not wanted:
        return docs
    by_id = {doc["_id"]: doc for doc in docs if "_id" in doc}
    if not by_id:
        return docs
    default_item_id = query.get("itemId") if isinstance(query.get("itemId"), str) else None
    for prices_id, graded in _match_graded(docs, _load_graded(db, query, wanted), default_item_id).items():
        doc = by_id[prices_id]
        for field in wanted:
            if field in graded:
                doc[field] = graded[field]
    return docs


def attach_graded_df(db: Database, prices_df, query: Dict[str, Any], fields: Optional[Iterable[str]] = None):
    """Come attach_graded, per un DataFrame di Prices con le colonne _id, itemId e createdAt."""
    import pandas as pd

    wanted = _requested_fields(fields)
    if prices_df.empty or "_id" not in prices_df.columns or not wanted:
        return prices_df
    graded_rows = _load_graded(db, query, wanted)
    if not graded_rows:
        return prices_df
    link_cols = [c for c in ("_id", "itemId", "createdAt") if c in prices_df.columns]
    prices_rows = prices_df[link_cols].to_dict("records")
    for row in prices_rows:
        if "createdAt" in row and hasattr(row["createdAt"], "to_pydatetime"):
            row["createdAt"] = row["createdAt"].to_pydatetime()
    matched = _match_graded(prices_rows, graded_rows)
    if not matched:
        return prices_df
    out = prices_df.copy()
    for field in wanted:
        values = out["_id"].map({pid: g.get(field) for pid, g in matched.items() if field in g})
        out[field] = values.combine_first(out[field]) if field in out.columns else values
    return out
