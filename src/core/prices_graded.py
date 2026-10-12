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


def attach_graded(
    db: Database,
    docs: List[Dict[str, Any]],
    query: Dict[str, Any],
    fields: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Riattacca ai documenti di Prices (letti con `query`) i campi PriceCharting di PricesGraded.
    `query` e' lo stesso filtro usato su Prices (itemId/createdAt): su PricesGraded usa
    l'indice metaField+timeField. I documenti devono contenere _id. Modifica docs in place.
    """
    wanted = _requested_fields(fields)
    if not docs or not wanted:
        return docs
    by_id = {doc["_id"]: doc for doc in docs if "_id" in doc}
    if not by_id:
        return docs
    projection = {field: 1 for field in wanted}
    for graded in db[PRICES_GRADED_COLLECTION].find(query, projection):
        doc = by_id.get(graded.get("_id"))
        if doc is None:
            continue
        for field in wanted:
            if field in graded:
                doc[field] = graded[field]
    return docs


def attach_graded_df(db: Database, prices_df, query: Dict[str, Any], fields: Optional[Iterable[str]] = None):
    """Come attach_graded, per un DataFrame di Prices con la colonna _id."""
    import pandas as pd

    wanted = _requested_fields(fields)
    if prices_df.empty or "_id" not in prices_df.columns or not wanted:
        return prices_df
    graded = pd.DataFrame(list(db[PRICES_GRADED_COLLECTION].find(query, {field: 1 for field in wanted})))
    if graded.empty:
        return prices_df
    graded = graded.drop_duplicates(subset="_id", keep="last").set_index("_id")
    out = prices_df.copy()
    keys = out["_id"]
    for field in wanted:
        if field not in graded.columns:
            continue
        values = keys.map(graded[field])
        out[field] = values.combine_first(out[field]) if field in out.columns else values
    return out
