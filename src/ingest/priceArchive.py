"""
Archiviazione della collection Prices: sposta le righe piu' vecchie di un anno verso
un database di archivio dedicato (MONGODB_URI_ARCHIVE/MONGODB_DB_ARCHIVE), in collection
"Prices_<anno>" basate sull'anno di createdAt della riga.

Va eseguita PRIMA dell'ingest vero e proprio (vedi src/ingest/main.py).

Ogni riga viene prima copiata nell'archivio e solo DOPO cancellata dalla sorgente. Se un
run viene interrotto a meta' (rete, timeout, ecc.), il run successivo ritrova le stesse
righe ancora in Prices e le ricopia: gli inserimenti duplicati nell'archivio (stesso _id,
stesso documento) vengono ignorati come no-op, non come errore.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pymongo.database import Database
from pymongo.errors import AutoReconnect, BulkWriteError, PyMongoError

RETENTION_DAYS = 365
ARCHIVE_BATCH_SIZE = 2000
ARCHIVE_RETRIES = 3
ARCHIVE_RETRY_SLEEP_SECONDS = 5

DUPLICATE_KEY_ERROR_CODE = 11000


def _ensure_archive_collection(archive_db: Database, collection_name: str) -> None:
    """
    Crea la collection di archivio come time series, solo se non esiste gia'. Stessa
    logica/motivazione di CardsStampChanges (src/scripts/backfill_stamp_changes.py):
    timeField=createdAt (obbligatorio, e' un evento datato), metaField=itemId (raggruppa
    lo storico della stessa carta nello stesso bucket, coerente con come Prices viene
    gia' interrogata ovunque nel resto del codice: {"itemId": ..., "createdAt": ...}),
    granularity="hours" (l'ingest scrive al massimo un documento al giorno per carta).
    """
    if collection_name in archive_db.list_collection_names():
        return
    archive_db.create_collection(
        collection_name,
        timeseries={
            "timeField": "createdAt",
            "metaField": "itemId",
            "granularity": "hours",
        },
    )
    archive_db[collection_name].create_index([("itemId", 1), ("createdAt", 1)])


def _is_only_duplicate_key_errors(bwe: BulkWriteError) -> bool:
    write_errors = bwe.details.get("writeErrors", []) if bwe.details else []
    return bool(write_errors) and all(e.get("code") == DUPLICATE_KEY_ERROR_CODE for e in write_errors)


def _insert_with_retry(collection, docs: List[Dict[str, Any]], logger: logging.Logger) -> None:
    for attempt in range(1, ARCHIVE_RETRIES + 1):
        try:
            collection.insert_many(docs, ordered=False)
            return
        except BulkWriteError as bwe:
            if _is_only_duplicate_key_errors(bwe):
                # Tutte le righe risultano gia' presenti nell'archivio: run precedente
                # interrotto dopo la copia ma prima della cancellazione dalla sorgente.
                # Va bene cosi', si procede comunque a cancellare dalla sorgente.
                return
            if attempt == ARCHIVE_RETRIES:
                raise
            logger.warning(
                "Insert su archivio Prices fallito (tentativo %d/%d): %s",
                attempt, ARCHIVE_RETRIES, bwe,
            )
            time.sleep(ARCHIVE_RETRY_SLEEP_SECONDS)
        except (AutoReconnect, PyMongoError) as exc:
            if attempt == ARCHIVE_RETRIES:
                raise
            logger.warning(
                "Insert su archivio Prices fallito (tentativo %d/%d): %s",
                attempt, ARCHIVE_RETRIES, exc,
            )
            time.sleep(ARCHIVE_RETRY_SLEEP_SECONDS)


def archive_old_prices(
    db: Database,
    archive_db: Optional[Database],
    logger: logging.Logger,
    retention_days: int = RETENTION_DAYS,
) -> int:
    """
    Sposta su archive_db le righe di Prices con createdAt piu' vecchio di
    retention_days, raggruppandole per collection "Prices_<anno di createdAt>".
    Ritorna il numero totale di righe spostate.
    """
    if archive_db is None:
        logger.info(
            "Archiviazione storica Prices saltata: MONGODB_URI_ARCHIVE/MONGODB_DB_ARCHIVE "
            "non configurati per questo gioco."
        )
        return 0

    coll_prices = db["Prices"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    has_rows_to_move = coll_prices.find_one({"createdAt": {"$lt": cutoff}}, {"_id": 1}) is not None
    if not has_rows_to_move:
        logger.info("Archiviazione Prices: nessuna riga piu' vecchia di %d giorni, nulla da fare.", retention_days)
        return 0

    total_moved = 0
    while True:
        batch = list(
            coll_prices.find({"createdAt": {"$lt": cutoff}})
            .sort("createdAt", 1)
            .limit(ARCHIVE_BATCH_SIZE)
        )
        if not batch:
            break

        by_year: Dict[int, List[Dict[str, Any]]] = {}
        for doc in batch:
            year = doc["createdAt"].year
            by_year.setdefault(year, []).append(doc)

        for year, docs in by_year.items():
            collection_name = f"Prices_{year}"
            _ensure_archive_collection(archive_db, collection_name)
            _insert_with_retry(archive_db[collection_name], docs, logger)

        ids_to_delete = [doc["_id"] for doc in batch]
        coll_prices.delete_many({"_id": {"$in": ids_to_delete}})

        total_moved += len(batch)
        logger.info(
            "Archiviazione Prices: %d righe spostate finora (cutoff=%s).",
            total_moved, cutoff.date(),
        )

        if len(batch) < ARCHIVE_BATCH_SIZE:
            break

    logger.info("Archiviazione Prices completata: %d righe totali spostate su archivio.", total_moved)
    return total_moved
