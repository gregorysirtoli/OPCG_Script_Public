from __future__ import annotations

from pymongo import MongoClient, UpdateOne
import pandas as pd

def get_db(uri: str, db_name: str):
    client = MongoClient(uri)
    return client[db_name]

def load_collection(db, col_name: str, match: dict | None = None, projection: dict | None = None) -> pd.DataFrame:
    q = match or {}
    cursor = db[col_name].find(q, projection)
    docs = list(cursor)
    return pd.DataFrame(docs)

def upsert_many(db, col_name: str, docs: list[dict], key_fields: list[str]):
    if not docs:
        return
    ops = []
    for d in docs:
        filt = {k: d[k] for k in key_fields}
        ops.append(UpdateOne(filt, {"$set": d}, upsert=True))
    db[col_name].bulk_write(ops, ordered=False)
