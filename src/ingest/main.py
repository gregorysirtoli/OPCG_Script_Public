from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient

from src.core.config import ROME, load_settings
from src.core.emailer import send_email
from src.core.logging_setup import configure_logger
from src.core.utils import TokenBucket, eur_to_usd, get_fx_eur_usd

# Carica env da .env.local + .env se presenti
load_dotenv(".env.local")
load_dotenv()

logger = configure_logger()

# Rate limiter opzionale
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "1.2"))
BURST = int(os.getenv("BURST", "10"))
_bucket = TokenBucket(RATE_PER_SEC, BURST)


def now_rome() -> datetime:
    return datetime.now(ROME)


def load_provider_module(module_name: Optional[str]):
    if not module_name:
        from src.providers.mock import PROVIDERS  # fallback mock

        return PROVIDERS
    import importlib

    m = importlib.import_module(module_name)
    return getattr(m, "PROVIDERS")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--shard-total", type=int, default=1)
    return ap.parse_args()


def partition_ok(mongo_id: Any, shard_idx: int, shard_total: int) -> bool:
    # partizionamento semplice con hash dello _id in stringa
    h = hash(str(mongo_id))
    return (h % shard_total) == shard_idx


def main() -> int:
    settings = load_settings()
    args = parse_args()

    logger.info("=== Start Ingestor ===")

    # DB
    client = MongoClient(settings.mongodb_uri, tz_aware=True)
    db = client[settings.mongodb_db]
    coll_cards = db["Cards"]
    coll_prices = db["Prices_TEST"]
    coll_logs = db.get("Logs", None)

    # Provider loading
    # (aggiungi qui eventuale caricamento provider e logica di ingest)

    return 0


if __name__ == "__main__":
    sys.exit(main())
