from __future__ import annotations
from dotenv import load_dotenv
from pymongo import MongoClient
from src.core.config import load_settings
from src.core.logging_setup import configure_logger
from src.metrics.getMarketData import update_cards_market_data

# =========================
# ENV & Logger
# =========================
load_dotenv(".env.local")
load_dotenv()
logger = configure_logger()

if __name__ == "__main__":
    logger.info("=== Start Ingestor ===")
    settings = load_settings()
    client = MongoClient(settings.mongodb_uri, tz_aware=True)
    db = client[settings.mongodb_db]

    ids = []

    touched, updated = update_cards_market_data(db, days_back=45, limit_ids=ids)
    print(f"Cards: {touched}, updated: {updated}")
