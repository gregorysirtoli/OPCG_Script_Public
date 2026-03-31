from __future__ import annotations
from dotenv import load_dotenv
from pymongo import MongoClient
from src.core.config import load_settings
from src.core.logging_setup import configure_logger
from src.metrics.getMarketData import update_cards_market_data
import time
from datetime import datetime
from src.core.emailer import send_email
import traceback

# =========================
# ENV & Logger
# =========================
load_dotenv(".env.local")
load_dotenv()
logger = configure_logger()

if __name__ == "__main__":
    try:
        # Timer start
        start_time = time.time()
        start_dt = datetime.now()

        print(f"🚀 Inizio esecuzione: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=== Start Ingestor ===")
        settings = load_settings()
        client = MongoClient(settings.mongodb_uri, tz_aware=True)
        db = client[settings.mongodb_db]

        ids = []

        touched, updated, sets_touched, sets_updated = update_cards_market_data(
            db,
            days_back=477,
            limit_ids=ids,
        )
        print(f"Cards: {touched}, updated: {updated}")
        print(f"Sets: {sets_touched}, updated: {sets_updated}")

        # Timer end
        end_time = time.time()
        end_dt = datetime.now()

        elapsed = end_time - start_time

        print(f"✅ Fine esecuzione: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Tempo totale: {elapsed:.2f} secondi")

        minutes = elapsed / 60.0
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)"
        )
        send_email("✅ [WORKFLOW] Market Data", body)
    except Exception:
        send_email("🚫 [WORKFLOW] Market Data", traceback.format_exc())
        raise
