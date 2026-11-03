"""
Post Instagram del lunedi', a rotazione di quattro settimane (generateSealedTierlist.monday_post):
Market Movers pump, Market Insight promo, Market Movers dump, Market Insight promo.

    python -m src.instagram.monday_rotation

Env MONDAY_POST = auto | pump | dump | promo per forzare il post (auto o vuoto = calendario).
"""
import os
import sys
from pathlib import Path
import traceback
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from src.core.emailer import send_email

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

POSTS = ("pump", "dump", "promo")
POST_NAMES = {"pump": "Market Movers (pump)", "dump": "Market Movers (dump)", "promo": "Market Insight (promo)"}


def resolve_post() -> str:
    from private_providers.instagram import generateSealedTierlist as calendar

    requested = os.getenv("MONDAY_POST", "").strip().lower()
    if requested and requested != "auto":
        if requested not in POSTS:
            raise RuntimeError(f"MONDAY_POST must be one of auto, {', '.join(POSTS)} (got {requested!r})")
        return requested
    return calendar.monday_post(datetime.now(ZoneInfo("Europe/Rome")).date())


def generate_monday_post(post: str) -> str:
    from private_providers.instagram import generateMarketInsight as market_insight_gen
    from private_providers.instagram import generateMarketMovers as market_movers_gen

    print(f"[Orchestrator] Starting Monday post generation: {POST_NAMES[post]}")

    # su GitHub MONGODB_URI/MONGODB_DB; in locale anche le variabili di One Piece di .env.local
    # (i provider leggono MONGODB_URI/MONGODB_DB, quindi si impostano quelle)
    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGODB_URI_ONEPIECE")
    db_name = os.getenv("MONGODB_DB") or os.getenv("MONGODB_DB_ONEPIECE")
    if not mongo_uri or not db_name:
        raise RuntimeError("MONGODB_URI/MONGODB_DB (or MONGODB_URI_ONEPIECE/MONGODB_DB_ONEPIECE) required")
    os.environ["MONGODB_URI"] = mongo_uri
    os.environ["MONGODB_DB"] = db_name
    # DB vendite per report PSA, vendite e cambi di stamp del Market Insight
    sales_uri = os.getenv("MONGODB_SALES_URI") or os.getenv("MONGODB_SALES_URI_ONEPIECE")
    sales_db = os.getenv("MONGODB_SALES_DB") or os.getenv("MONGODB_SALES_DB_ONEPIECE")
    if sales_uri and sales_db:
        os.environ["MONGODB_SALES_URI"] = sales_uri
        os.environ["MONGODB_SALES_DB"] = sales_db

    print(f"[Orchestrator] MongoDB DB: {db_name}")
    if post == "promo":
        os.environ["MARKET_INSIGHT_PRICE_BAND"] = "PROMO"
        market_insight_gen.main()
        result = "published"
    else:
        result = market_movers_gen.main(post)

    print(f"[Orchestrator] Monday post workflow completed ({POST_NAMES[post]}, {result})")
    return result


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()
    # in locale (DEBUG_LOCAL_ONLY) solo file su disco: niente mail
    local_only = os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
    post = "pump"

    try:
        post = resolve_post()
        result = generate_monday_post(post)
        if local_only:
            sys.exit(0)

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = f"✅ [1/1][INSTAGRAM] {POST_NAMES[post]} generation completed ({result})"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception as e:
        if not local_only:
            send_email(f"🚫 [1/1][INSTAGRAM] {POST_NAMES[post]} generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
