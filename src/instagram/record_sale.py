import os
import sys
from pathlib import Path
import traceback
import time
from datetime import datetime

from dotenv import load_dotenv
from src.core.emailer import send_email

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def generate_record_sale_post() -> None:
    from private_providers.instagram import generateRecordSale as record_sale_gen

    print("[Orchestrator] Starting record sale generation workflow")
    print("[Orchestrator] Provider module: private_providers.instagram.generateRecordSale")

    # su GitHub MONGODB_URI/MONGODB_DB (+ MONGODB_SALES_*); in locale anche le variabili di One Piece
    # di .env.local. Il provider legge MONGODB_URI/MONGODB_DB/MONGODB_SALES_*, quindi si impostano quelle.
    use_onepiece_env = not (os.getenv("MONGODB_URI") and os.getenv("MONGODB_DB"))
    if use_onepiece_env:
        mongo_uri, db_name = os.getenv("MONGODB_URI_ONEPIECE"), os.getenv("MONGODB_DB_ONEPIECE")
        sales_uri, sales_db = os.getenv("MONGODB_SALES_URI_ONEPIECE"), os.getenv("MONGODB_SALES_DB_ONEPIECE")
    else:
        mongo_uri, db_name = os.getenv("MONGODB_URI"), os.getenv("MONGODB_DB")
        sales_uri, sales_db = os.getenv("MONGODB_SALES_URI"), os.getenv("MONGODB_SALES_DB")
    if not mongo_uri or not db_name:
        raise RuntimeError("MONGODB_URI/MONGODB_DB (or MONGODB_URI_ONEPIECE/MONGODB_DB_ONEPIECE) required")
    os.environ["MONGODB_URI"] = mongo_uri
    os.environ["MONGODB_DB"] = db_name
    print(f"[Orchestrator] Sales DB: {sales_db or db_name}")
    if sales_uri and sales_db:
        os.environ["MONGODB_SALES_URI"] = sales_uri
        os.environ["MONGODB_SALES_DB"] = sales_db

    print(f"[Orchestrator] MongoDB DB: {db_name}")
    print("[Orchestrator] Delegating to provider main()")

    record_sale_gen.main()

    print("[Orchestrator] Record sale workflow completed")


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()
    # in locale (DEBUG_LOCAL_ONLY) solo file su disco: niente mail
    local_only = os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

    try:
        generate_record_sale_post()
        if local_only:
            sys.exit(0)

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = "✅ [1/1][INSTAGRAM] Record sale generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception as e:
        if not local_only:
            send_email("🚫 [1/1][INSTAGRAM] Record sale generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
