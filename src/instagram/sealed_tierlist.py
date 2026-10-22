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


def generate_sealed_tierlist_post() -> str:
    from private_providers.instagram import generateSealedTierlist as sealed_tierlist_gen

    print("[Orchestrator] Starting sealed tierlist generation workflow")
    print("[Orchestrator] Provider module: private_providers.instagram.generateSealedTierlist")

    # su GitHub MONGODB_URI/MONGODB_DB; in locale anche le variabili di One Piece di .env.local
    # (il provider legge MONGODB_URI/MONGODB_DB, quindi si impostano quelle)
    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGODB_URI_ONEPIECE")
    db_name = os.getenv("MONGODB_DB") or os.getenv("MONGODB_DB_ONEPIECE")
    if not mongo_uri or not db_name:
        raise RuntimeError("MONGODB_URI/MONGODB_DB (or MONGODB_URI_ONEPIECE/MONGODB_DB_ONEPIECE) required")
    os.environ["MONGODB_URI"] = mongo_uri
    os.environ["MONGODB_DB"] = db_name

    print(f"[Orchestrator] MongoDB DB: {db_name}")
    print("[Orchestrator] Delegating to provider main()")

    # box / case, oppure "skipped" nei giovedi' senza post
    result = sealed_tierlist_gen.main()

    print(f"[Orchestrator] Sealed tierlist workflow completed ({result})")
    return result


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()
    # in locale (DEBUG_LOCAL_ONLY) solo file su disco: niente mail
    local_only = os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

    try:
        result = generate_sealed_tierlist_post()
        # nei giovedi' senza post nessuna mail
        if local_only or result == "skipped":
            sys.exit(0)

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = f"✅ [1/1][INSTAGRAM] Sealed tierlist ({result}) generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception as e:
        if not local_only:
            send_email("🚫 [1/1][INSTAGRAM] Sealed tierlist generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
