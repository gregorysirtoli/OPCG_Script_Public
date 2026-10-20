"""
Post Instagram Top Mover / Top Drop (stesso script, cambia solo il movimento):

    python -m src.instagram.big_movement --movement pump
    python -m src.instagram.big_movement --movement dump
"""
import argparse
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

MOVEMENTS = ("pump", "dump")
POST_NAMES = {"pump": "Top Mover", "dump": "Top Drop"}


def generate_big_movement_post(movement: str) -> None:
    from private_providers.instagram import generateBigMovement as big_movement_gen

    print(f"[Orchestrator] Starting {POST_NAMES[movement]} generation workflow")
    print("[Orchestrator] Provider module: private_providers.instagram.generateBigMovement")

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

    big_movement_gen.main(movement)

    print(f"[Orchestrator] {POST_NAMES[movement]} workflow completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Top Mover / Top Drop Instagram post")
    parser.add_argument("--movement", required=True, choices=MOVEMENTS, help="pump = Top Mover (rialzo a 30 giorni), dump = Top Drop (ribasso)")
    movement = parser.parse_args().movement

    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()
    # in locale (DEBUG_LOCAL_ONLY) solo file su disco: niente mail
    local_only = os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

    try:
        generate_big_movement_post(movement)
        if local_only:
            sys.exit(0)

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = f"✅ [1/1][INSTAGRAM] {POST_NAMES[movement]} generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception as e:
        if not local_only:
            send_email(f"🚫 [1/1][INSTAGRAM] {POST_NAMES[movement]} generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
