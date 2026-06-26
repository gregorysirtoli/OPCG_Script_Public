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


def generate_big_dump_post() -> None:
    from private_providers.instagram import generateBigDump as big_dump_gen

    print("[Orchestrator] Starting big dump generation workflow")
    print("[Orchestrator] Provider module: private_providers.instagram.generateBigDump")

    mongo_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")
    if not mongo_uri or not db_name:
        raise RuntimeError("MONGODB_URI and MONGODB_DB environment variables required")

    print(f"[Orchestrator] MongoDB DB: {db_name}")
    print("[Orchestrator] Delegating to provider main()")

    big_dump_gen.main()

    print("[Orchestrator] Big dump workflow completed")


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()

    try:
        generate_big_dump_post()

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = "✅ [1/1][INSTAGRAM] Big dump generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception:
        send_email("🚫 [1/1][INSTAGRAM] Big dump generation failed", traceback.format_exc())
        raise
