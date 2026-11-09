import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from src.core.emailer import send_email

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()
    # in locale (DEBUG_LOCAL_ONLY) nessuna scrittura su MongoDB e niente mail
    local_only = os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

    try:
        from private_providers.instagram import fetchInstagramInsights as insights

        print("[Orchestrator] Starting Instagram insights workflow")
        result = insights.main()
        print(f"[Orchestrator] Insights workflow completed: {result['fetched']} saved, {result['failed']} failed")
        # mail solo con il riepilogo mensile o con errori: nei giorni normali il job e' silenzioso
        if local_only or not (result["summary"] or result["failed"]):
            sys.exit(0)

        elapsed = time.time() - start_time
        body = (result["summary"] or "") + "\n\n"
        if result["failed"]:
            body += f"{result['failed']} insights requests failed:\n" + "\n".join(result["errors"][:30]) + "\n\n"
        body += (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {elapsed / 60.0:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        subject = ("📊 [INSTAGRAM] Monthly insights summary" if result["summary"]
                   else f"⚠️ [INSTAGRAM] Insights: {result['failed']} requests failed")
        send_email(subject, body)
    except Exception as e:
        if not local_only:
            send_email("🚫 [INSTAGRAM] Insights workflow failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
