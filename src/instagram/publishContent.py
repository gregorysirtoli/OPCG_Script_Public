import os
import sys
from pathlib import Path
import traceback
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from src.core.emailer import send_email

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def publish_instagram_content() -> str:
    from private_providers.instagram import publishContent as publish_content_provider

    print("[IG] Starting Instagram publish workflow")
    print("[IG] Provider module: private_providers.instagram.publishContent")

    mongo_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")
    if not mongo_uri or not db_name:
        raise RuntimeError("URI and DB environment variables required")

    print(f"[IG] DB: {db_name}")
    print("[IG] Delegating to provider main()")

    # "published", "already_published" (oggi e' gia' uscito un post) o "no_draft"
    result = publish_content_provider.main() or "published"

    print(f"[IG] Instagram publish workflow completed ({result})")
    return result


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()

    # PUBLISH_WINDOW_UTC="15-21" (solo i cron di GitHub): fuori da [15, 21) ora UTC non si pubblica e non si manda
    # nessuna mail. I cron di GitHub partono anche con ore di ritardo: meglio saltare che pubblicare di notte.
    window = os.getenv("PUBLISH_WINDOW_UTC", "").strip()
    if window:
        start_hour, end_hour = (int(part) for part in window.split("-"))
        now_hour = datetime.now(timezone.utc).hour
        if not start_hour <= now_hour < end_hour:
            print(f"[IG] {datetime.now(timezone.utc):%H:%M} UTC is outside the publish window {window} UTC: skipping")
            sys.exit(0)

    try:
        result = publish_instagram_content()
        if result == "already_published":
            sys.exit(0)
        if result == "no_draft":
            # mail solo all'ultimo giro del giorno (PUBLISH_FINAL_ATTEMPT): al primo la bozza puo' essere in ritardo
            if os.getenv("PUBLISH_FINAL_ATTEMPT", "").strip().lower() in {"1", "true", "yes", "on"}:
                send_email("[INSTAGRAM] No post today: no draft to publish",
                           f"{datetime.now():%Y-%m-%d %H:%M} - nessuna bozza pronta in InstagramPosts all'ultimo giro di "
                           "pubblicazione. Controllare i workflow di generazione di oggi su GitHub Actions.")
            sys.exit(0)

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = "[INSTAGRAM] Publish content completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            "Exit code: 0"
        )
        send_email(subject, body)
    except Exception:
        send_email("[INSTAGRAM] Publish content failed", traceback.format_exc())
        raise