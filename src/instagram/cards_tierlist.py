import os
import sys
import importlib
from pathlib import Path
import traceback
import time
from datetime import datetime
from unittest import result

from dotenv import load_dotenv
from src.core.emailer import send_email

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def load_provider_main(module_name: str):
    module = importlib.import_module(module_name)
    provider_main = getattr(module, "main", None)
    if not callable(provider_main):
        raise AttributeError(f"Module {module_name!r} does not expose a callable main()")
    return provider_main


def generate_cards_tierlist_post() -> None:
    from private_providers import bundle as providers_bundle

    module_name = getattr(
        providers_bundle,
        "CARDS_TIERLIST_MODULE",
        "private_providers.instagram.generateCardsTierlist",
    )
    provider_main = load_provider_main(module_name)

    print("[Orchestrator] Starting cards tierlist generation workflow")
    print(f"[Orchestrator] Provider module: {module_name}")

    mongo_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")
    if not mongo_uri or not db_name:
        raise RuntimeError("MONGODB_URI and MONGODB_DB environment variables required")

    print(f"[Orchestrator] MongoDB DB: {db_name}")
    print("[Orchestrator] Delegating to provider main()")

    provider_main()

    print("[Orchestrator] Cards tierlist workflow completed")


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    load_dotenv()

    start_time = time.time()
    start_dt = datetime.now()

    try:
        generate_cards_tierlist_post()

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        subject = "✅ [1/1][INSTAGRAM] Cards tierlist generation completed"
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            f"Exit code: {result}"
        )
        send_email(subject, body)
    except Exception as e:
        send_email("🚫 [1/1][INSTAGRAM] Cards tierlist generation failed", traceback.format_exc())
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
