from __future__ import annotations

import importlib
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

load_dotenv(ROOT_DIR / ".env.local")
load_dotenv(ROOT_DIR / ".env")
load_dotenv()


def load_provider_main(module_name: str):
    module = importlib.import_module(module_name)
    provider_main = getattr(module, "main", None)
    if not callable(provider_main):
        raise AttributeError(f"Module {module_name!r} does not expose a callable main()")
    return provider_main


def main() -> int:
    start_time = time.time()
    start_dt = datetime.now()

    try:
        module_name = os.getenv("CARDS_GRADING_POPULATION_MODULE", "private_providers.cardsGradingPopulation")
        provider_main = load_provider_main(module_name)
        result = provider_main()
        if result not in (None, 0):
            raise RuntimeError(f"Provider exited with code {result}")

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)"
        )
        send_email("✅ [1/1][WORKFLOW] PSA Cards Grading Population", body)
        return 0
    except Exception:
        send_email("🚫 [1/1][WORKFLOW] PSA Cards Grading Population", traceback.format_exc())
        raise


if __name__ == "__main__":
    raise SystemExit(main())
