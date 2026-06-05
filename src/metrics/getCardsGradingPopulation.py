from __future__ import annotations

import contextlib
import importlib
import io
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


class TeeStderr(io.TextIOBase):
    def __init__(self, original_stderr: io.TextIOBase, capture_buffer: io.StringIO):
        self.original_stderr = original_stderr
        self.capture_buffer = capture_buffer

    def write(self, s: str) -> int:
        self.original_stderr.write(s)
        self.capture_buffer.write(s)
        return len(s)

    def flush(self) -> None:
        self.original_stderr.flush()
        self.capture_buffer.flush()


def format_error_excerpt(stderr_text: str, max_lines: int = 30) -> str:
    lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
    if not lines:
        return "Nessun dettaglio errore disponibile su stderr."

    relevant = [
        line
        for line in lines
        if "error" in line.lower() or "exception" in line.lower() or "traceback" in line.lower()
    ]
    selected = (relevant or lines)[-max_lines:]
    return "\n".join(selected)


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
        stderr_capture = io.StringIO()
        tee_stderr = TeeStderr(sys.stderr, stderr_capture)
        with contextlib.redirect_stderr(tee_stderr):
            result = provider_main()

        end_time = time.time()
        end_dt = datetime.now()
        elapsed = end_time - start_time
        minutes = elapsed / 60.0

        has_partial_errors = result not in (None, 0)
        subject = (
            "⚠️ [1/1][WORKFLOW] PSA Cards Grading Population (partial errors)"
            if has_partial_errors
            else "✅ [1/1][WORKFLOW] PSA Cards Grading Population"
        )
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)\n"
            f"Exit code: {result}"
        )
        if has_partial_errors:
            body += (
                "\n\nDettagli errori (estratto):\n"
                f"{format_error_excerpt(stderr_capture.getvalue())}"
            )
        send_email(subject, body)
        return 0
    except Exception:
        send_email("🚫 [1/1][WORKFLOW] PSA Cards Grading Population", traceback.format_exc())
        raise


if __name__ == "__main__":
    raise SystemExit(main())
