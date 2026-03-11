import time
from datetime import datetime
from .trainer import train_all
from src.core.emailer import send_email
import traceback

if __name__ == "__main__":
    try:
        # Timer start
        start_time = time.time()
        start_dt = datetime.now()

        print(f"🚀 Inizio esecuzione: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # Run main function
        train_all(artifacts_dir="./artifacts")

        # Timer end
        end_time = time.time()
        end_dt = datetime.now()

        elapsed = end_time - start_time

        print(f"✅ Fine esecuzione: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Tempo totale: {elapsed:.2f} secondi")

        minutes = elapsed / 60.0
        body = (
            f"Start: {start_dt:%Y-%m-%d %H:%M:%S}\n"
            f"End:   {end_dt:%Y-%m-%d %H:%M:%S}\n"
            f"Durata: {minutes:.1f} minuti ({elapsed:.1f} secondi)"
        )

        send_email("✅ [WORKFLOW] Train Report", body)

    except Exception:
        send_email("🚫 [WORKFLOW] Train Report", traceback.format_exc())
        raise
