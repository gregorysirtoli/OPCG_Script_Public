"""
Consulente Instagram (workflow OnePiece - IG - Consultant).

  python -m src.instagram.consultant dump <dir>   dati e immagini per l'agente Claude (sola lettura)
  python -m src.instagram.consultant send <dir>   salva in MongoDB e invia per mail il report scritto dall'agente
                                                  (<dir>/report.md, <dir>/report.html, <dir>/summary.json)
"""
import json
import os
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.emailer import send_email  # noqa: E402


def send(out_dir: Path) -> None:
    from private_providers.instagram import consultantDump

    report_md, report_html = out_dir / "report.md", out_dir / "report.html"
    if not report_md.exists() or not report_html.exists():
        raise RuntimeError(f"The agent did not write {report_md.name} and {report_html.name} in {out_dir}")
    summary_path = out_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    markdown, html = report_md.read_text(encoding="utf-8"), report_html.read_text(encoding="utf-8")
    consultantDump.save_report(markdown, html, summary)
    headline = str(summary.get("headline") or "monthly review").strip()[:120]
    error = send_email(f"🧭 [INSTAGRAM] Consultant: {headline}", html)
    if error:
        raise RuntimeError(error)
    print(f"[Consultant] Report saved and sent: {headline}")


if __name__ == "__main__":
    load_dotenv(ROOT_DIR / ".env.local")
    load_dotenv(ROOT_DIR / ".env")
    if len(sys.argv) != 3 or sys.argv[1] not in {"dump", "send"}:
        sys.exit(__doc__)
    command, directory = sys.argv[1], Path(sys.argv[2])
    try:
        if command == "dump":
            from private_providers.instagram import consultantDump

            consultantDump.dump(directory)
        else:
            send(directory)
    except Exception as e:
        if os.getenv("DEBUG_LOCAL_ONLY", "").strip().lower() not in {"1", "true", "yes", "on"}:
            send_email(f"🚫 [INSTAGRAM] Consultant {command} failed", f"<pre>{traceback.format_exc()}</pre>")
        print(f"\nFatal Error: {e}", file=sys.stderr)
        sys.exit(1)
