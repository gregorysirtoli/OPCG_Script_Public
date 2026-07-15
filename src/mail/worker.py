import os
import smtplib
import sys
import uuid
import random
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument

try:
    from dotenv import load_dotenv
except ImportError:  # Optional in CI, where env vars are injected by workflow secrets.
    load_dotenv = None

# =============================================================================
# Environment & constants
# =============================================================================

if load_dotenv:
    load_dotenv(".env.local")
    load_dotenv()

MONGODB_URI   = os.environ["MONGODB_URI"]
MONGODB_DB    = os.environ["MONGODB_DB"]
SMTP_HOST     = os.environ["SMTP_HOST"]
SMTP_PORT     = int(os.environ["SMTP_PORT"])
SMTP_USER     = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
MAIL_FROM     = os.environ.get("SMPT_FROM") or os.environ.get("SMTP_FROM") or SMTP_USER

MAX_BATCH          = int(os.environ.get("MAIL_WORKER_BATCH", 10))
MAX_RETRIES        = int(os.environ.get("MAIL_WORKER_MAX_RETRIES", 5))
LOCK_STALE_MINUTES = int(os.environ.get("MAIL_LOCK_STALE_MINUTES", 15))

WORKER_ID = f"gh-actions-mail-worker-{uuid.uuid4().hex[:8]}"


def get_smtp_connection():
    """Return an authenticated SMTP connection. Aruba: 465=SSL, 587=STARTTLS."""
    if SMTP_PORT == 465:
        conn = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
    else:
        conn = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        conn.starttls()
    conn.login(SMTP_USER, SMTP_PASSWORD)
    return conn


def send_html_email(smtp: smtplib.SMTP, to: str, subject: str, body: str):
    msg = MIMEMultipart("alternative")
    msg["From"]    = MAIL_FROM
    msg["To"]      = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html", "utf-8"))
    smtp.sendmail(SMTP_USER, [to], msg.as_string())


def run():
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
    db  = client[MONGODB_DB]
    col = db["Mail"]

    now          = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=LOCK_STALE_MINUTES)
    threshold    = now - timedelta(days=90)  # 3 months

    # === Cleanup: delete mails older than 3 months ===
    deleted = col.delete_many({"createdAt": {"$lt": threshold}}).deleted_count
    print(f"[cleanup] Deleted {deleted} old mail(s).")

    results = []

    smtp = get_smtp_connection()

    try:
        for _ in range(MAX_BATCH):
            # 1) Atomic claim
            mail = col.find_one_and_update(
                {
                    "$and": [
                        {
                            "$or": [
                                {
                                    "status": "queued",
                                    "$or": [
                                        # scheduledAt is set and due
                                        {"scheduledAt": {"$nin": [None, ""], "$lte": now}},
                                        # scheduledAt absent/null → use createdAt
                                        {"scheduledAt": {"$in": [None, ""]}, "createdAt": {"$lte": now}},
                                    ],
                                },
                                # stale locked job (worker crashed)
                                {"status": "locked", "lockedAt": {"$lt": stale_cutoff}},
                            ]
                        },
                        {"retries": {"$lt": MAX_RETRIES}},
                    ]
                },
                {
                    "$set": {
                        "status":   "locked",
                        "lockedAt": now,
                        "lockedBy": WORKER_ID,
                    }
                },
                sort=[("createdAt", 1), ("_id", 1)],
                return_document=ReturnDocument.AFTER,
            )

            if not mail:
                break  # queue empty

            mail_id = mail["_id"]

            # 2) Validate required fields
            if not mail.get("to") or not mail.get("subject") or not mail.get("body"):
                col.update_one(
                    {"_id": mail_id},
                    {
                        "$set": {
                            "status":    "failed",
                            "lastError": "Missing required fields: to/subject/body",
                            "lockedAt":  None,
                            "lockedBy":  None,
                        },
                        "$inc": {"retries": 1},
                    },
                )
                results.append({"_id": str(mail_id), "to": mail.get("to"), "ok": False, "status": "failed", "error": "Missing required fields"})
                continue

            # 3) Send
            try:
                send_html_email(smtp, mail["to"], mail["subject"], mail["body"])

                col.update_one(
                    {"_id": mail_id},
                    {
                        "$set": {
                            "status":    "sent",
                            "sentAt":    now,
                            "lastError": None,
                            "lockedAt":  None,
                            "lockedBy":  None,
                        }
                    },
                )
                results.append({"_id": str(mail_id), "to": mail["to"], "ok": True, "status": "sent"})
                print(f"  [sent] {mail['to']} — {mail['subject'][:60]}")

            except Exception as err:
                err_msg       = str(err)
                next_retries  = int(mail.get("retries", 0)) + 1
                next_status   = "failed" if next_retries >= MAX_RETRIES else "queued"

                col.update_one(
                    {"_id": mail_id},
                    {
                        "$set": {
                            "status":    next_status,
                            "lastError": err_msg,
                            "lockedAt":  None,
                            "lockedBy":  None,
                        },
                        "$inc": {"retries": 1},
                    },
                )
                results.append({"_id": str(mail_id), "to": mail["to"], "ok": False, "status": next_status, "error": err_msg})
                print(f"  [error] {mail['to']} — {err_msg}", file=sys.stderr)

    finally:
        smtp.quit()
        client.close()

    sent  = sum(1 for r in results if r["ok"])
    fails = len(results) - sent
    print(f"\n[done] worker={WORKER_ID} processed={len(results)} sent={sent} failed={fails} deleted={deleted}")

    if fails > 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
