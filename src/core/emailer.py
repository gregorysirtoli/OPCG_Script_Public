import os, smtplib
from email.mime.text import MIMEText
from typing import Optional

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("EMAIL_ADDRESS")
SMTP_PASS = os.getenv("EMAIL_PASSWORD")

DEFAULT_TO = os.getenv("EMAIL_TO")
MAIL_FROM = os.getenv("MAIL_FROM")

def send_email(subject: str, body: str, to: Optional[str] = None) -> Optional[str]:
    final_to = (to or DEFAULT_TO or "").strip()

    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and MAIL_FROM and final_to):
        return "SMTP not configured; skipped"

    try:
        msg = MIMEText(body, "html", "utf-8")
        msg["Subject"] = subject
        msg["From"] = MAIL_FROM
        msg["To"] = final_to

        recipients = [m.strip() for m in final_to.split(",") if m.strip()]

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM, recipients, msg.as_string())

        return None
    except Exception as e:
        return f"Email error: {e}"