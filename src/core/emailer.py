import os, smtplib
from email.mime.text import MIMEText
from typing import Optional

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("EMAIL_ADDRESS")
SMTP_PASS = os.getenv("EMAIL_PASSWORD")
MAIL_TO = os.getenv("EMAIL_TO")
MAIL_FROM = os.getenv("MAIL_FROM")

def send_email(subject: str, body: str) -> Optional[str]:
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and MAIL_FROM and MAIL_TO):
        return "SMTP not configured; skipped"
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = MAIL_FROM
        msg["To"] = MAIL_TO
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(MAIL_FROM, [m.strip() for m in MAIL_TO.split(",") if m.strip()], msg.as_string())
        return None
    except Exception as e:
        return f"email error: {e}"