import json
import os
import urllib.request
from datetime import datetime, timezone

# Notifica push: dopo l'inserimento il sito invia la notifica ai dispositivi
# dell'utente (rotta interna /api/internal/push, segreto PUSH_NOTIFY_SECRET).
PUSH_TIMEOUT_SECONDS = 10

def ensure_notification_indexes(db) -> None:
    db.Notification.create_index(
        [("userId", 1), ("readAt", 1), ("createdAt", -1)],
        name="idx_notification_user_read_created",
    )
    db.Notification.create_index(
        [("userId", 1), ("createdAt", -1)],
        name="idx_notification_user_created",
    )


def send_push_notification(notification_id) -> None:
    # Senza segreto la push è disattivata; un errore non blocca lo script.
    secret = (os.getenv("PUSH_NOTIFY_SECRET") or "").strip()
    if not secret:
        return

    url = os.getenv("PUSH_NOTIFY_URL").strip()
    request = urllib.request.Request(
        url,
        data=json.dumps({"notificationId": str(notification_id)}).encode("utf-8"),
        headers={"Content-Type": "application/json", "x-push-secret": secret},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=PUSH_TIMEOUT_SECONDS) as response:
            response.read()
    except Exception as exc:
        print(f"[push] notification {notification_id} not sent: {exc}")


def enqueue_notification(db, user_id, notification_text: str) -> None:
    result = db.Notification.insert_one({
        "userId": user_id,
        "createdAt": datetime.now(timezone.utc),
        "readAt": None,
        "notificationText": notification_text,
    })
    send_push_notification(result.inserted_id)
