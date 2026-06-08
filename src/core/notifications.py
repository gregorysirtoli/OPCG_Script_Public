from datetime import datetime, timezone


def ensure_notification_indexes(db) -> None:
    db.Notification.create_index(
        [("userId", 1), ("readAt", 1), ("createdAt", -1)],
        name="idx_notification_user_read_created",
    )
    db.Notification.create_index(
        [("userId", 1), ("createdAt", -1)],
        name="idx_notification_user_created",
    )


def enqueue_notification(db, user_id, notification_text: str) -> None:
    db.Notification.insert_one({
        "userId": user_id,
        "createdAt": datetime.now(timezone.utc),
        "readAt": None,
        "notificationText": notification_text,
    })
