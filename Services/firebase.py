from firebase_admin import messaging

def send_push_notification_to_topic(username, title, body):
    """Send push notification to a Firebase topic (all devices of a user)."""
    sanitized_topic = username.replace("@", "_at_").replace(".", "_dot_")

    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            topic=sanitized_topic
        )
        response = messaging.send(message)
        print(f"Notification sent successfully to topic {sanitized_topic}: {response}")
    except Exception as e:
        print(f"Error sending notification: {str(e)}")
