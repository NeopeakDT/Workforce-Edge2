"""
Notification Dispatcher

Reads unresolved alerts and delivers them.
Delivery channels are pluggable.

Current:
- APP (dashboard notification)

Future:
- EMAIL
- SMS
- WhatsApp
- Slack

Alert → Delivery: This does not decide alerts.
It only delivers unresolved alerts via configured channels.

Design:
- Reads alert_log for undelivered alerts
- Delivers via configured channels
- Marks alerts as delivered
- Pluggable channel architecture
"""

from common.db import get_cursor
from common.time_utils import utc_now


def fetch_pending_alerts(limit=50):
    """
    Fetch unresolved alerts that haven't been delivered yet.
    
    Args:
        limit: Maximum number of alerts to fetch
        
    Returns:
        list: List of alert records
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM alert_log
            WHERE resolved_at IS NULL
              AND delivered_at IS NULL
            ORDER BY created_at
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def mark_delivered(alert_id):
    """
    Mark an alert as delivered by setting delivered_at timestamp.
    
    Args:
        alert_id: UUID of the alert to mark as delivered
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE alert_log
            SET delivered_at = %s
            WHERE id = %s
            """,
            (utc_now(), alert_id),
        )


# ---------------- DELIVERY CHANNELS ----------------

def deliver_app(alert):
    """
    App delivery is implicit (dashboard polling).
    Just mark delivered.
    
    Args:
        alert: Alert record dictionary
    """
    mark_delivered(alert["id"])


def deliver_email(alert):
    """
    Placeholder for email delivery.
    
    TODO: Integrate with SES / SendGrid / SMTP
    
    Args:
        alert: Alert record dictionary
    """
    # integrate SES / SendGrid later
    mark_delivered(alert["id"])


# ---------------- MAIN ----------------

def dispatch_notifications():
    """
    Main dispatch function.
    
    Fetches pending alerts and delivers them via configured channels.
    Currently only supports APP delivery (dashboard notifications).
    
    Process:
    1. Fetch unresolved, undelivered alerts
    2. Deliver via APP channel (default)
    3. Mark as delivered
    
    Future: Add support for EMAIL, SMS, WhatsApp, Slack channels.
    """
    alerts = fetch_pending_alerts()

    for alert in alerts:
        # Default: APP
        deliver_app(alert)
