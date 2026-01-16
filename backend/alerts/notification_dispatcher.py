"""
STEP 6.2 — Notification Dispatcher (Enum-safe)

Reads existing alerts and delivers them via configured channels.

No alert creation.

No activity evaluation.
"""

from common.db import get_cursor
from common.time_utils import utc_now
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import psycopg2.extensions as ext

# -------------------------------------------------
# Fetch alerts pending delivery (enum-correct)
# -------------------------------------------------

def fetch_pending_alerts(limit=50):
    """
    Fetch alerts that are visible (SENT) but not yet dispatched to channels.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                al.id              AS alert_id,
                al.farm_id,
                al.alert_rule_id,
                al.activity_instance_id,

                ar.name            AS rule_name,
                ar.channel         AS rule_channels,
                ar.severity        AS rule_severity,

                ai.activity_type_id,
                ai.status          AS activity_status,
                ai.started_offset_min,
                ai.activity_schedule_id

            FROM alert_log al
            JOIN alert_rule ar ON ar.id = al.alert_rule_id
            JOIN activity_instance ai ON ai.id = al.activity_instance_id

            WHERE al.status = 'SENT'
              AND al.channel IS NULL
            ORDER BY al.triggered_at
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()

# -------------------------------------------------
# Message & details builders
# -------------------------------------------------

def build_message(rule_name: str, activity_status: str) -> str:
    """
    Human-readable alert message.
    """
    return f"{rule_name} ({activity_status})"

def build_details(row: dict) -> dict:
    """
    Structured metadata for debugging / UI drilldown.
    """
    return {
        "activity_type_id": row["activity_type_id"],
        "activity_status": row["activity_status"],
        "started_offset_min": row["started_offset_min"],
        "activity_schedule_id": row["activity_schedule_id"],
    }

# -------------------------------------------------
# Channel delivery stubs (V1)
# -------------------------------------------------

def deliver_email(alert_id: str, message: str) -> bool:
    print(f"[EMAIL] alert_id={alert_id} | {message}")
    return True

def deliver_sms(alert_id: str, message: str) -> bool:
    print(f"[SMS] alert_id={alert_id} | {message}")
    return True

def deliver_whatsapp(alert_id: str, message: str) -> bool:
    print(f"[WHATSAPP] alert_id={alert_id} | {message}")
    return True

# -------------------------------------------------
# Dispatcher main
# -------------------------------------------------

def dispatch_notifications():
    alerts = fetch_pending_alerts()

    if not alerts:
        return

    with get_cursor() as cur:
        for row in alerts:
            message = build_message(
                row["rule_name"],
                row["activity_status"],
            )

            details = build_details(row)

            rule_channels_raw = row["rule_channels"]
            
            # Handle different formats from database
            if isinstance(rule_channels_raw, list):
                channels = rule_channels_raw
            elif isinstance(rule_channels_raw, str):
                # Parse PostgreSQL array string format like "{EMAIL,SMS}" or "{EMAIL}"
                if rule_channels_raw.startswith("{") and rule_channels_raw.endswith("}"):
                    # Remove braces and split by comma
                    inner = rule_channels_raw[1:-1]
                    channels = [ch.strip().strip('"') for ch in inner.split(",")] if inner else ["APP"]
                else:
                    channels = [rule_channels_raw] if rule_channels_raw else ["APP"]
            else:
                channels = ["APP"]

            delivery_ok = True

            for ch in channels:
                if ch == "EMAIL":
                    delivery_ok &= deliver_email(row["alert_id"], message)
                elif ch == "SMS":
                    delivery_ok &= deliver_sms(row["alert_id"], message)
                elif ch == "WHATSAPP":
                    delivery_ok &= deliver_whatsapp(row["alert_id"], message)
                elif ch == "APP":
                    # APP is already visible — no-op
                    pass

            # Store first channel (column is single enum, not array)
            channel_value = channels[0] if channels else "APP"
            
            cur.execute(
                """
                UPDATE alert_log
                SET
                    channel = %s,
                    message = %s,
                    details = %s,
                    status = %s
                WHERE id = %s
                """,
                (
                    channel_value,
                    message,
                    Json(details),
                    "SENT" if delivery_ok else "FAILED",
                    row["alert_id"],
                ),
            )
