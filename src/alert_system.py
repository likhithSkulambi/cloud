"""
alert_system.py
---------------
Pub/Sub + SendGrid email alert system for the Smart Irrigation Advisor.

Responsibilities
----------------
* Publish irrigation recommendations as Pub/Sub messages (for downstream
  consumers – dashboards, SMS gateways, farm management systems).
* Send formatted HTML email alerts via SendGrid whenever urgency reaches
  a configured threshold.
* Provide a unified ``send_irrigation_alert`` function used by main.py.

Environment variables required
-------------------------------
    GCP_PROJECT_ID        – GCP project (for Pub/Sub)
    PUBSUB_TOPIC_ID       – Pub/Sub topic name (e.g., "irrigation-alerts")
    SENDGRID_API_KEY      – SendGrid API key
    ALERT_FROM_EMAIL      – Verified sender email address
    ALERT_MIN_URGENCY     – Minimum urgency to trigger email (default: HIGH)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
PUBSUB_TOPIC_ID = os.environ.get("PUBSUB_TOPIC_ID", "irrigation-alerts")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
ALERT_FROM_EMAIL = os.environ.get("ALERT_FROM_EMAIL", "alerts@smart-irrigation.example.com")
ALERT_MIN_URGENCY = os.environ.get("ALERT_MIN_URGENCY", "HIGH")   # CRITICAL | HIGH | MODERATE | NONE

_URGENCY_RANK = {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "NONE": 1}


def _urgency_rank(level: str) -> int:
    return _URGENCY_RANK.get(level.upper(), 0)


# ---------------------------------------------------------------------------
# Pub/Sub publisher
# ---------------------------------------------------------------------------

def publish_to_pubsub(
    message_data: dict[str, Any],
    topic_id: str | None = None,
    project_id: str | None = None,
    attributes: dict[str, str] | None = None,
) -> str | None:
    """
    Publish a JSON message to a Pub/Sub topic.

    Parameters
    ----------
    message_data : dict     Payload to publish (will be JSON-serialised).
    topic_id : str          Override the default PUBSUB_TOPIC_ID.
    project_id : str        Override the default GCP_PROJECT_ID.
    attributes : dict       Optional Pub/Sub message attributes (key-value strings).

    Returns
    -------
    str | None  Published message ID, or None on failure.
    """
    try:
        from google.cloud import pubsub_v1  # type: ignore
    except ImportError:
        logger.warning("google-cloud-pubsub not installed; skipping Pub/Sub publish")
        return None

    project = project_id or GCP_PROJECT_ID
    topic = topic_id or PUBSUB_TOPIC_ID
    topic_path = f"projects/{project}/topics/{topic}"

    publisher = pubsub_v1.PublisherClient()
    payload = json.dumps(message_data, default=str).encode("utf-8")

    try:
        future = publisher.publish(topic_path, payload, **(attributes or {}))
        message_id = future.result(timeout=30)
        logger.info("Published Pub/Sub message %s to %s", message_id, topic_path)
        return message_id
    except Exception as exc:
        logger.error("Pub/Sub publish failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Email formatting
# ---------------------------------------------------------------------------

_URGENCY_COLOR = {
    "CRITICAL": "#c0392b",  # Red
    "HIGH": "#e67e22",       # Orange
    "MODERATE": "#f1c40f",   # Yellow
    "NONE": "#27ae60",       # Green
}

_URGENCY_EMOJI = {
    "CRITICAL": "🚨",
    "HIGH": "⚠️",
    "MODERATE": "💧",
    "NONE": "✅",
}


def _build_html_email(
    field_id: str,
    farm_name: str,
    crop_type: str,
    urgency: str,
    recommended_water_mm: float,
    cumulative_et0_mm: float,
    cumulative_rain_mm: float,
    net_deficit_mm: float,
    triggered_rules: list[int],
    summary: str,
    analysis_date: str,
) -> str:
    """Return a styled HTML email body for an irrigation alert."""
    color = _URGENCY_COLOR.get(urgency, "#888888")
    emoji = _URGENCY_EMOJI.get(urgency, "💧")
    rule_list = ", ".join(f"Rule {r}" for r in triggered_rules) if triggered_rules else "None"

    water_text = (
        f"{recommended_water_mm:.1f} mm"
        if recommended_water_mm > 0
        else "No irrigation required"
    )

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Irrigation Alert – {farm_name or field_id}</title>
  <style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f4f6f8; margin: 0; padding: 0; }}
    .container {{ max-width: 600px; margin: 30px auto; background: #ffffff; border-radius: 10px;
                  box-shadow: 0 4px 12px rgba(0,0,0,0.1); overflow: hidden; }}
    .header {{ background: {color}; color: #fff; padding: 28px 32px; }}
    .header h1 {{ margin: 0; font-size: 22px; }}
    .header .badge {{ display: inline-block; background: rgba(255,255,255,0.25);
                      padding: 4px 12px; border-radius: 20px; font-size: 13px; margin-top: 8px; }}
    .body {{ padding: 28px 32px; }}
    .metric {{ display: flex; justify-content: space-between; padding: 10px 0;
               border-bottom: 1px solid #eee; }}
    .metric:last-child {{ border-bottom: none; }}
    .metric-label {{ color: #666; font-size: 14px; }}
    .metric-value {{ font-weight: 600; color: #222; font-size: 14px; }}
    .recommendation-box {{ background: #f8f9fa; border-left: 4px solid {color};
                            padding: 14px 18px; border-radius: 4px; margin: 20px 0; }}
    .footer {{ background: #f4f6f8; padding: 16px 32px; font-size: 12px; color: #999;
               text-align: center; }}
    a {{ color: {color}; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>{emoji} Irrigation Alert – {urgency}</h1>
      <div class="badge">Smart Irrigation Advisor · {analysis_date}</div>
    </div>
    <div class="body">
      <p>An irrigation recommendation has been generated for your field. Please review the details below.</p>

      <div class="metric">
        <span class="metric-label">Field ID</span>
        <span class="metric-value">{field_id}</span>
      </div>
      <div class="metric">
        <span class="metric-label">Farm Name</span>
        <span class="metric-value">{farm_name or "—"}</span>
      </div>
      <div class="metric">
        <span class="metric-label">Crop Type</span>
        <span class="metric-value">{crop_type.title()}</span>
      </div>
      <div class="metric">
        <span class="metric-label">Urgency Level</span>
        <span class="metric-value" style="color:{color}; font-weight:700;">{emoji} {urgency}</span>
      </div>
      <div class="metric">
        <span class="metric-label">Recommended Water</span>
        <span class="metric-value">{water_text}</span>
      </div>
      <div class="metric">
        <span class="metric-label">Cumulative ET₀ (window)</span>
        <span class="metric-value">{cumulative_et0_mm:.2f} mm</span>
      </div>
      <div class="metric">
        <span class="metric-label">Cumulative Rainfall (window)</span>
        <span class="metric-value">{cumulative_rain_mm:.2f} mm</span>
      </div>
      <div class="metric">
        <span class="metric-label">Net Water Deficit</span>
        <span class="metric-value">{net_deficit_mm:.2f} mm</span>
      </div>
      <div class="metric">
        <span class="metric-label">Triggered Rules</span>
        <span class="metric-value">{rule_list}</span>
      </div>

      <div class="recommendation-box">
        <strong>Summary:</strong><br/>
        {summary}
      </div>

      <p style="font-size:13px;color:#555;">
        This recommendation was generated by the <strong>Smart Irrigation Advisor</strong>
        using the FAO-56 Penman-Monteith method with 7-day NASA POWER meteorological data.
      </p>
    </div>
    <div class="footer">
      Smart Irrigation Advisor · Automated Alert System<br/>
      To unsubscribe or manage preferences, contact your farm administrator.
    </div>
  </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# SendGrid email sender
# ---------------------------------------------------------------------------

def send_email_alert(
    to_email: str,
    field_id: str,
    farm_name: str,
    crop_type: str,
    urgency: str,
    recommended_water_mm: float,
    cumulative_et0_mm: float,
    cumulative_rain_mm: float,
    net_deficit_mm: float,
    triggered_rules: list[int],
    summary: str,
    analysis_date: str,
) -> bool:
    """
    Send an HTML email alert via SendGrid.

    Returns True on success, False on failure.
    """
    if not SENDGRID_API_KEY:
        logger.warning("SENDGRID_API_KEY not set; skipping email alert")
        return False

    try:
        from sendgrid import SendGridAPIClient  # type: ignore
        from sendgrid.helpers.mail import Mail, Content  # type: ignore
    except ImportError:
        logger.warning("sendgrid package not installed; skipping email alert")
        return False

    emoji = _URGENCY_EMOJI.get(urgency, "💧")
    subject = f"{emoji} [{urgency}] Irrigation Alert – {farm_name or field_id} – {analysis_date}"

    html_content = _build_html_email(
        field_id=field_id,
        farm_name=farm_name,
        crop_type=crop_type,
        urgency=urgency,
        recommended_water_mm=recommended_water_mm,
        cumulative_et0_mm=cumulative_et0_mm,
        cumulative_rain_mm=cumulative_rain_mm,
        net_deficit_mm=net_deficit_mm,
        triggered_rules=triggered_rules,
        summary=summary,
        analysis_date=analysis_date,
    )

    message = Mail(
        from_email=ALERT_FROM_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=html_content,
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        if response.status_code in (200, 202):
            logger.info("Email alert sent to %s (status %d)", to_email, response.status_code)
            return True
        else:
            logger.error("SendGrid returned status %d", response.status_code)
            return False
    except Exception as exc:
        logger.error("Failed to send email via SendGrid: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main public interface
# ---------------------------------------------------------------------------

def send_irrigation_alert(
    recommendation: Any,  # IrrigationRecommendation dataclass
    farmer_email: str | None = None,
    farm_name: str = "",
    analysis_date: str | None = None,
) -> dict[str, Any]:
    """
    Orchestrate a full alert for one field:
        1. Publish to Pub/Sub (always).
        2. Send email via SendGrid (if urgency ≥ ALERT_MIN_URGENCY and email provided).

    Parameters
    ----------
    recommendation : IrrigationRecommendation
    farmer_email : str, optional
    farm_name : str, optional
    analysis_date : str, optional   ISO date string; defaults to UTC today.

    Returns
    -------
    dict with keys: pubsub_message_id, email_sent, email_recipient
    """
    if analysis_date is None:
        analysis_date = datetime.now(timezone.utc).date().isoformat()

    urgency = recommendation.final_urgency.value
    triggered_rule_ids = [r.rule_id for r in recommendation.triggered_rules]

    # 1. Publish to Pub/Sub
    pubsub_payload = {
        "field_id": recommendation.field_id,
        "crop_type": recommendation.crop_type,
        "farm_name": farm_name,
        "farmer_email": farmer_email,
        "urgency": urgency,
        "recommended_water_mm": recommendation.recommended_water_mm,
        "cumulative_et0_mm": recommendation.cumulative_et0_mm,
        "cumulative_rain_mm": recommendation.cumulative_rain_mm,
        "net_water_deficit_mm": recommendation.net_water_deficit_mm,
        "triggered_rules": triggered_rule_ids,
        "summary": recommendation.summary,
        "analysis_date": analysis_date,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    msg_id = publish_to_pubsub(
        pubsub_payload,
        attributes={
            "urgency": urgency,
            "field_id": recommendation.field_id,
        },
    )

    # 2. Conditional email
    email_sent = False
    min_rank = _urgency_rank(ALERT_MIN_URGENCY)
    should_email = (
        farmer_email
        and _urgency_rank(urgency) >= min_rank
    )

    if should_email:
        email_sent = send_email_alert(
            to_email=farmer_email,
            field_id=recommendation.field_id,
            farm_name=farm_name,
            crop_type=recommendation.crop_type,
            urgency=urgency,
            recommended_water_mm=recommendation.recommended_water_mm,
            cumulative_et0_mm=recommendation.cumulative_et0_mm,
            cumulative_rain_mm=recommendation.cumulative_rain_mm,
            net_deficit_mm=recommendation.net_water_deficit_mm,
            triggered_rules=triggered_rule_ids,
            summary=recommendation.summary,
            analysis_date=analysis_date,
        )
    else:
        reason = (
            "no email provided"
            if not farmer_email
            else f"urgency {urgency} below threshold {ALERT_MIN_URGENCY}"
        )
        logger.info("Email skipped for field '%s': %s", recommendation.field_id, reason)

    return {
        "pubsub_message_id": msg_id,
        "email_sent": email_sent,
        "email_recipient": farmer_email if email_sent else None,
    }
