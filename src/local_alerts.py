"""
local_alerts.py
---------------
Console logging alternative to Pub/Sub and SendGrid for local runs.
"""

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

def send_irrigation_alert(
    recommendation: Any,
    farmer_email: str | None = None,
    farm_name: str = "",
    analysis_date: str | None = None,
) -> dict[str, Any]:
    
    if analysis_date is None:
        analysis_date = datetime.now(timezone.utc).date().isoformat()
        
    logger.info(f"🚨 LOCAL ALERT: Field {recommendation.field_id} | Urgency: {recommendation.final_urgency.value}")
    logger.info(f"   Recommended Water: {recommendation.recommended_water_mm} mm")
    if farmer_email:
        logger.info(f"   (Simulation) Email would be sent to {farmer_email}")
        
    return {
        "pubsub_message_id": "local-simulation-id",
        "email_sent": bool(farmer_email),
        "email_recipient": farmer_email
    }
