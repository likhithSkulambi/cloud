"""
main.py
-------
Google Cloud Functions entry points for the Smart Irrigation Advisor.

Three HTTP-triggered Cloud Functions:
--------------------------------------
1. fetch_and_store_weather
   Fetches NASA POWER weather data for every active field in BigQuery
   and stores the records.  Triggered by Cloud Scheduler (daily).

2. evaluate_and_recommend
   Reads stored weather records, runs the irrigation rule engine, saves
   recommendations, and dispatches alerts.  Called after weather ingestion.

3. get_recommendations
   HTTP API endpoint that returns current recommendations in JSON.
   Supports query parameters: ?urgency=CRITICAL|HIGH|MODERATE|NONE&limit=N

Deployment
----------
    See deployment/deploy.sh for the gcloud commands.

Environment variables
-----------------------
    GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_LOCATION,
    PUBSUB_TOPIC_ID, SENDGRID_API_KEY, ALERT_FROM_EMAIL, ALERT_MIN_URGENCY
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import date, timedelta
from typing import Any

import functions_framework  # provided by the Cloud Functions runtime

# Internal modules (co-deployed in the same source directory)
from fetch_nasa_data import fetch_weather_data
from bigquery_store import (
    initialize_schema,
    list_active_fields,
    insert_weather_records,
    insert_recommendation,
    get_weather_for_field,
    get_latest_recommendations,
    get_dashboard_summary,
)
from irrigation_rules import evaluate_irrigation_rules
from alert_system import send_irrigation_alert

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WEATHER_LOOKBACK_DAYS = int(os.environ.get("WEATHER_LOOKBACK_DAYS", "7"))
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": CORS_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }


def _json_response(data: Any, status: int = 200):
    """Return a tuple (response_body, status_code, headers) for Flask-style CF returns."""
    return json.dumps(data, default=str), status, _cors_headers()


def _error_response(message: str, status: int = 500):
    return _json_response({"error": message, "status": "error"}, status)


# ---------------------------------------------------------------------------
# Cloud Function 1 – Weather Ingestion
# ---------------------------------------------------------------------------

@functions_framework.http
def fetch_and_store_weather(request):
    """
    HTTP Cloud Function – Fetch NASA POWER weather data for all active fields
    and store records in BigQuery.

    Method: POST or GET
    Expected request body (JSON, optional):
        {
            "field_ids": ["field-001", "field-002"],   // optional filter
            "days": 7                                   // optional override
        }

    Returns:
        {
            "status": "ok",
            "fields_processed": 5,
            "records_inserted": 35,
            "errors": []
        }
    """
    # Handle CORS preflight
    if request.method == "OPTIONS":
        return "", 204, _cors_headers()

    logger.info("=== fetch_and_store_weather triggered ===")

    try:
        body: dict[str, Any] = request.get_json(silent=True) or {}
        days = int(body.get("days", WEATHER_LOOKBACK_DAYS))
        filter_field_ids: list[str] | None = body.get("field_ids")

        # Ensure schema is ready
        initialize_schema()

        # Get fields to process
        active_fields = list_active_fields()
        if not active_fields:
            logger.warning("No active fields found in field_registry")
            return _json_response({"status": "ok", "fields_processed": 0, "records_inserted": 0, "errors": []})

        if filter_field_ids:
            active_fields = [f for f in active_fields if f["field_id"] in filter_field_ids]

        today = date.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        total_inserted = 0
        errors: list[str] = []

        for field in active_fields:
            field_id = field["field_id"]
            lat = field["latitude"]
            lon = field["longitude"]

            try:
                records = fetch_weather_data(lat, lon, start_date, end_date)
                if records:
                    n = insert_weather_records(field_id, records)
                    total_inserted += n
                    logger.info("Field %s: %d records ingested", field_id, n)
                else:
                    logger.warning("Field %s: no weather records returned", field_id)
            except Exception as exc:
                msg = f"Field {field_id}: {exc}"
                logger.error(msg)
                errors.append(msg)

        result = {
            "status": "ok" if not errors else "partial",
            "fields_processed": len(active_fields),
            "records_inserted": total_inserted,
            "errors": errors,
        }
        return _json_response(result)

    except Exception as exc:
        logger.error("Unhandled exception in fetch_and_store_weather: %s", traceback.format_exc())
        return _error_response(str(exc))


# ---------------------------------------------------------------------------
# Cloud Function 2 – Evaluate & Recommend
# ---------------------------------------------------------------------------

@functions_framework.http
def evaluate_and_recommend(request):
    """
    HTTP Cloud Function – Read stored weather records, run irrigation rules,
    store recommendations, and dispatch alerts.

    Method: POST or GET
    Expected request body (JSON, optional):
        {
            "field_ids": ["field-001"],   // optional filter
            "days": 7                     // look-back window
        }

    Returns:
        {
            "status": "ok",
            "recommendations": [
                {
                    "field_id": "...",
                    "urgency": "HIGH",
                    "recommended_water_mm": 25.0,
                    "alert_sent": true
                },
                ...
            ],
            "errors": []
        }
    """
    if request.method == "OPTIONS":
        return "", 204, _cors_headers()

    logger.info("=== evaluate_and_recommend triggered ===")

    try:
        body: dict[str, Any] = request.get_json(silent=True) or {}
        days = int(body.get("days", WEATHER_LOOKBACK_DAYS))
        filter_field_ids: list[str] | None = body.get("field_ids")

        active_fields = list_active_fields()
        if filter_field_ids:
            active_fields = [f for f in active_fields if f["field_id"] in filter_field_ids]

        recommendations_out: list[dict[str, Any]] = []
        errors: list[str] = []
        analysis_date = (date.today() - timedelta(days=1)).isoformat()

        for field in active_fields:
            field_id = field["field_id"]
            crop_type = field.get("crop_type", "default")
            lat = field.get("latitude", 20.0)
            farmer_email = field.get("farmer_email")
            farm_name = field.get("farm_name", "")

            try:
                # Prefer cached BigQuery data; fall back to live NASA API
                weather_records = get_weather_for_field(field_id, days=days)
                if not weather_records:
                    logger.info(
                        "No cached records for field %s; fetching from NASA POWER", field_id
                    )
                    end_date = date.today() - timedelta(days=1)
                    start_date = end_date - timedelta(days=days - 1)
                    weather_records = fetch_weather_data(lat, field["longitude"], start_date, end_date)

                if not weather_records:
                    errors.append(f"Field {field_id}: no weather data available")
                    continue

                # Convert BigQuery Row objects to plain dicts if necessary
                weather_dicts = [dict(r) for r in weather_records]

                # Run rule engine
                recommendation = evaluate_irrigation_rules(
                    field_id=field_id,
                    crop_type=crop_type,
                    weather_records=weather_dicts,
                    latitude=lat,
                )

                # Persist recommendation
                rec_id = insert_recommendation(recommendation, analysis_date)

                # Send alert
                alert_result = send_irrigation_alert(
                    recommendation=recommendation,
                    farmer_email=farmer_email,
                    farm_name=farm_name,
                    analysis_date=analysis_date,
                )

                recommendations_out.append({
                    "field_id": field_id,
                    "recommendation_id": rec_id,
                    "urgency": recommendation.final_urgency.value,
                    "recommended_water_mm": recommendation.recommended_water_mm,
                    "net_deficit_mm": recommendation.net_water_deficit_mm,
                    "triggered_rules": [r.rule_id for r in recommendation.triggered_rules],
                    "alert": alert_result,
                })

            except Exception as exc:
                msg = f"Field {field_id}: {exc}"
                logger.error(msg)
                errors.append(msg)

        result = {
            "status": "ok" if not errors else "partial",
            "recommendations": recommendations_out,
            "errors": errors,
        }
        return _json_response(result)

    except Exception as exc:
        logger.error("Unhandled exception in evaluate_and_recommend: %s", traceback.format_exc())
        return _error_response(str(exc))


# ---------------------------------------------------------------------------
# Cloud Function 3 – Get Recommendations (HTTP API)
# ---------------------------------------------------------------------------

@functions_framework.http
def get_recommendations(request):
    """
    HTTP Cloud Function – Return current irrigation recommendations as JSON.

    Method: GET
    Query parameters:
        urgency  – Filter by urgency level: CRITICAL | HIGH | MODERATE | NONE
        limit    – Max records returned (default: 100)
        summary  – If "true", return dashboard summary statistics

    Examples:
        GET /get_recommendations
        GET /get_recommendations?urgency=CRITICAL
        GET /get_recommendations?summary=true

    Returns:
        {
            "status": "ok",
            "count": 12,
            "recommendations": [ ... ]
        }

        OR (when summary=true):
        {
            "status": "ok",
            "dashboard_summary": { ... }
        }
    """
    if request.method == "OPTIONS":
        return "", 204, _cors_headers()

    logger.info("=== get_recommendations called ===")

    try:
        urgency = request.args.get("urgency", "").upper() or None
        limit = int(request.args.get("limit", 100))
        want_summary = request.args.get("summary", "false").lower() == "true"

        if want_summary:
            summary_data = get_dashboard_summary()
            return _json_response({"status": "ok", "dashboard_summary": summary_data})

        # Validate urgency
        valid_urgencies = {"CRITICAL", "HIGH", "MODERATE", "NONE", None}
        if urgency not in valid_urgencies:
            return _error_response(
                f"Invalid urgency '{urgency}'. Must be one of CRITICAL, HIGH, MODERATE, NONE",
                status=400,
            )

        recs = get_latest_recommendations(urgency_filter=urgency, limit=limit)
        return _json_response({"status": "ok", "count": len(recs), "recommendations": recs})

    except Exception as exc:
        logger.error("Unhandled exception in get_recommendations: %s", traceback.format_exc())
        return _error_response(str(exc))
