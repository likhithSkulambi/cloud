"""
bigquery_store.py
-----------------
BigQuery schema definition, data insertion, and query operations for the
Smart Irrigation Advisor.

Tables managed
--------------
    weather_data          – Raw daily NASA POWER records
    irrigation_recommendations  – Rule engine outputs
    field_registry        – Farm/field metadata

Schema conventions
------------------
    * All timestamps use UTC.
    * Nullable fields use REQUIRED only where a value is always present.
    * Partition by DATE to enable cost-effective time-range queries.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.api_core.exceptions import AlreadyExists, NotFound

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (read from environment)
# ---------------------------------------------------------------------------

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "smart_irrigation")
LOCATION = os.environ.get("BIGQUERY_LOCATION", "US")


# ---------------------------------------------------------------------------
# Schema Definitions
# ---------------------------------------------------------------------------

WEATHER_TABLE_ID = "weather_data"
RECOMMENDATIONS_TABLE_ID = "irrigation_recommendations"
FIELD_REGISTRY_TABLE_ID = "field_registry"
USERS_TABLE_ID = "users"

WEATHER_SCHEMA: list[SchemaField] = [
    SchemaField("record_id", "STRING", mode="REQUIRED", description="UUID for the row"),
    SchemaField("field_id", "STRING", mode="REQUIRED", description="Field identifier"),
    SchemaField("date", "DATE", mode="REQUIRED", description="Calendar date of observation"),
    SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED", description="UTC ingestion timestamp"),
    SchemaField("latitude", "FLOAT64", mode="NULLABLE", description="Decimal latitude"),
    SchemaField("longitude", "FLOAT64", mode="NULLABLE", description="Decimal longitude"),
    SchemaField("T2M_MAX", "FLOAT64", mode="NULLABLE", description="Max temperature at 2 m (°C)"),
    SchemaField("T2M_MIN", "FLOAT64", mode="NULLABLE", description="Min temperature at 2 m (°C)"),
    SchemaField("T2M", "FLOAT64", mode="NULLABLE", description="Mean temperature at 2 m (°C)"),
    SchemaField("RH2M", "FLOAT64", mode="NULLABLE", description="Mean relative humidity (%)"),
    SchemaField("WS2M", "FLOAT64", mode="NULLABLE", description="Wind speed at 2 m (m/s)"),
    SchemaField("ALLSKY_SFC_SW_DWN", "FLOAT64", mode="NULLABLE", description="Solar radiation (MJ/m²/day)"),
    SchemaField("PRECTOTCORR", "FLOAT64", mode="NULLABLE", description="Precipitation (mm/day)"),
]

RECOMMENDATIONS_SCHEMA: list[SchemaField] = [
    SchemaField("recommendation_id", "STRING", mode="REQUIRED"),
    SchemaField("field_id", "STRING", mode="REQUIRED"),
    SchemaField("crop_type", "STRING", mode="REQUIRED"),
    SchemaField("generated_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("analysis_date", "DATE", mode="REQUIRED", description="Last date of weather window"),
    SchemaField("final_urgency", "STRING", mode="REQUIRED", description="CRITICAL|HIGH|MODERATE|NONE"),
    SchemaField("recommended_water_mm", "FLOAT64", mode="NULLABLE"),
    SchemaField("cumulative_et0_mm", "FLOAT64", mode="NULLABLE"),
    SchemaField("cumulative_rain_mm", "FLOAT64", mode="NULLABLE"),
    SchemaField("net_water_deficit_mm", "FLOAT64", mode="NULLABLE"),
    SchemaField("triggered_rules", "STRING", mode="NULLABLE", description="JSON array of triggered rule IDs"),
    SchemaField("summary", "STRING", mode="NULLABLE"),
]

FIELD_REGISTRY_SCHEMA: list[SchemaField] = [
    SchemaField("field_id", "STRING", mode="REQUIRED"),
    SchemaField("farm_name", "STRING", mode="NULLABLE"),
    SchemaField("farmer_email", "STRING", mode="NULLABLE"),
    SchemaField("crop_type", "STRING", mode="REQUIRED"),
    SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
    SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
    SchemaField("area_hectares", "FLOAT64", mode="NULLABLE"),
    SchemaField("active", "BOOL", mode="REQUIRED"),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
]

USERS_SCHEMA: list[SchemaField] = [
    SchemaField("user_id", "STRING", mode="REQUIRED"),
    SchemaField("email", "STRING", mode="REQUIRED"),
    SchemaField("password_hash", "STRING", mode="REQUIRED"),
    SchemaField("is_verified", "BOOL", mode="REQUIRED"),
    SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]

# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def get_client() -> bigquery.Client:
    """Return a BigQuery client for the configured project."""
    return bigquery.Client(project=PROJECT_ID)


# ---------------------------------------------------------------------------
# Schema / Dataset management
# ---------------------------------------------------------------------------

def ensure_dataset(client: bigquery.Client | None = None) -> None:
    """
    Create the dataset if it does not already exist.
    Idempotent – safe to call on every cold-start.
    """
    client = client or get_client()
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = LOCATION
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info("Dataset %s.%s ready", PROJECT_ID, DATASET_ID)
    except Exception as exc:
        logger.error("Failed to create/verify dataset: %s", exc)
        raise


def _full_table_id(table_id: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_id}"


def _create_table_if_not_exists(
    client: bigquery.Client,
    table_id: str,
    schema: list[SchemaField],
    partition_field: str | None = None,
    clustering_fields: list[str] | None = None,
) -> bigquery.Table:
    """Create a BigQuery table with optional date partitioning + clustering."""
    full_id = _full_table_id(table_id)
    table = bigquery.Table(full_id, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    if clustering_fields:
        table.clustering_fields = clustering_fields

    try:
        table = client.create_table(table, exists_ok=True)
        logger.info("Table %s ready", full_id)
    except Exception as exc:
        logger.error("Failed to create table %s: %s", full_id, exc)
        raise
    return table


def initialize_schema(client: bigquery.Client | None = None) -> None:
    """
    Create all three tables (weather_data, irrigation_recommendations,
    field_registry) inside the dataset.  Idempotent.
    """
    client = client or get_client()
    ensure_dataset(client)

    _create_table_if_not_exists(
        client,
        WEATHER_TABLE_ID,
        WEATHER_SCHEMA,
        partition_field="date",
        clustering_fields=["field_id"],
    )
    _create_table_if_not_exists(
        client,
        RECOMMENDATIONS_TABLE_ID,
        RECOMMENDATIONS_SCHEMA,
        partition_field="analysis_date",
        clustering_fields=["field_id", "final_urgency"],
    )
    _create_table_if_not_exists(
        client,
        FIELD_REGISTRY_TABLE_ID,
        FIELD_REGISTRY_SCHEMA,
    )
    _create_table_if_not_exists(
        client,
        USERS_TABLE_ID,
        USERS_SCHEMA,
    )

# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def insert_weather_records(
    field_id: str,
    weather_records: list[dict[str, Any]],
    client: bigquery.Client | None = None,
) -> int:
    """
    Insert daily weather records into the weather_data table.

    Parameters
    ----------
    field_id : str
    weather_records : list[dict]
        Records as returned by fetch_nasa_data.fetch_weather_data().
    client : BigQuery client (optional).

    Returns
    -------
    int : Number of rows inserted.
    """
    import uuid

    client = client or get_client()
    table_ref = _full_table_id(WEATHER_TABLE_ID)
    now = _now_utc()

    rows = []
    for rec in weather_records:
        row = {
            "record_id": str(uuid.uuid4()),
            "field_id": field_id,
            "date": rec.get("date"),
            "ingested_at": now,
            "latitude": rec.get("latitude"),
            "longitude": rec.get("longitude"),
            "T2M_MAX": rec.get("T2M_MAX"),
            "T2M_MIN": rec.get("T2M_MIN"),
            "T2M": rec.get("T2M"),
            "RH2M": rec.get("RH2M"),
            "WS2M": rec.get("WS2M"),
            "ALLSKY_SFC_SW_DWN": rec.get("ALLSKY_SFC_SW_DWN"),
            "PRECTOTCORR": rec.get("PRECTOTCORR"),
        }
        rows.append(row)

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logger.error("BigQuery insert errors (weather_data): %s", errors)
        raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

    logger.info("Inserted %d weather records for field '%s'", len(rows), field_id)
    return len(rows)


def insert_recommendation(
    recommendation: Any,  # IrrigationRecommendation dataclass
    analysis_date: str,
    client: bigquery.Client | None = None,
) -> str:
    """
    Store an IrrigationRecommendation in the irrigation_recommendations table.

    Parameters
    ----------
    recommendation : IrrigationRecommendation
    analysis_date : str   ISO date string (YYYY-MM-DD) of the last day in the window.
    client : BigQuery client (optional).

    Returns
    -------
    str : recommendation_id (UUID).
    """
    import uuid

    client = client or get_client()
    table_ref = _full_table_id(RECOMMENDATIONS_TABLE_ID)
    rec_id = str(uuid.uuid4())

    row = {
        "recommendation_id": rec_id,
        "field_id": recommendation.field_id,
        "crop_type": recommendation.crop_type,
        "generated_at": _now_utc(),
        "analysis_date": analysis_date,
        "final_urgency": recommendation.final_urgency.value,
        "recommended_water_mm": recommendation.recommended_water_mm,
        "cumulative_et0_mm": recommendation.cumulative_et0_mm,
        "cumulative_rain_mm": recommendation.cumulative_rain_mm,
        "net_water_deficit_mm": recommendation.net_water_deficit_mm,
        "triggered_rules": json.dumps(
            [r.rule_id for r in recommendation.triggered_rules]
        ),
        "summary": recommendation.summary,
    }

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        logger.error("BigQuery insert errors (recommendations): %s", errors)
        raise RuntimeError(f"BigQuery streaming insert failed: {errors}")

    logger.info(
        "Recommendation %s stored for field '%s' (urgency=%s)",
        rec_id,
        recommendation.field_id,
        recommendation.final_urgency.value,
    )
    return rec_id


def upsert_field(
    field_data: dict[str, Any],
    client: bigquery.Client | None = None,
) -> None:
    """
    Insert or update a field record in field_registry.

    Because BigQuery streaming inserts are append-only, we simply insert
    a new row.  The query layer (get_field / list_active_fields) uses
    ROW_NUMBER() to surface only the latest version.
    """
    import uuid

    client = client or get_client()
    table_ref = _full_table_id(FIELD_REGISTRY_TABLE_ID)
    row = {
        "field_id": field_data.get("field_id", str(uuid.uuid4())),
        "farm_name": field_data.get("farm_name"),
        "farmer_email": field_data.get("farmer_email"),
        "crop_type": field_data.get("crop_type", "default"),
        "latitude": float(field_data["latitude"]),
        "longitude": float(field_data["longitude"]),
        "area_hectares": field_data.get("area_hectares"),
        "active": field_data.get("active", True),
        "created_at": field_data.get("created_at", _now_utc()),
        "updated_at": _now_utc(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"BigQuery streaming insert (field_registry) failed: {errors}")
    logger.info("Field '%s' upserted in registry", row["field_id"])


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def list_active_fields(farmer_email: str | None = None, client: bigquery.Client | None = None) -> list[dict[str, Any]]:
    """
    Return all active fields from field_registry (deduplicated, latest row wins).
    """
    client = client or get_client()
    sql = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY updated_at DESC) AS rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{FIELD_REGISTRY_TABLE_ID}`
        )
        SELECT
            field_id, farm_name, farmer_email, crop_type,
            latitude, longitude, area_hectares, active, created_at, updated_at
        FROM ranked
        WHERE rn = 1 AND active = TRUE
    """
    
    params = []
    if farmer_email:
        sql += " AND farmer_email = @email"
        params.append(bigquery.ScalarQueryParameter("email", "STRING", farmer_email))
        
    sql += " ORDER BY farm_name, field_id"
    
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    rows = list(client.query(sql, job_config=job_config).result())
    return [dict(row) for row in rows]


def get_weather_for_field(
    field_id: str,
    days: int = 7,
    client: bigquery.Client | None = None,
) -> list[dict[str, Any]]:
    """
    Retrieve the most recent ``days`` days of weather records for a field
    from BigQuery (instead of re-fetching from NASA).
    """
    client = client or get_client()
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.{WEATHER_TABLE_ID}`
        WHERE field_id = @field_id
        ORDER BY date DESC
        LIMIT @days
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("field_id", "STRING", field_id),
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return sorted([dict(row) for row in rows], key=lambda r: r["date"])


def get_latest_recommendations(
    urgency_filter: str | None = None,
    limit: int = 100,
    client: bigquery.Client | None = None,
) -> list[dict[str, Any]]:
    """
    Retrieve the most recent recommendation per field, optionally filtered
    by urgency level.

    Parameters
    ----------
    urgency_filter : str, optional
        One of CRITICAL, HIGH, MODERATE, NONE
    limit : int
        Maximum number of rows returned.
    """
    client = client or get_client()
    urgency_clause = ""
    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("limit_val", "INT64", limit),
    ]
    if urgency_filter:
        urgency_clause = "AND final_urgency = @urgency"
        params.append(bigquery.ScalarQueryParameter("urgency", "STRING", urgency_filter))

    sql = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY generated_at DESC) AS rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{RECOMMENDATIONS_TABLE_ID}`
        )
        SELECT
            recommendation_id, field_id, crop_type, generated_at,
            analysis_date, final_urgency, recommended_water_mm,
            cumulative_et0_mm, cumulative_rain_mm, net_water_deficit_mm,
            triggered_rules, summary
        FROM ranked
        WHERE rn = 1 {urgency_clause}
        ORDER BY generated_at DESC
        LIMIT @limit_val
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    return [dict(row) for row in rows]


def get_dashboard_summary(client: bigquery.Client | None = None) -> dict[str, Any]:
    """
    Return aggregated dashboard statistics:
        - total active fields
        - urgency distribution (last recommendation per field)
        - average ET₀ across all fields (last 7 days)
        - total recommended water volume (m³, assuming 1 ha)
    """
    client = client or get_client()
    sql = f"""
        WITH latest_recs AS (
            SELECT field_id, final_urgency, recommended_water_mm,
                   ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY generated_at DESC) AS rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{RECOMMENDATIONS_TABLE_ID}`
        )
        SELECT
            COUNT(DISTINCT field_id)                                    AS total_fields,
            COUNTIF(final_urgency = 'CRITICAL')                         AS critical_count,
            COUNTIF(final_urgency = 'HIGH')                             AS high_count,
            COUNTIF(final_urgency = 'MODERATE')                         AS moderate_count,
            COUNTIF(final_urgency = 'NONE')                             AS none_count,
            ROUND(AVG(recommended_water_mm), 2)                         AS avg_recommended_water_mm,
            ROUND(SUM(recommended_water_mm), 2)                         AS total_recommended_water_mm
        FROM latest_recs
        WHERE rn = 1
    """
    rows = list(client.query(sql).result())
    return dict(rows[0]) if rows else {}

def get_detailed_field_status(farmer_email: str | None = None, client: bigquery.Client | None = None) -> list[dict[str, Any]]:
    client = client or get_client()
    sql = f"""
        WITH latest_recs AS (
            SELECT field_id, final_urgency, recommended_water_mm, generated_at,
                   ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY generated_at DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{RECOMMENDATIONS_TABLE_ID}`
        ),
        latest_weather AS (
            SELECT field_id, T2M, PRECTOTCORR, RH2M, date, ingested_at,
                   ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY date DESC, ingested_at DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{WEATHER_TABLE_ID}`
            WHERE T2M IS NOT NULL
        ),
        ranked_fields AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY updated_at DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{FIELD_REGISTRY_TABLE_ID}`
        )
        SELECT 
            f.field_id,
            f.farm_name,
            f.latitude,
            f.longitude,
            f.crop_type,
            r.final_urgency,
            r.recommended_water_mm,
            r.generated_at,
            w.T2M as temp,
            w.PRECTOTCORR as rain,
            w.RH2M as moisture,
            w.date as weather_date
        FROM ranked_fields f
        LEFT JOIN latest_recs r ON f.field_id = r.field_id AND r.rn = 1
        LEFT JOIN latest_weather w ON f.field_id = w.field_id AND w.rn = 1
        WHERE f.rn = 1 AND f.active = TRUE
    """
    params = []
    if farmer_email:
        sql += " AND f.farmer_email = @email"
        params.append(bigquery.ScalarQueryParameter("email", "STRING", farmer_email))

    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    rows = list(client.query(sql, job_config=job_config).result())
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# User Authentication Methods for BigQuery
# ---------------------------------------------------------------------------

def create_user(email: str, password_hash: str, client: bigquery.Client | None = None) -> str:
    import uuid
    client = client or get_client()
    table_ref = _full_table_id(USERS_TABLE_ID)
    user_id = str(uuid.uuid4())
    
    row = {
        "user_id": user_id,
        "email": email,
        "password_hash": password_hash,
        "is_verified": False,
        "created_at": _now_utc(),
    }
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert (users) failed: {errors}")
    return user_id

def get_user_by_email(email: str, client: bigquery.Client | None = None) -> dict[str, Any] | None:
    client = client or get_client()
    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE_ID}`
        WHERE email = @email
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return dict(rows[0]) if rows else None

def verify_user(email: str, client: bigquery.Client | None = None) -> None:
    client = client or get_client()
    sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE_ID}`
        SET is_verified = TRUE
        WHERE email = @email
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    client.query(sql, job_config=job_config).result()

def update_user_password(email: str, password_hash: str, client: bigquery.Client | None = None) -> None:
    client = client or get_client()
    sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE_ID}`
        SET password_hash = @password_hash
        WHERE email = @email
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("password_hash", "STRING", password_hash),
            bigquery.ScalarQueryParameter("email", "STRING", email)
        ]
    )
    client.query(sql, job_config=job_config).result()
