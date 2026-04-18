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

try:
    from google.cloud import bigquery
    from google.cloud.bigquery import SchemaField
    from google.api_core.exceptions import AlreadyExists, NotFound
    _BIGQUERY_AVAILABLE = True
except ImportError:
    _BIGQUERY_AVAILABLE = False
    bigquery = None  # type: ignore
    SchemaField = None  # type: ignore
    AlreadyExists = Exception
    NotFound = Exception

logger = logging.getLogger(__name__)

# ===========================================================================
# LOCAL MODE — SQLite implementation (used when google-cloud-bigquery is
# not installed or GCP_PROJECT_ID is not set to a real project).
# All public functions auto-detect local mode and delegate here.
# ===========================================================================

import sqlite3
import uuid as _uuid_mod

_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "smart_irrigation.db")

_LOCAL_MODE = (not _BIGQUERY_AVAILABLE) or (os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") in ("your-gcp-project-id", ""))

def _get_local_conn():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _local_initialize_schema():
    conn = _get_local_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS weather_data (
            record_id TEXT PRIMARY KEY,
            field_id TEXT NOT NULL,
            date TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            latitude REAL, longitude REAL,
            T2M_MAX REAL, T2M_MIN REAL, T2M REAL, RH2M REAL,
            WS2M REAL, ALLSKY_SFC_SW_DWN REAL, PRECTOTCORR REAL
        );
        CREATE TABLE IF NOT EXISTS irrigation_recommendations (
            recommendation_id TEXT PRIMARY KEY,
            field_id TEXT NOT NULL,
            crop_type TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            analysis_date TEXT NOT NULL,
            final_urgency TEXT NOT NULL,
            recommended_water_mm REAL,
            cumulative_et0_mm REAL,
            cumulative_rain_mm REAL,
            net_water_deficit_mm REAL,
            triggered_rules TEXT,
            summary TEXT,
            fao_validation TEXT,
            fao_data_quality TEXT,
            fao_nearest_station TEXT,
            fao_reference_eto_mm REAL,
            fao_deviation_pct REAL
        );
        CREATE TABLE IF NOT EXISTS field_registry (
            field_id TEXT PRIMARY KEY,
            farm_name TEXT,
            farmer_email TEXT,
            crop_type TEXT NOT NULL,
            soil_type TEXT DEFAULT 'loam',
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            area_hectares REAL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_verified INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()
    logger.info("[LOCAL] SQLite schema ready at %s", _DB_PATH)

def _local_list_active_fields(farmer_email=None):
    conn = _get_local_conn()
    c = conn.cursor()
    if farmer_email:
        c.execute("SELECT * FROM field_registry WHERE active=1 AND farmer_email=?", (farmer_email,))
    else:
        c.execute("SELECT * FROM field_registry WHERE active=1")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    # Ensure boolean active field
    for r in rows:
        r["active"] = bool(r.get("active", 1))
    return rows

def _local_upsert_field(field_data: dict):
    conn = _get_local_conn()
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    c.execute("""
        INSERT INTO field_registry
            (field_id, farm_name, farmer_email, crop_type, soil_type,
             latitude, longitude, area_hectares, active, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(field_id) DO UPDATE SET
            farm_name=excluded.farm_name,
            farmer_email=excluded.farmer_email,
            crop_type=excluded.crop_type,
            soil_type=excluded.soil_type,
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            area_hectares=excluded.area_hectares,
            active=excluded.active,
            updated_at=excluded.updated_at
    """, (
        field_data["field_id"],
        field_data.get("farm_name"),
        field_data.get("farmer_email"),
        field_data.get("crop_type", "wheat"),
        field_data.get("soil_type", "loam"),
        field_data["latitude"],
        field_data["longitude"],
        field_data.get("area_hectares"),
        1 if field_data.get("active", True) else 0,
        now, now,
    ))
    conn.commit()
    conn.close()

def _local_insert_weather_records(field_id: str, records: list[dict]) -> int:
    conn = _get_local_conn()
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for rec in records:
        rid = str(_uuid_mod.uuid4())
        try:
            c.execute("""
                INSERT OR IGNORE INTO weather_data
                    (record_id, field_id, date, ingested_at, latitude, longitude,
                     T2M_MAX, T2M_MIN, T2M, RH2M, WS2M, ALLSKY_SFC_SW_DWN, PRECTOTCORR)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rid, field_id,
                str(rec.get("date", "")),
                now,
                rec.get("latitude"), rec.get("longitude"),
                rec.get("T2M_MAX"), rec.get("T2M_MIN"), rec.get("T2M"),
                rec.get("RH2M"), rec.get("WS2M"),
                rec.get("ALLSKY_SFC_SW_DWN"), rec.get("PRECTOTCORR"),
            ))
            inserted += c.rowcount
        except Exception as exc:
            logger.warning("[LOCAL] weather insert error: %s", exc)
    conn.commit()
    conn.close()
    return inserted

def _local_get_weather_for_field(field_id: str, days: int = 7) -> list[dict]:
    conn = _get_local_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM weather_data
        WHERE field_id=?
        ORDER BY date DESC
        LIMIT ?
    """, (field_id, days))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def _local_insert_recommendation(recommendation, analysis_date: str) -> str:
    conn = _get_local_conn()
    c = conn.cursor()
    rec_id = str(_uuid_mod.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    fao = getattr(recommendation, "fao_validation", {}) or {}
    c.execute("""
        INSERT INTO irrigation_recommendations
            (recommendation_id, field_id, crop_type, generated_at, analysis_date,
             final_urgency, recommended_water_mm, cumulative_et0_mm, cumulative_rain_mm,
             net_water_deficit_mm, triggered_rules, summary,
             fao_validation, fao_data_quality, fao_nearest_station,
             fao_reference_eto_mm, fao_deviation_pct)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        rec_id,
        recommendation.field_id,
        recommendation.crop_type,
        now,
        analysis_date,
        recommendation.final_urgency.value,
        recommendation.recommended_water_mm,
        recommendation.cumulative_et0_mm,
        recommendation.cumulative_rain_mm,
        recommendation.net_water_deficit_mm,
        json.dumps([r.rule_id for r in recommendation.triggered_rules]),
        recommendation.summary,
        json.dumps(fao, default=str) if fao else None,
        fao.get("data_quality_overall") if fao else None,
        fao.get("nearest_station") if fao else None,
        fao.get("avg_fao_reference_eto_mm") if fao else None,
        fao.get("avg_deviation_pct") if fao else None,
    ))
    conn.commit()
    conn.close()
    return rec_id

def _local_get_latest_recommendations(urgency_filter=None, limit=100, farmer_email=None):
    conn = _get_local_conn()
    c = conn.cursor()
    if farmer_email:
        if urgency_filter:
            c.execute("""
                SELECT r.*, f.farm_name, f.latitude, f.longitude, f.soil_type
                FROM irrigation_recommendations r
                JOIN field_registry f ON r.field_id = f.field_id
                WHERE f.farmer_email=? AND r.final_urgency=?
                ORDER BY r.generated_at DESC LIMIT ?
            """, (farmer_email, urgency_filter, limit))
        else:
            c.execute("""
                SELECT r.*, f.farm_name, f.latitude, f.longitude, f.soil_type
                FROM irrigation_recommendations r
                JOIN field_registry f ON r.field_id = f.field_id
                WHERE f.farmer_email=?
                ORDER BY r.generated_at DESC LIMIT ?
            """, (farmer_email, limit))
    else:
        if urgency_filter:
            c.execute("""
                SELECT * FROM irrigation_recommendations
                WHERE final_urgency=? ORDER BY generated_at DESC LIMIT ?
            """, (urgency_filter, limit))
        else:
            c.execute("""
                SELECT * FROM irrigation_recommendations
                ORDER BY generated_at DESC LIMIT ?
            """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def _local_get_dashboard_summary(farmer_email=None):
    conn = _get_local_conn()
    c = conn.cursor()
    if farmer_email:
        c.execute("""
            SELECT r.final_urgency, COUNT(*) as count
            FROM irrigation_recommendations r
            JOIN field_registry f ON r.field_id = f.field_id
            WHERE f.farmer_email=?
            GROUP BY r.final_urgency
        """, (farmer_email,))
    else:
        c.execute("""
            SELECT final_urgency, COUNT(*) as count
            FROM irrigation_recommendations
            GROUP BY final_urgency
        """)
    counts = {row["final_urgency"]: row["count"] for row in c.fetchall()}
    if farmer_email:
        c.execute("SELECT COUNT(*) as n FROM field_registry WHERE active=1 AND farmer_email=?",
                  (farmer_email,))
    else:
        c.execute("SELECT COUNT(*) as n FROM field_registry WHERE active=1")
    total_fields = c.fetchone()["n"]
    conn.close()
    return {
        "total_fields": total_fields,
        "critical": counts.get("CRITICAL", 0),
        "high": counts.get("HIGH", 0),
        "moderate": counts.get("MODERATE", 0),
        "none": counts.get("NONE", 0),
        "total_recommendations": sum(counts.values()),
    }

def _local_get_detailed_field_status(farmer_email=None):
    conn = _get_local_conn()
    c = conn.cursor()
    if farmer_email:
        c.execute("SELECT * FROM field_registry WHERE active=1 AND farmer_email=?", (farmer_email,))
    else:
        c.execute("SELECT * FROM field_registry WHERE active=1")
    fields = [dict(r) for r in c.fetchall()]
    result = []
    for f in fields:
        fid = f["field_id"]
        c.execute("""
            SELECT * FROM irrigation_recommendations
            WHERE field_id=? ORDER BY generated_at DESC LIMIT 1
        """, (fid,))
        rec = c.fetchone()
        c.execute("""
            SELECT T2M, RH2M, PRECTOTCORR FROM weather_data
            WHERE field_id=? ORDER BY date DESC LIMIT 1
        """, (fid,))
        wx = c.fetchone()
        row = {
            "field_id": fid,
            "farm_name": f.get("farm_name"),
            "crop_type": f.get("crop_type"),
            "soil_type": f.get("soil_type"),
            "latitude": f.get("latitude"),
            "longitude": f.get("longitude"),
            "final_urgency": rec["final_urgency"] if rec else None,
            "recommended_water_mm": rec["recommended_water_mm"] if rec else None,
            "generated_at": rec["generated_at"] if rec else None,
            "analysis_date": rec["analysis_date"] if rec else None,
            "moisture": wx["RH2M"] if wx else None,
            "temp": wx["T2M"] if wx else None,
            "rain": wx["PRECTOTCORR"] if wx else None,
        }
        result.append(row)
    conn.close()
    return result

def _local_create_user(email: str, password_hash: str):
    conn = _get_local_conn()
    c = conn.cursor()
    uid = str(_uuid_mod.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    c.execute("""
        INSERT OR IGNORE INTO users (user_id, email, password_hash, is_verified, created_at)
        VALUES (?,?,?,0,?)
    """, (uid, email, password_hash, now))
    conn.commit()
    conn.close()

def _local_get_user_by_email(email: str):
    conn = _get_local_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if row:
        d = dict(row)
        d["is_verified"] = bool(d["is_verified"])
        return d
    return None

def _local_verify_user(email: str):
    conn = _get_local_conn()
    c = conn.cursor()
    c.execute("UPDATE users SET is_verified=1 WHERE email=?", (email,))
    conn.commit()
    conn.close()

def _local_update_user_password(email: str, password_hash: str):
    conn = _get_local_conn()
    c = conn.cursor()
    c.execute("UPDATE users SET password_hash=? WHERE email=?", (password_hash, email))
    conn.commit()
    conn.close()

# ===========================================================================
# END LOCAL MODE
# ===========================================================================


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

if _BIGQUERY_AVAILABLE:
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
        SchemaField("fao_validation", "STRING", mode="NULLABLE", description="JSON: FAO CLIMWAT cross-validation result"),
        SchemaField("fao_data_quality", "STRING", mode="NULLABLE", description="FAO validation quality: GOOD | WARNING | CRITICAL"),
        SchemaField("fao_nearest_station", "STRING", mode="NULLABLE", description="Nearest FAO CLIMWAT station name"),
        SchemaField("fao_reference_eto_mm", "FLOAT64", mode="NULLABLE", description="FAO monthly reference ET₀ (mm/day)"),
        SchemaField("fao_deviation_pct", "FLOAT64", mode="NULLABLE", description="ET₀ deviation from FAO reference (%)"),
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
    
else:
    WEATHER_SCHEMA = []
    RECOMMENDATIONS_SCHEMA = []
    FIELD_REGISTRY_SCHEMA = []
    USERS_SCHEMA = []

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


def initialize_schema(client=None) -> None:
    if _LOCAL_MODE:
        _local_initialize_schema()
        return
    _bq_initialize_schema(client)

def _bq_initialize_schema(client: bigquery.Client | None = None) -> None:
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


def insert_weather_records(field_id: str, records: list[dict]) -> int:
    if _LOCAL_MODE:
        return _local_insert_weather_records(field_id, records)
    return _bq_insert_weather_records(field_id, records)

def _bq_insert_weather_records(
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


def insert_recommendation(recommendation, analysis_date: str) -> str:
    if _LOCAL_MODE:
        return _local_insert_recommendation(recommendation, analysis_date)
    return _bq_insert_recommendation(recommendation, analysis_date)

def _bq_insert_recommendation(
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
        "fao_validation": json.dumps(recommendation.fao_validation, default=str)
            if recommendation.fao_validation else None,
        "fao_data_quality": recommendation.fao_validation.get("data_quality_overall")
            if recommendation.fao_validation else None,
        "fao_nearest_station": recommendation.fao_validation.get("nearest_station")
            if recommendation.fao_validation else None,
        "fao_reference_eto_mm": recommendation.fao_validation.get("avg_fao_reference_eto_mm")
            if recommendation.fao_validation else None,
        "fao_deviation_pct": recommendation.fao_validation.get("avg_deviation_pct")
            if recommendation.fao_validation else None,
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


def upsert_field(field_data: dict) -> None:
    if _LOCAL_MODE:
        _local_upsert_field(field_data)
        return
    _bq_upsert_field(field_data)

def _bq_upsert_field(
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

def list_active_fields(farmer_email=None):
    if _LOCAL_MODE:
        return _local_list_active_fields(farmer_email)
    return _bq_list_active_fields(farmer_email)

def _bq_list_active_fields(farmer_email: str | None = None, client: bigquery.Client | None = None) -> list[dict[str, Any]]:
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


def get_weather_for_field(field_id: str, days: int = 7) -> list[dict]:
    if _LOCAL_MODE:
        return _local_get_weather_for_field(field_id, days)
    return _bq_get_weather_for_field(field_id, days)

def _bq_get_weather_for_field(
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


def get_latest_recommendations(urgency_filter=None, limit=100, farmer_email=None):
    if _LOCAL_MODE:
        return _local_get_latest_recommendations(urgency_filter, limit, farmer_email)
    return _bq_get_latest_recommendations(urgency_filter, limit, farmer_email)

def _bq_get_latest_recommendations(
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


def get_dashboard_summary(farmer_email=None):
    if _LOCAL_MODE:
        return _local_get_dashboard_summary(farmer_email)
    return _bq_get_dashboard_summary(farmer_email)

def _bq_get_dashboard_summary(client: bigquery.Client | None = None) -> dict[str, Any]:
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

def get_detailed_field_status(farmer_email=None):
    if _LOCAL_MODE:
        return _local_get_detailed_field_status(farmer_email)
    return _bq_get_detailed_field_status(farmer_email)

def _bq_get_detailed_field_status(farmer_email: str | None = None, client: bigquery.Client | None = None) -> list[dict[str, Any]]:
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

def create_user(email: str, password_hash: str):
    if _LOCAL_MODE:
        _local_create_user(email, password_hash)
        return
    _bq_create_user(email, password_hash)

def _bq_create_user(email: str, password_hash: str, client: bigquery.Client | None = None) -> str:
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

def get_user_by_email(email: str):
    if _LOCAL_MODE:
        return _local_get_user_by_email(email)
    return _bq_get_user_by_email(email)

def _bq_get_user_by_email(email: str, client: bigquery.Client | None = None) -> dict[str, Any] | None:
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

def verify_user(email: str):
    if _LOCAL_MODE:
        _local_verify_user(email)
        return
    _bq_verify_user(email)

def _bq_verify_user(email: str, client: bigquery.Client | None = None) -> None:
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

def update_user_password(email: str, password_hash: str):
    if _LOCAL_MODE:
        _local_update_user_password(email, password_hash)
        return
    _bq_update_user_password(email, password_hash)

def _bq_update_user_password(email: str, password_hash: str, client: bigquery.Client | None = None) -> None:
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
