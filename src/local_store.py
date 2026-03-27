"""
local_store.py
--------------
SQLite backend for local execution.
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone
from typing import Any
import uuid
import os

logger = logging.getLogger(__name__)

DB_PATH = "smart_irrigation.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_schema():
    with get_conn() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS weather_data (
                record_id TEXT PRIMARY KEY,
                field_id TEXT NOT NULL,
                date TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                T2M_MAX REAL,
                T2M_MIN REAL,
                T2M REAL,
                RH2M REAL,
                WS2M REAL,
                ALLSKY_SFC_SW_DWN REAL,
                PRECTOTCORR REAL
            )
        ''')
        conn.execute('''
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
                simulated_moisture_percent REAL
            )
        ''')
        
        try:
            conn.execute('ALTER TABLE irrigation_recommendations ADD COLUMN simulated_moisture_percent REAL')
        except sqlite3.OperationalError:
            pass

        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_verified BOOLEAN DEFAULT 0,
                created_at TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS email_tokens (
                token TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                token_type TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS field_registry (
                field_id TEXT PRIMARY KEY,
                farm_name TEXT,
                farmer_email TEXT,
                crop_type TEXT NOT NULL,
                soil_type TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                area_hectares REAL,
                active BOOLEAN NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        conn.commit()

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def insert_weather_records(field_id: str, weather_records: list[dict[str, Any]]) -> int:
    with get_conn() as conn:
        for rec in weather_records:
            conn.execute('''
                INSERT INTO weather_data (
                    record_id, field_id, date, ingested_at, latitude, longitude,
                    T2M_MAX, T2M_MIN, T2M, RH2M, WS2M, ALLSKY_SFC_SW_DWN, PRECTOTCORR
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(uuid.uuid4()), field_id, rec.get("date"), _now_utc(),
                rec.get("latitude"), rec.get("longitude"), rec.get("T2M_MAX"),
                rec.get("T2M_MIN"), rec.get("T2M"), rec.get("RH2M"), rec.get("WS2M"),
                rec.get("ALLSKY_SFC_SW_DWN"), rec.get("PRECTOTCORR")
            ))
        conn.commit()
    return len(weather_records)

def insert_recommendation(recommendation: Any, analysis_date: str) -> str:
    rec_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute('''
            INSERT INTO irrigation_recommendations (
                recommendation_id, field_id, crop_type, generated_at, analysis_date,
                final_urgency, recommended_water_mm, cumulative_et0_mm, cumulative_rain_mm,
                net_water_deficit_mm, triggered_rules, summary, simulated_moisture_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            rec_id, recommendation.field_id, recommendation.crop_type, _now_utc(), analysis_date,
            recommendation.final_urgency.value, recommendation.recommended_water_mm,
            recommendation.cumulative_et0_mm, recommendation.cumulative_rain_mm,
            recommendation.net_water_deficit_mm, json.dumps([r.rule_id for r in recommendation.triggered_rules]),
            recommendation.summary, getattr(recommendation, "simulated_moisture_percent", 50.0)
        ))
        conn.commit()
    return rec_id

def upsert_field(field_data: dict[str, Any]) -> None:
    field_id = field_data.get("field_id", str(uuid.uuid4()))
    with get_conn() as conn:
        cur = conn.execute("SELECT field_id FROM field_registry WHERE field_id = ?", (field_id,))
        exists = cur.fetchone() is not None
        if exists:
            conn.execute('''
                UPDATE field_registry SET
                    farm_name = ?, farmer_email = ?, crop_type = ?, soil_type = ?, latitude = ?, longitude = ?,
                    area_hectares = ?, active = ?, updated_at = ?
                WHERE field_id = ?
            ''', (
                field_data.get("farm_name"), field_data.get("farmer_email"),
                field_data.get("crop_type", "default"), field_data.get("soil_type", "loam"), float(field_data["latitude"]),
                float(field_data["longitude"]), field_data.get("area_hectares"),
                field_data.get("active", True), _now_utc(), field_id
            ))
        else:
            conn.execute('''
                INSERT INTO field_registry (
                    field_id, farm_name, farmer_email, crop_type, soil_type, latitude, longitude,
                    area_hectares, active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                field_id, field_data.get("farm_name"), field_data.get("farmer_email"),
                field_data.get("crop_type", "default"), field_data.get("soil_type", "loam"), float(field_data["latitude"]),
                float(field_data["longitude"]), field_data.get("area_hectares"),
                field_data.get("active", True), field_data.get("created_at", _now_utc()), _now_utc()
            ))
        conn.commit()

def list_active_fields(farmer_email: str | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        query = "SELECT * FROM field_registry WHERE active = 1"
        params = []
        if farmer_email:
            query += " AND farmer_email = ?"
            params.append(farmer_email)
        query += " ORDER BY farm_name, field_id"
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_weather_for_field(field_id: str, days: int = 7) -> list[dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.execute('''
            SELECT * FROM weather_data
            WHERE field_id = ?
            ORDER BY date DESC
            LIMIT ?
        ''', (field_id, days))
        rows = [dict(row) for row in cur.fetchall()]
        rows.sort(key=lambda r: r["date"])
        return rows

def get_detailed_field_status(farmer_email: str | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        query = '''
            WITH latest_recs AS (
                SELECT field_id, final_urgency, recommended_water_mm, generated_at, simulated_moisture_percent,
                       ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY generated_at DESC) as rn
                FROM irrigation_recommendations
            ),
            latest_weather AS (
                SELECT field_id, T2M, PRECTOTCORR, RH2M, date, ingested_at,
                       ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY date DESC, ingested_at DESC) as rn
                FROM weather_data
                WHERE T2M IS NOT NULL
            )
            SELECT 
                f.field_id,
                f.farm_name,
                f.latitude,
                f.longitude,
                f.crop_type,
                f.soil_type,
                r.final_urgency,
                r.recommended_water_mm,
                r.generated_at,
                w.T2M as temp,
                w.PRECTOTCORR as rain,
                COALESCE(r.simulated_moisture_percent, w.RH2M, 50.0) as moisture,
                w.date as weather_date
            FROM field_registry f
            LEFT JOIN latest_recs r ON f.field_id = r.field_id AND r.rn = 1
            LEFT JOIN latest_weather w ON f.field_id = w.field_id AND w.rn = 1
            WHERE f.active = 1
        '''
        params = []
        if farmer_email:
            query += " AND f.farmer_email = ?"
            params.append(farmer_email)
            
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_latest_recommendations(urgency_filter: str | None = None, limit: int = 100, farmer_email: str | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        query = '''
            WITH ranked AS (
                SELECT r.*, f.soil_type, ROW_NUMBER() OVER (PARTITION BY r.field_id ORDER BY r.generated_at DESC) as rn
                FROM irrigation_recommendations r
                LEFT JOIN field_registry f ON r.field_id = f.field_id
            )
            SELECT * FROM ranked
            WHERE rn = 1
        '''
        params = []
        if farmer_email:
            query += " AND farmer_email = ?"
            params.append(farmer_email)
            
        if urgency_filter:
            query += " AND final_urgency = ?"
            params.append(urgency_filter)
        query += " ORDER BY generated_at DESC LIMIT ?"
        params.append(limit)
        
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_dashboard_summary(farmer_email: str | None = None) -> dict[str, Any]:
    with get_conn() as conn:
        query = '''
            WITH latest_recs AS (
                SELECT r.field_id, r.final_urgency, r.recommended_water_mm,
                       ROW_NUMBER() OVER (PARTITION BY r.field_id ORDER BY r.generated_at DESC) as rn
                FROM irrigation_recommendations r
                LEFT JOIN field_registry f ON r.field_id = f.field_id
                WHERE 1=1
        '''
        params = []
        if farmer_email:
            query += " AND f.farmer_email = ?"
            params.append(farmer_email)
            
        query += '''
            )
            SELECT
                (SELECT COUNT(*) FROM field_registry WHERE active = 1'''
                
        if farmer_email:
            query += " AND farmer_email = ?"
            params.append(farmer_email)
            
        query += ''') as total_fields,
                SUM(CASE WHEN final_urgency = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
                SUM(CASE WHEN final_urgency = 'HIGH' THEN 1 ELSE 0 END) as high_count,
                SUM(CASE WHEN final_urgency = 'MODERATE' THEN 1 ELSE 0 END) as moderate_count,
                SUM(CASE WHEN final_urgency = 'NONE' THEN 1 ELSE 0 END) as none_count,
                ROUND(AVG(recommended_water_mm), 2) as avg_recommended_water_mm,
                ROUND(SUM(recommended_water_mm), 2) as total_recommended_water_mm
            FROM latest_recs
            WHERE rn = 1
        '''
        cur = conn.execute(query, params)
        row = cur.fetchone()
        
        # If no records exist, the SUMs might be None
        result = dict(row) if row else {}
        for k, v in result.items():
            if v is None:
                result[k] = 0
        return result

# --- User Authentication Methods ---
def create_user(email: str, password_hash: str) -> str:
    user_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute('''
            INSERT INTO users (user_id, email, password_hash, is_verified, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, email, password_hash, False, _now_utc()))
        conn.commit()
    return user_id

def get_user_by_email(email: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM users WHERE email = ?", (email,))
        row = cur.fetchone()
        return dict(row) if row else None

def verify_user(email: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE users SET is_verified = 1 WHERE email = ?", (email,))
        conn.commit()

def create_email_token(email: str, token: str, token_type: str, expires_at: str) -> None:
    with get_conn() as conn:
        conn.execute('''
            INSERT INTO email_tokens (token, email, token_type, expires_at)
            VALUES (?, ?, ?, ?)
        ''', (token, email, token_type, expires_at))
        conn.commit()

def get_email_token(token: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM email_tokens WHERE token = ?", (token,))
        row = cur.fetchone()
        return dict(row) if row else None

def delete_email_token(token: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM email_tokens WHERE token = ?", (token,))
        conn.commit()

def update_user_password(email: str, password_hash: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE users SET password_hash = ? WHERE email = ?", (password_hash, email))
        conn.commit()
