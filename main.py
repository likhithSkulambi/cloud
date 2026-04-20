"""
main.py
-------
A standalone Flask server that runs the Smart Irrigation Advisor locally.
It serves a static web dashboard and provides the REST endpoints, bypassing GCP.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from datetime import date, timedelta
import os

from src.bigquery_store import (
    initialize_schema,
    list_active_fields,
    insert_weather_records,
    insert_recommendation,
    get_weather_for_field,
    get_latest_recommendations,
    get_dashboard_summary,
    upsert_field,
    get_detailed_field_status,
    create_user,
    get_user_by_email,
    verify_user,
    update_user_password
)
from src.fetch_nasa_data import fetch_weather_data
from src.irrigation_rules import evaluate_irrigation_rules
from src.email_service import send_irrigation_alert, send_verification_email, send_reset_email, is_dev_mode

import re
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

app = Flask(__name__, static_folder="dashboard/public")
CORS(app)

SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "super_secret_fallback_key")
serializer = URLSafeTimedSerializer(SECRET_KEY)

WEATHER_LOOKBACK_DAYS = 7

# Initialize app on startup (for Cloud Run and local deployment)
_initialized = False

@app.before_request
def before_request():
    global _initialized
    if not _initialized:
        try:
            initialize()
            _initialized = True
            print("[INIT] Smart Irrigation Advisor initialized successfully")
        except Exception as e:
            print(f"[INIT ERROR] Failed to initialize app: {e}")
            _initialized = False


def initialize():
    # Initialize the local SQLite schema
    initialize_schema()
    
    # Check if there are active fields; if not, add a demo field
    fields = list_active_fields()
    if not fields:
        print("No active fields found. Adding demo fields...")
        upsert_field({
            "field_id": "demo-field-001",
            "farm_name": "Green Acres Demo",
            "farmer_email": "demo@example.com",
            "crop_type": "wheat",
            "soil_type": "loam",
            "latitude": 38.6270,   # Example mid-latitude
            "longitude": -90.1994,
            "area_hectares": 5.0,
            "active": True,
        })
        upsert_field({
            "field_id": "demo-field-002",
            "farm_name": "Valley View Demo",
            "farmer_email": "demo2@example.com",
            "crop_type": "maize",
            "soil_type": "sandy",
            "latitude": 36.7783,   # California Central Valley Example
            "longitude": -119.4179,
            "area_hectares": 12.0,
            "active": True,
        })

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

@app.route("/api/auth/register", methods=["POST"])
def auth_register():
    data = request.json
    email = data.get("email")
    password = data.get("password")
    if not email or not password or not is_valid_email(email):
        return jsonify({"status": "error", "message": "Valid email and password required"}), 400
    
    existing = get_user_by_email(email)
    if existing:
        return jsonify({"status": "error", "message": "Email already registered"}), 400
        
    pwd_hash = generate_password_hash(password)
    create_user(email, pwd_hash)

    # Dev mode: auto-verify account so user can log in immediately without email
    if is_dev_mode():
        verify_user(email)
        token = serializer.dumps(email, salt='email-verify')
        print(f"[DEV MODE] Verification skipped — account auto-verified for {email}")
        print(f"[DEV MODE] Verify link (optional): http://127.0.0.1:8000/api/auth/verify?token={token}")
        return jsonify({"status": "ok", "message": "Registration successful! You can now log in."})
    
    # Production mode: send real verification email
    token = serializer.dumps(email, salt='email-verify')
    send_verification_email(email, token)
    return jsonify({"status": "ok", "message": "Registration successful. Please check your email to verify."})

@app.route("/api/auth/verify", methods=["GET"])
def auth_verify():
    token = request.args.get("token")
    if not token:
        return jsonify({"status": "error", "message": "Missing token"}), 400
    try:
        email = serializer.loads(token, salt='email-verify', max_age=86400) # 24 hours
        user = get_user_by_email(email)
        if not user:
            return """<html><body style="font-family:Arial;text-align:center;padding:50px;"><h2 style="color:#d32f2f;">Error: User not found</h2><p><a href="/">Return to Dashboard</a></p></body></html>""", 404
        if user["is_verified"]:
            return """<html><body style="font-family:Arial;text-align:center;padding:50px;"><h2 style="color:#4caf50;">✓ Email already verified!</h2><p>Your account is all set. <a href="/">Log in to Dashboard</a></p></body></html>"""

        verify_user(email)
        return """<html><body style="font-family:Arial;text-align:center;padding:50px;"><h2 style="color:#4caf50;">✓ Email successfully verified!</h2><p>Your email has been confirmed. <a href="/">Go to Dashboard</a></p></body></html>"""
    except SignatureExpired:
        return """<html><body style="font-family:Arial;text-align:center;padding:50px;"><h2 style="color:#d32f2f;">Error: Verification link expired</h2><p>Please <a href="/">request a new verification email</a>.</p></body></html>""", 400
    except BadSignature:
        return """<html><body style="font-family:Arial;text-align:center;padding:50px;"><h2 style="color:#d32f2f;">Error: Invalid verification link</h2><p>The link may be corrupted. Please check your email and try again, or <a href="/">contact support</a>.</p></body></html>""", 400

@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.json or {}
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"status": "error", "message": "Email and password are required"}), 400

    user = get_user_by_email(email)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"status": "error", "message": "Invalid email or password"}), 401

    if not user.get("is_verified", False):
        if is_dev_mode():
            verify_user(email)
            user = get_user_by_email(email) or user
        else:
            return jsonify({"status": "error", "message": "Please verify your email before logging in"}), 403

    auth_token = serializer.dumps(email, salt="auth-token")
    return jsonify({"status": "ok", "token": auth_token, "email": email})

@app.route("/api/auth/forgot-password", methods=["POST"])
def auth_forgot_password():
    data = request.json
    email = data.get("email")
    if not email:
        return jsonify({"status": "error", "message": "Email is required"}), 400
    
    user = get_user_by_email(email)
    if user:
        token = serializer.dumps(email, salt='password-reset')
        send_reset_email(email, token)
        
    return jsonify({"status": "ok", "message": "If that email is registered, a password reset link has been sent."})

@app.route("/api/auth/reset-password", methods=["POST"])
def auth_reset_password():
    data = request.json
    token = data.get("token")
    new_password = data.get("password")
    if not token or not new_password:
        return jsonify({"status": "error", "message": "Token and new password required"}), 400
    
    try:
        email = serializer.loads(token, salt='password-reset', max_age=3600) # 1 hour
        user = get_user_by_email(email)
        if not user:
            return jsonify({"status": "error", "message": "User not found"}), 404
            
        pwd_hash = generate_password_hash(new_password)
        update_user_password(email, pwd_hash)
        return jsonify({"status": "ok", "message": "Password successfully reset. You can now log in."})
    except SignatureExpired:
        return jsonify({"status": "error", "message": "Reset link has expired"}), 400
    except BadSignature:
        return jsonify({"status": "error", "message": "Invalid reset link"}), 400

@app.route("/api/weather/fetch", methods=["GET"])
def fetch_weather_endpoint():
    """Fetches weather data for all active fields."""
    days = int(request.args.get("days", WEATHER_LOOKBACK_DAYS))
    email = request.args.get("email")
    active_fields = list_active_fields(farmer_email=email)
    
    today = date.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    
    total_inserted = 0
    errors = []
    
    for field in active_fields:
        field_id = field["field_id"]
        lat = field["latitude"]
        lon = field["longitude"]
        
        try:
            records = fetch_weather_data(lat, lon, start_date, end_date)
            if records:
                n = insert_weather_records(field_id, records)
                total_inserted += n
        except Exception as exc:
            errors.append(f"Field {field_id}: {exc}")
            
    return jsonify({
        "status": "ok" if not errors else "partial",
        "fields_processed": len(active_fields),
        "records_inserted": total_inserted,
        "errors": errors
    })

@app.route("/api/recommendations/evaluate", methods=["GET"])
def evaluate_recommendations_endpoint():
    """Evaluates irrigation rules for all active fields."""
    days = int(request.args.get("days", WEATHER_LOOKBACK_DAYS))
    email = request.args.get("email")
    active_fields = list_active_fields(farmer_email=email)
    recommendations_out = []
    errors = []
    should_send_auto_alert = False
    analysis_date = (date.today() - timedelta(days=1)).isoformat()
    
    for field in active_fields:
        field_id = field["field_id"]
        crop_type = field.get("crop_type", "default")
        lat = field.get("latitude", 20.0)
        farmer_email = field.get("farmer_email")
        farm_name = field.get("farm_name", "")
        
        try:
            weather_records = get_weather_for_field(field_id, days=days)
            if not weather_records:
                end_date = date.today() - timedelta(days=1)
                start_date = end_date - timedelta(days=days - 1)
                weather_records = fetch_weather_data(lat, field["longitude"], start_date, end_date)
                # Always persist fetched records so email/dashboard can read them
                if weather_records:
                    insert_weather_records(field_id, weather_records)

            if not weather_records:
                errors.append(f"Field {field_id}: no weather metadata")
                continue
                
            recommendation = evaluate_irrigation_rules(
                field_id=field_id,
                crop_type=crop_type,
                weather_records=weather_records,
                latitude=lat,
            )
            
            rec_id = insert_recommendation(recommendation, analysis_date)
            
            alert_result = False
            if recommendation.final_urgency.value in ["HIGH", "CRITICAL"]:
                should_send_auto_alert = True
                alert_result = True  # We will send it at the end
            
            recommendations_out.append({
                "field_id": field_id,
                "recommendation_id": rec_id,
                "urgency": recommendation.final_urgency.value,
                "recommended_water_mm": recommendation.recommended_water_mm,
                "net_deficit_mm": recommendation.net_water_deficit_mm,
                "triggered_rules": [r.rule_id for r in recommendation.triggered_rules],
                "alert": alert_result,
                "fao_validation": recommendation.fao_validation,
            })
            
        except Exception as exc:
            errors.append(f"Field {field_id}: {exc}")

    auto_alert_msg = ""
    # If any field has HIGH or CRITICAL urgency, auto-send the consolidated alert email
    if should_send_auto_alert and email:
        ok, msg = _dispatch_alert(email)
        auto_alert_msg = msg if ok else "Auto-alert email failed"

    return jsonify({
        "status": "ok" if not errors else "partial",
        "fields_processed": len(active_fields),
        "recommendations": recommendations_out,
        "errors": errors,
        "auto_alert_msg": auto_alert_msg
    })

@app.route("/api/fields/add", methods=["POST"])
def add_field_endpoint():
    try:
        data = request.json
        if not data:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400
            
        required_keys = ["farm_name", "crop_type", "soil_type", "latitude", "longitude", "area_hectares"]
        for k in required_keys:
            if k not in data or data[k] is None or str(data[k]).strip() == "":
                return jsonify({"status": "error", "message": f"Missing required field: {k}"}), 400
                
        # Generate a unique field ID
        import uuid
        field_id = f"field-{uuid.uuid4().hex[:8]}"
        
        field_data = {
            "field_id": field_id,
            "farm_name": data["farm_name"],
            "farmer_email": data.get("farmer_email", ""),
            "crop_type": data["crop_type"],
            "soil_type": data["soil_type"],
            "latitude": float(data["latitude"]),
            "longitude": float(data["longitude"]),
            "area_hectares": float(data["area_hectares"]),
            "active": True
        }
        upsert_field(field_data)
        
        return jsonify({"status": "ok", "message": "Field added successfully", "field_id": field_id})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500

@app.route("/api/fields", methods=["GET"])
def get_fields_endpoint():
    """Returns all active fields for the given user."""
    email = request.args.get("email")
    if not email:
        return jsonify({"status": "error", "message": "Email is required"}), 400
    fields = list_active_fields(farmer_email=email)
    return jsonify({"status": "ok", "fields": fields})

@app.route("/api/fields/status", methods=["GET"])
def get_fields_status_endpoint():
    """Returns detailed status including weather and recommendations for active fields."""
    email = request.args.get("email")
    if not email:
        return jsonify({"status": "error", "message": "Email is required"}), 400
    status_data = get_detailed_field_status(farmer_email=email)
    return jsonify({"status": "ok", "data": status_data})

def _dispatch_alert(email):
    fields = get_detailed_field_status(farmer_email=email)
    if not fields:
        return jsonify({"status": "error", "message": "No fields found for this user"}), 404

    URGENCY_BG    = {"CRITICAL": "#7f1d1d", "HIGH": "#7c2d12", "MODERATE": "#713f12", "NONE": "#14532d"}
    URGENCY_LIGHT = {"CRITICAL": "#fee2e2", "HIGH": "#ffedd5", "MODERATE": "#fef9c3", "NONE": "#dcfce7"}
    URGENCY_TEXT  = {"CRITICAL": "#ef4444", "HIGH": "#f97316", "MODERATE": "#ca8a04", "NONE": "#16a34a"}
    URGENCY_EMOJI = {"CRITICAL": "🚨", "HIGH": "⚠️", "MODERATE": "💧", "NONE": "✅"}
    URGENCY_LABEL = {
        "CRITICAL": "CRITICAL — Irrigate Immediately",
        "HIGH":     "HIGH — Irrigate Within 24 Hours",
        "MODERATE": "MODERATE — Irrigate Within 48 Hours",
        "NONE":     "NO ACTION — Soil Moisture is Adequate",
    }

    cards_html = ""
    summary_counts = {"CRITICAL": 0, "HIGH": 0, "MODERATE": 0, "NONE": 0}

    for idx, f in enumerate(fields):
        urgency      = (f.get("final_urgency") or "NONE").upper()
        summary_counts[urgency] = summary_counts.get(urgency, 0) + 1
        farm_name    = f.get("farm_name") or f"Field {idx + 1}"
        crop         = (f.get("crop_type") or "Unknown").capitalize()
        water_mm     = f.get("recommended_water_mm")
        moisture     = f.get("moisture")
        temp         = f.get("temp")
        rain         = f.get("rain")
        lat          = f.get("latitude")
        lon          = f.get("longitude")

        water_str    = f"{water_mm:.1f} mm" if water_mm is not None else "—"
        moisture_str = f"{moisture:.1f}%" if moisture is not None else "—"
        temp_str     = f"{temp:.1f}°C" if temp is not None else "—"
        rain_str     = f"{rain:.1f} mm/day" if rain is not None else "—"
        loc_str      = f"{lat:.4f}°, {lon:.4f}°" if (lat is not None and lon is not None) else "—"

        u_bg    = URGENCY_BG.get(urgency, "#1e293b")
        u_light = URGENCY_LIGHT.get(urgency, "#f1f5f9")
        u_text  = URGENCY_TEXT.get(urgency, "#64748b")
        u_emoji = URGENCY_EMOJI.get(urgency, "💧")
        u_label = URGENCY_LABEL.get(urgency, urgency)

        if water_mm and water_mm > 0:
            water_banner = (
                f'<tr><td colspan="2" style="padding-top:12px;">'
                f'<table width="100%" cellpadding="0" cellspacing="0">'
                f'<tr><td style="background-color:{u_light};border-left:4px solid {u_text};'
                f'border-radius:6px;padding:14px 16px;">'
                f'<div style="font-size:10px;color:{u_text};font-weight:700;text-transform:uppercase;'
                f'letter-spacing:0.08em;margin-bottom:6px;">💧 Irrigation Recommendation</div>'
                f'<div><span style="font-size:26px;font-weight:800;color:{u_text};line-height:1;">'
                f'{water_str}</span>'
                f'<span style="font-size:12px;color:#555;margin-left:8px;">of water required</span></div>'
                f'<div style="font-size:12px;color:#555;margin-top:8px;line-height:1.5;word-break:break-word;">'
                f'Apply <strong>{water_str}</strong> to restore soil moisture to field capacity. '
                f'Calculated using FAO-56 water balance with 80%% irrigation efficiency.</div>'
                f'</td></tr></table></td></tr>'
            )
        else:
            water_banner = (
                '<tr><td colspan="2" style="padding-top:12px;">'
                '<table width="100%" cellpadding="0" cellspacing="0">'
                '<tr><td style="background-color:#dcfce7;border-left:4px solid #16a34a;'
                'border-radius:6px;padding:14px 16px;">'
                '<span style="font-size:13px;font-weight:700;color:#15803d;">'
                '✅ No irrigation needed — soil moisture is adequate.</span>'
                '</td></tr></table></td></tr>'
            )

        cards_html += f"""
        <table width="100%" cellpadding="0" cellspacing="0"
               style="margin-bottom:20px;border:1px solid #e2e8f0;border-radius:10px;
                      overflow:hidden;border-collapse:separate;">
          <tr>
            <td style="background-color:{u_bg};padding:16px 18px;">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="word-break:break-word;vertical-align:middle;">
                    <div style="font-size:16px;font-weight:800;color:#ffffff;
                                word-break:break-word;line-height:1.3;">{u_emoji} {farm_name}</div>
                    <div style="font-size:11px;color:rgba(255,255,255,0.65);margin-top:3px;">
                      📍 {loc_str}</div>
                  </td>
                  <td style="text-align:right;vertical-align:middle;padding-left:10px;white-space:nowrap;">
                    <span style="background:rgba(255,255,255,0.2);color:#fff;
                                 padding:4px 10px;border-radius:20px;font-size:10px;
                                 font-weight:700;text-transform:uppercase;letter-spacing:0.05em;">
                      {urgency}
                    </span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="background:#ffffff;padding:16px 18px;">
              <div style="font-size:12px;font-weight:700;color:{u_text};
                          margin-bottom:14px;word-break:break-word;">{u_label}</div>
              <table width="100%" cellpadding="0" cellspacing="8">
                <tr>
                  <td width="50%" style="vertical-align:top;padding-right:6px;">
                    <table width="100%" cellpadding="0" cellspacing="0"
                           style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;">
                      <tr><td style="padding:12px 14px;">
                        <div style="font-size:9px;color:#94a3b8;font-weight:700;
                                    text-transform:uppercase;letter-spacing:0.07em;">🌾 Crop</div>
                        <div style="font-size:15px;font-weight:700;color:#1e293b;
                                    margin-top:4px;word-break:break-word;">{crop}</div>
                      </td></tr>
                    </table>
                  </td>
                  <td width="50%" style="vertical-align:top;padding-left:6px;">
                    <table width="100%" cellpadding="0" cellspacing="0"
                           style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;">
                      <tr><td style="padding:12px 14px;">
                        <div style="font-size:9px;color:#94a3b8;font-weight:700;
                                    text-transform:uppercase;letter-spacing:0.07em;">🌡️ Temperature</div>
                        <div style="font-size:15px;font-weight:700;color:#1e293b;margin-top:4px;">
                          {temp_str}</div>
                      </td></tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td width="50%" style="vertical-align:top;padding-right:6px;padding-top:8px;">
                    <table width="100%" cellpadding="0" cellspacing="0"
                           style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;">
                      <tr><td style="padding:12px 14px;">
                        <div style="font-size:9px;color:#94a3b8;font-weight:700;
                                    text-transform:uppercase;letter-spacing:0.07em;">🌧️ Rainfall</div>
                        <div style="font-size:15px;font-weight:700;color:#1e293b;margin-top:4px;">
                          {rain_str}</div>
                      </td></tr>
                    </table>
                  </td>
                  <td width="50%" style="vertical-align:top;padding-left:6px;padding-top:8px;">
                    <table width="100%" cellpadding="0" cellspacing="0"
                           style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;">
                      <tr><td style="padding:12px 14px;">
                        <div style="font-size:9px;color:#94a3b8;font-weight:700;
                                    text-transform:uppercase;letter-spacing:0.07em;">💧 Soil Moisture</div>
                        <div style="font-size:15px;font-weight:700;color:#1e293b;margin-top:4px;">
                          {moisture_str}</div>
                      </td></tr>
                    </table>
                  </td>
                </tr>
                {water_banner}
              </table>
            </td>
          </tr>
        </table>"""

    total_n    = len(fields)
    critical_n = summary_counts.get("CRITICAL", 0)
    high_n     = summary_counts.get("HIGH", 0)
    moderate_n = summary_counts.get("MODERATE", 0)
    none_n     = summary_counts.get("NONE", 0)

    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Smart Irrigation Alert</title>
</head>
<body style="margin:0;padding:0;background-color:#f1f5f9;
             font-family:Arial,Helvetica,sans-serif;-webkit-text-size-adjust:100%;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:20px 0;">
  <tr><td align="center">
    <table width="100%" cellpadding="0" cellspacing="0"
           style="max-width:580px;background:#ffffff;border-radius:14px;
                  overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.10);">
      <tr>
        <td style="background:linear-gradient(135deg,#0f2027,#203a43,#2c5364);
                   padding:30px 24px;text-align:center;">
          <div style="font-size:34px;line-height:1;">🌱</div>
          <h1 style="margin:10px 0 4px 0;font-size:20px;font-weight:800;
                     color:#ffffff;word-break:break-word;line-height:1.3;">
            Smart Irrigation — Field Status Alert
          </h1>
          <p style="margin:0;font-size:12px;color:rgba(255,255,255,0.55);word-break:break-word;">
            {email}
          </p>
        </td>
      </tr>
      <tr>
        <td style="padding:18px 24px 0 24px;">
          <p style="margin:0 0 12px 0;font-size:13px;color:#64748b;line-height:1.5;">
            Latest FAO-56 irrigation analysis for your <strong>{total_n}</strong> registered field(s):
          </p>
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;">
            <tr>
              <td width="25%" style="text-align:center;padding:12px 4px;">
                <div style="font-size:22px;font-weight:800;color:#ef4444;">{critical_n}</div>
                <div style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;">Critical</div>
              </td>
              <td width="25%" style="text-align:center;padding:12px 4px;border-left:1px solid #e2e8f0;">
                <div style="font-size:22px;font-weight:800;color:#f97316;">{high_n}</div>
                <div style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;">High</div>
              </td>
              <td width="25%" style="text-align:center;padding:12px 4px;border-left:1px solid #e2e8f0;">
                <div style="font-size:22px;font-weight:800;color:#ca8a04;">{moderate_n}</div>
                <div style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;">Moderate</div>
              </td>
              <td width="25%" style="text-align:center;padding:12px 4px;border-left:1px solid #e2e8f0;">
                <div style="font-size:22px;font-weight:800;color:#16a34a;">{none_n}</div>
                <div style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em;">No Action</div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr>
        <td style="padding:20px 24px 8px 24px;">
          <p style="margin:0 0 14px 0;font-size:11px;font-weight:700;color:#94a3b8;
                    text-transform:uppercase;letter-spacing:0.07em;">
            📋 Per-Field Details &amp; Recommendations
          </p>
          {cards_html}
        </td>
      </tr>
      <tr>
        <td style="padding:0 24px 20px 24px;">
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;">
            <tr>
              <td style="padding:12px 14px;font-size:11px;color:#1e40af;
                         word-break:break-word;line-height:1.6;">
                <strong>ℹ️ Methodology:</strong> FAO-56 Penman-Monteith ET₀ &middot;
                NASA POWER 7-day data &middot; FAO CLIMWAT 2.0 cross-validation &middot;
                80%% irrigation efficiency assumed.
              </td>
            </tr>
          </table>
        </td>
      </tr>
      <tr>
        <td style="background:#f8fafc;border-top:1px solid #e2e8f0;
                   padding:16px 24px;text-align:center;">
          <p style="margin:0 0 6px 0;font-size:11px;color:#94a3b8;word-break:break-word;">
            Smart Irrigation Advisor &mdash; Automated Alert System
          </p>
          <a href="http://127.0.0.1:8000/"
             style="font-size:12px;color:#3b82f6;text-decoration:none;font-weight:700;">
            View Live Dashboard &rarr;
          </a>
        </td>
      </tr>
    </table>
  </td></tr>
</table>
</body>
</html>"""

    from src.email_service import send_email
    ok = send_email(email, "🌱 Field Status Alert — Smart Irrigation Advisor", html_body)
    if ok:
        urgent_count = summary_counts.get("CRITICAL", 0) + summary_counts.get("HIGH", 0)
        msg = f"Alert email dispatched for {total_n} field(s)."
        if urgent_count:
            msg += f" {urgent_count} field(s) require urgent irrigation!"
        return True, msg
    else:
        return False, "Failed to send email. Check SMTP config."

@app.route("/api/alerts/send", methods=["GET"])
def send_alert_endpoint():
    """Manually send a field status summary alert email to the registered farmer."""
    email = request.args.get("email")
    if not email:
        return jsonify({"status": "error", "message": "Email is required"}), 400
    
    if not get_detailed_field_status(farmer_email=email):
        return jsonify({"status": "error", "message": "No fields found for this user"}), 404

    ok, msg = _dispatch_alert(email)
    if ok:
        return jsonify({"status": "ok", "message": msg})
    else:
        return jsonify({"status": "error", "message": msg}), 500


@app.route("/api/recommendations", methods=["GET"])
def get_recommendations_endpoint():
    urgency = request.args.get("urgency", "").upper() or None
    limit = int(request.args.get("limit", 100))
    email = request.args.get("email")
    recs = get_latest_recommendations(urgency_filter=urgency, limit=limit, farmer_email=email)
    return jsonify({"status": "ok", "count": len(recs), "recommendations": recs})

@app.route("/api/summary", methods=["GET"])
def get_summary_endpoint():
    email = request.args.get("email")
    summary_data = get_dashboard_summary(farmer_email=email)
    return jsonify({"status": "ok", "dashboard_summary": summary_data})


@app.route("/api/cron/run", methods=["GET", "POST"])
def cron_run_endpoint():
    """
    Unified endpoint for Cloud Scheduler.
    1. Fetches NASA weather for all active fields.
    2. Runs the evaluation engine for all active fields.
    3. Handles critical alerts automatically.
    """
    days = WEATHER_LOOKBACK_DAYS
    active_fields = list_active_fields()
    
    if not active_fields:
        return jsonify({"status": "ok", "message": "No active fields to process."})

    today = date.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    analysis_date = end_date.isoformat()
    
    total_weather_inserted = 0
    errors = []
    recommendations_out = []

    for field in active_fields:
        field_id = field["field_id"]
        lat = field["latitude"]
        lon = field["longitude"]
        crop_type = field.get("crop_type", "default")
        farmer_email = field.get("farmer_email")
        farm_name = field.get("farm_name", "")

        try:
            records = fetch_weather_data(lat, lon, start_date, end_date)
            if records:
                n = insert_weather_records(field_id, records)
                total_weather_inserted += n
            else:
                errors.append(f"Field {field_id}: no weather records returned")
                continue
                
            recommendation = evaluate_irrigation_rules(
                field_id=field_id,
                crop_type=crop_type,
                weather_records=records,
                latitude=lat,
            )
            
            rec_id = insert_recommendation(recommendation, analysis_date)
            
            alert_result = send_irrigation_alert(
                recommendation=recommendation,
                farmer_email=farmer_email,
                farm_name=farm_name,
                analysis_date=analysis_date,
            )
            
            recommendations_out.append({
                "field_id": field_id,
                "urgency": recommendation.final_urgency.value,
                "alert": alert_result
            })
            
        except Exception as exc:
            msg = f"Field {field_id}: {exc}"
            import logging
            logging.error(msg)
            errors.append(msg)

    return jsonify({
        "status": "ok" if not errors else "partial",
        "fields_processed": len(active_fields),
        "weather_inserted": total_weather_inserted,
        "recommendations": recommendations_out,
        "errors": errors
    })


@app.route("/")
def index():
    return send_from_directory("dashboard/public", "index.html")

@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("dashboard/public", path)


def cloud_function_entry(request):
    """
    Adapter to run the entire Flask app inside a Google Cloud Function Gen2.
    GCP Functions require a pure function as an entry point, not a Flask app object.
    This safely translates the exact request into our WSGI app context.
    """
    from werkzeug.test import run_wsgi_app
    from flask import Response
    import io

    body = request.get_data()
    environ = request.environ.copy()
    environ['wsgi.input'] = io.BytesIO(body)
    environ['CONTENT_LENGTH'] = str(len(body))
    
    app_iter, status, headers = run_wsgi_app(app, environ, buffered=True)
    return Response(app_iter, status=status, headers=headers)


if __name__ == "__main__":
    initialize()
    print("Starting Flask server on http://127.0.0.1:8000")
    app.run(host="0.0.0.0", port=8000, debug=True)
