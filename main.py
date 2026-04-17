"""
local_app.py
------------
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
            return jsonify({"status": "error", "message": "User not found"}), 404
        if user["is_verified"]:
            return """<h2>Email already verified!</h2><p><a href="/">Go to Dashboard</a></p>"""
            
        verify_user(email)
        return """<h2>Email successfully verified!</h2><p>You can now log in to the <a href="/">Dashboard</a>.</p>"""
    except SignatureExpired:
        return """<h2>Error: Verification link expired</h2>""", 400
    except BadSignature:
        return """<h2>Error: Invalid verification link</h2>""", 400

@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.json
    email = data.get("email")
    password = data.get("password")
    
    user = get_user_by_email(email)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"status": "error", "message": "Invalid email or password"}), 401
        
    if not user["is_verified"]:
        return jsonify({"status": "error", "message": "Please verify your email before logging in"}), 403
        
    auth_token = serializer.dumps(email, salt='auth-token')
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
            
            rec_id = insert_recommendation(recommendation, analysis_date)
            
            alert_result = False
            if recommendation.final_urgency.value in ["HIGH", "CRITICAL"]:
                alert_result = send_irrigation_alert(
                    to_email=farmer_email,
                    farm_name=farm_name,
                    crop_type=crop_type,
                    recommendation=recommendation.recommended_water_mm,
                    moisture=getattr(recommendation, "simulated_moisture_percent", 50.0),
                    action=recommendation.summary
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
            errors.append(f"Field {field_id}: {exc}")

    return jsonify({
        "status": "ok" if not errors else "partial",
        "recommendations": recommendations_out,
        "errors": errors
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

@app.route("/api/alerts/send", methods=["GET"])
def send_alert_endpoint():
    """Manually send a field status summary alert email to the registered farmer."""
    email = request.args.get("email")
    if not email:
        return jsonify({"status": "error", "message": "Email is required"}), 400

    fields = get_detailed_field_status(farmer_email=email)
    if not fields:
        return jsonify({"status": "error", "message": "No fields found for this user"}), 404

    # Build HTML rows for each field
    rows_html = ""
    for f in fields:
        urgency = f.get("final_urgency") or "N/A"
        water = f"{f['recommended_water_mm']:.1f} mm" if f.get("recommended_water_mm") is not None else "—"
        moisture = f"{f['moisture']:.1f}%" if f.get("moisture") is not None else "—"
        color = {"CRITICAL": "#ef4444", "HIGH": "#f97316", "MODERATE": "#eab308", "NONE": "#22c55e"}.get(urgency, "#9ca3af")
        rows_html += f"""
        <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #ddd;">{f.get('farm_name','—')}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #ddd;text-transform:capitalize;">{f.get('crop_type','—')}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #ddd;">{moisture}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #ddd;color:#e74c3c;font-weight:bold;">{water}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #ddd;color:{color};font-weight:bold;">{urgency}</td>
        </tr>"""

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:700px;margin:auto;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;">
        <div style="background-color:#1a2535;padding:24px;color:white;text-align:center;">
            <h2 style="margin:0;font-size:22px;">🌱 Smart Irrigation — Field Status Alert</h2>
            <p style="margin:6px 0 0 0;opacity:0.75;font-size:14px;">Summary for {email}</p>
        </div>
        <div style="padding:20px;">
            <p style="color:#333;">Here is the latest irrigation status for all your registered fields:</p>
            <table style="width:100%;border-collapse:collapse;margin-top:10px;font-size:14px;">
                <thead>
                    <tr style="background-color:#f0f4f8;">
                        <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Farm</th>
                        <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Crop</th>
                        <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Moisture</th>
                        <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Water Needed</th>
                        <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Urgency</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        <div style="background-color:#f8f9fa;padding:15px;text-align:center;font-size:12px;color:#7f8c8d;">
            Smart Irrigation Advisor — Automated Alert &nbsp;|&nbsp;
            <a href="http://127.0.0.1:8000/" style="color:#3498db;">View Dashboard</a>
        </div>
    </div>"""

    from src.email_service import send_email
    ok = send_email(email, "🌱 Field Status Alert — Smart Irrigation", html_body)
    if ok:
        critical_count = sum(1 for f in fields if (f.get("final_urgency") or "") in ("CRITICAL","HIGH"))
        msg = f"Alert email dispatched for {len(fields)} field(s)."
        if critical_count:
            msg += f" {critical_count} field(s) require urgent irrigation!"
        return jsonify({"status": "ok", "message": msg})
    else:
        return jsonify({"status": "error", "message": "Failed to send email. Check SMTP config."}), 500

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
