import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging

logger = logging.getLogger(__name__)

# Load .env manually to avoid extra dependencies
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ[k.strip()] = v.strip()

SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
SMTP_APP_PASSWORD = os.environ.get("SMTP_APP_PASSWORD", "")

def is_dev_mode() -> bool:
    """Returns True when SMTP is not configured (local dev mode)."""
    return not SMTP_EMAIL or not SMTP_APP_PASSWORD or SMTP_EMAIL == "your_email@gmail.com"

def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send an HTML email, or mock it to console if credentials missing."""
    if not SMTP_EMAIL or not SMTP_APP_PASSWORD or SMTP_EMAIL == "your_email@gmail.com":
        logger.warning(f"--- MOCK EMAIL TO {to_email} ---")
        logger.warning(f"Subject: {subject}")
        logger.warning(f"Body: {html_body}")
        logger.warning("---------------------------------")
        return True

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_EMAIL
        msg["To"] = to_email

        # Attach the HTML message
        part = MIMEText(html_body, "html")
        msg.attach(part)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_APP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())
        
        logger.info(f"Email sent successfully to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False

def send_verification_email(to_email: str, token: str):
    base_url = os.environ.get("FRONTEND_URL", "http://127.0.0.1:8000")
    verify_link = f"{base_url}/api/auth/verify?token={token}"
    
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto;">
        <h2 style="color: #4CAF50;">Welcome to Smart Irrigation Advisor! 🌱</h2>
        <p>Thank you for registering. Please click the button below to verify your email address and activate your account:</p>
        <p style="text-align: center; margin: 30px 0;">
            <a href="{verify_link}" style="padding: 12px 24px; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 6px; font-weight: bold;">Verify Email Address</a>
        </p>
        <p style="color: #666; font-size: 12px;">If the button doesn't work, you can copy and paste this link into your browser: <br>{verify_link}</p>
    </div>
    """
    return send_email(to_email, "Verify Your Account - Smart Irrigation", html)

def send_reset_email(to_email: str, token: str):
    base_url = os.environ.get("FRONTEND_URL", "http://127.0.0.1:8000")
    reset_link = f"{base_url}/?reset_token={token}"
    
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto;">
        <h2 style="color: #4CAF50;">Password Reset Request 🔐</h2>
        <p>We received a request to reset your password for the Smart Irrigation Advisor. Click the button below to choose a new password:</p>
        <p style="text-align: center; margin: 30px 0;">
            <a href="{reset_link}" style="padding: 12px 24px; background-color: #3498db; color: white; text-decoration: none; border-radius: 6px; font-weight: bold;">Reset Password</a>
        </p>
        <p style="color: #666; font-size: 12px;">If you didn't request this, you can safely ignore this email.</p>
    </div>
    """
    return send_email(to_email, "Password Reset - Smart Irrigation", html)

def send_irrigation_alert(to_email: str, farm_name: str, crop_type: str, recommendation: float, moisture: float, action: str):
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
        <div style="background-color: #2c3e50; padding: 20px; color: white; text-align: center;">
            <h2 style="margin: 0;">🌱 Irrigation Alert</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.8;">{farm_name}</p>
        </div>
        <div style="padding: 20px;">
            <p>Your crop requires attention today based on our latest FAO-56 soil moisture analysis.</p>
            <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee;"><strong>Crop:</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #eee; text-transform: capitalize;">{crop_type}</td></tr>
                <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee;"><strong>Water Required:</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #eee; color: #e74c3c; font-weight: bold;">{recommendation:.1f} mm</td></tr>
                <tr><td style="padding: 8px 0; border-bottom: 1px solid #eee;"><strong>Soil Moisture:</strong></td><td style="padding: 8px 0; border-bottom: 1px solid #eee;">{moisture:.1f}%</td></tr>
                <tr><td style="padding: 8px 0;"><strong>Recommended Action:</strong></td><td style="padding: 8px 0; color: #4CAF50; font-weight: bold;">{action}</td></tr>
            </table>
        </div>
        <div style="background-color: #f8f9fa; padding: 15px; text-align: center; font-size: 12px; color: #7f8c8d;">
            Smart Irrigation Advisor Automated Alert System<br>
            <a href="http://127.0.0.1:8000/" style="color: #3498db;">View full dashboard</a>
        </div>
    </div>
    """
    return send_email(to_email, f"Irrigation Alert: {farm_name}", html)
