# Running the Smart Irrigation Advisor Locally

The app runs as a **Flask server** on your machine using a local **SQLite database**,
completely bypassing Google Cloud. No GCP account or billing needed.

## Prerequisites

- Python 3.10+ installed
- PowerShell terminal (Windows)

---

## Step 1 — Activate the Virtual Environment

```powershell
cd D:\Smart-Irrigation-Advisor
.venv\Scripts\Activate.ps1
```

> If you get a script execution error, run this first:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```

---

## Step 2 — Install Dependencies

```powershell
pip install -r requirements.txt
```

> Local-only extra packages (if missing):
> ```powershell
> pip install werkzeug flask flask-cors itsdangerous
> ```

---

## Step 3 — Set Up the `.env` File

Make sure `D:\Smart-Irrigation-Advisor\.env` exists and contains:

```env
JWT_SECRET_KEY=super_secret_jwt_key_here
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_EMAIL=your_gmail@gmail.com
SMTP_APP_PASSWORD=your_gmail_app_password
FRONTEND_URL=http://127.0.0.1:8000
```

> For email alerts to work, you need a **Gmail App Password**:
> Google Account → Security → 2-Step Verification → App Passwords

---

## Step 4 — Start the Server

```powershell
python main.py
```

You should see:
```
Starting Flask server on http://127.0.0.1:8000
```

---

## Step 5 — Open the Dashboard

Open your browser and go to:

**http://127.0.0.1:8000/**

---

## Step 6 — Use the Dashboard

| Step | Button | What it does |
|------|--------|-------------|
| 1 | **Register / Log In** | Create an account (auto-verified in dev mode) |
| 2 | **Add Field** | Register your farm (crop type, soil, lat/lon, area) |
| 3 | **Fetch Weather** | Downloads 7 days of NASA POWER data for your fields |
| 4 | **Run Engine** | Runs FAO-56 rule engine → generates irrigation recommendations |
| 5 | **Send Alert** | Emails a per-field report to your registered address |

Demo fields are auto-created on first launch if you have no fields registered.

---

## API Endpoints (manually accessible)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /` | GET | Dashboard UI |
| `GET /api/weather/fetch` | GET | Fetch NASA POWER data |
| `GET /api/recommendations/evaluate` | GET | Run irrigation engine |
| `GET /api/recommendations` | GET | List recommendations |
| `GET /api/summary` | GET | Dashboard summary counts |
| `GET /api/fields` | GET | List your fields |
| `POST /api/fields/add` | POST | Add a new field |
| `GET /api/alerts/send?email=X` | GET | Send email alert |
| `GET /api/cron/run` | GET | Full pipeline (fetch + evaluate + alert) |

---

## Local Data Storage

All data is stored in **`smart_irrigation.db`** (SQLite) in the project root.
Delete this file to reset all data.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| Port 8000 already in use | Kill the old process or change port in `main.py` line 521 |
| Script execution policy error | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Email not sending | Check `.env` SMTP settings and Gmail App Password |
| No weather data | Check internet connection (NASA POWER API) |
