# 🌱 Smart Irrigation Advisor

> An intelligent, cloud-native irrigation recommendation system powered by
> **NASA POWER** weather data, **FAO-56 Penman-Monteith** evapotranspiration
> modelling, and **Google Cloud** infrastructure.

---

## ✨ Features

| Feature | Detail |
|---|---|
| 🛰 NASA POWER Integration | Fetches 7 daily meteorological parameters via REST API |
| 🌿 FAO-56 ET₀ Engine | Full Penman-Monteith evapotranspiration calculation |
| 📋 6 Irrigation Rules | Each with 4 urgency levels: CRITICAL / HIGH / MODERATE / NONE |
| 🗄 BigQuery Storage | Partitioned tables for weather data & recommendations |
| 📣 Dual Alerts | Pub/Sub messages + SendGrid HTML email per recommendation |
| ☁️ Serverless | Three Google Cloud Functions (Gen2) + Cloud Scheduler |
| 📊 Dashboard | Looker Studio with pre-built BigQuery SQL views |

---

## 🏗 Architecture

```
Cloud Scheduler (Daily 02:00 UTC)
        │
        ▼
┌───────────────────────┐   NASA POWER API
│  fetch-and-store-     │──────────────────► BigQuery: weather_data
│  weather (CF)         │
└───────────────────────┘
        │ triggers (02:30 UTC)
        ▼
┌───────────────────────┐   BigQuery: weather_data
│  evaluate-and-        │◄─────────────────
│  recommend (CF)       │
│                       │──► BigQuery: irrigation_recommendations
│  FAO-56 Rule Engine   │──► Pub/Sub: irrigation-alerts
│  (6 rules, 4 levels)  │──► SendGrid Email Alerts
└───────────────────────┘
        │
        ▼
┌───────────────────────┐
│  get-recommendations  │◄── HTTP GET (Looker Studio / REST clients)
│  (CF – REST API)      │
└───────────────────────┘
```

---

## 📁 Project Structure

```
Smart-Irrigation-Advisor/
├── src/
│   ├── main.py                # Cloud Function entry points (3 functions)
│   ├── fetch_nasa_data.py     # NASA POWER API integration
│   ├── bigquery_store.py      # BigQuery schema, insert, query
│   ├── irrigation_rules.py    # FAO-56 rule engine (6 rules, 4 urgency levels)
│   └── alert_system.py        # Pub/Sub + SendGrid email alerts
├── tests/
│   └── test_irrigation_rules.py   # 15 unit tests
├── config/
│   └── .env.example           # Environment variable template
├── deployment/
│   ├── setup_gcp.sh           # One-time GCP setup
│   └── deploy.sh              # Deploy Cloud Functions + Scheduler
├── dashboard/
│   └── DASHBOARD_SETUP.md     # Looker Studio setup + BigQuery SQL views
├── requirements.txt
└── README.md
```

---

## 🚀 Quickstart

### 1. Clone & Configure

```bash
git clone https://github.com/your-org/smart-irrigation-advisor.git
cd smart-irrigation-advisor

cp config/.env.example config/.env
# Edit config/.env with your GCP project ID, SendGrid key, etc.
```

### 2. One-Time GCP Setup

```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# Export required variables
export GCP_PROJECT_ID="your-gcp-project-id"
export SENDGRID_API_KEY="SG.xxxxx"
export ALERT_FROM_EMAIL="alerts@your-domain.com"

# Run setup (APIs, IAM, BigQuery, Pub/Sub)
chmod +x deployment/setup_gcp.sh
./deployment/setup_gcp.sh
```

### 3. Register Your First Field

```python
from src.bigquery_store import upsert_field, initialize_schema

initialize_schema()   # creates tables if they don't exist

upsert_field({
    "field_id":       "field-001",
    "farm_name":      "Green Acres Farm",
    "farmer_email":   "farmer@example.com",
    "crop_type":      "wheat",
    "latitude":       20.5937,
    "longitude":      78.9629,
    "area_hectares":  5.0,
    "active":         True,
})
```

### 4. Deploy to Google Cloud

```bash
chmod +x deployment/deploy.sh
./deployment/deploy.sh
```

### 5. Test the REST API

```bash
# Get all recommendations
curl "https://<REGION>-<PROJECT_ID>.cloudfunctions.net/get-recommendations"

# Filter by urgency
curl "https://.../get-recommendations?urgency=CRITICAL"

# Dashboard summary
curl "https://.../get-recommendations?summary=true"
```

### 6. Run Unit Tests Locally

```bash
pip install -r requirements.txt
pytest tests/ -v --tb=short
```

---

## 🌿 Irrigation Rules

| # | Rule | Urgency Trigger |
|---|---|---|
| 1 | High ET₀ & Low Rainfall | ET₀ > 6 mm & rain < 2 mm → HIGH; Temp > 38°C → CRITICAL |
| 2 | Consecutive Dry Days | ≥ 3 days → MODERATE; ≥ 5 → HIGH; ≥ 7 → CRITICAL |
| 3 | Cumulative ET₀ vs Crop Need | Deficit > 10 → MODERATE; > 25 → HIGH; > 40 → CRITICAL |
| 4 | Low Relative Humidity | RH < 40% → MODERATE; < 30% → HIGH; < 20% → CRITICAL |
| 5 | High Wind Speed | WS > 5 m/s → MODERATE; > 8 m/s → HIGH |
| 6 | Heavy Recent Rainfall | ≥ 30 mm in 2 days → suppresses all recommendations |

---

## 🌾 Supported Crops & Kc Values

| Crop | Kc (mid-season) |
|---|---|
| Wheat | 1.15 |
| Maize | 1.20 |
| Rice | 1.20 |
| Cotton | 1.20 |
| Tomato | 1.15 |
| Potato | 1.15 |
| Sugarcane | 1.25 |
| Sorghum | 1.10 |
| Default | 1.00 |

---

## 🌍 Data Sources

- **NASA POWER** – [power.larc.nasa.gov](https://power.larc.nasa.gov)
  Daily agricultural meteorology data at any global coordinate.
- **FAO-56** – Allen, R.G. et al. (1998). *Crop evapotranspiration – Guidelines for computing crop water requirements.* FAO Irrigation and Drainage Paper 56.

---

## 🔐 Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GCP_PROJECT_ID` | ✅ | GCP project ID |
| `BIGQUERY_DATASET` | ✅ | BigQuery dataset name (default: `smart_irrigation`) |
| `PUBSUB_TOPIC_ID` | ✅ | Pub/Sub topic for alerts |
| `SENDGRID_API_KEY` | ✅ | SendGrid email API key |
| `ALERT_FROM_EMAIL` | ✅ | Verified sender email |
| `ALERT_MIN_URGENCY` | ✅ | Minimum urgency for email (default: `HIGH`) |
| `WEATHER_LOOKBACK_DAYS` | ➖ | Days of weather data to analyse (default: `7`) |
| `BIGQUERY_LOCATION` | ➖ | BQ dataset location (default: `US`) |
| `GCP_REGION` | ➖ | Cloud Functions region (default: `us-central1`) |

---

## 📊 Dashboard

See [`dashboard/DASHBOARD_SETUP.md`](dashboard/DASHBOARD_SETUP.md) for step-by-step
instructions to build a Looker Studio dashboard with geo maps, urgency scorecards,
and time-series charts.

---

## 🧪 Testing

```
tests/test_irrigation_rules.py (15 tests)
├── TestGetKc               (2 tests)  – Crop Kc lookup
├── TestComputeET0          (3 tests)  – Penman-Monteith calculation
├── TestComputeRecommendedWater (2 tests) – Irrigation depth
├── TestRuleConsecutiveDryDays (2 tests) – Rule 2
├── TestRuleHeavyRainfall   (2 tests)  – Rule 6 suppression
├── TestRuleLowHumidity     (1 test)   – Rule 4
├── TestRuleHighWindSpeed   (1 test)   – Rule 5
└── TestEvaluateIrrigationRules (2 tests) – End-to-end integration
```

---

## 📄 License

MIT License – see [LICENSE](LICENSE) for details.

---

*Built with ❤️ using Google Cloud, NASA POWER, and FAO-56 agronomic standards.*
