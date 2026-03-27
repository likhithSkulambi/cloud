# Looker Studio Dashboard Setup Guide

## Smart Irrigation Advisor – Dashboard

This guide explains how to connect your BigQuery tables to Google Looker Studio
(formerly Data Studio) and build a real-time irrigation monitoring dashboard.

---

## Prerequisites

- BigQuery dataset `smart_irrigation` with at least one day of data
- Access to [Looker Studio](https://lookerstudio.google.com)
- BigQuery Data Viewer role on your GCP project

---

## BigQuery SQL Views

Create the following views in BigQuery to simplify dashboard queries.
Run each SQL block in the [BigQuery Console](https://console.cloud.google.com/bigquery).

### View 1 – Latest Recommendation per Field

```sql
CREATE OR REPLACE VIEW `YOUR_PROJECT.smart_irrigation.v_latest_recommendations` AS
WITH ranked AS (
  SELECT
    r.*,
    f.farm_name,
    f.farmer_email,
    f.area_hectares,
    f.latitude,
    f.longitude,
    ROW_NUMBER() OVER (PARTITION BY r.field_id ORDER BY r.generated_at DESC) AS rn
  FROM `YOUR_PROJECT.smart_irrigation.irrigation_recommendations` r
  LEFT JOIN (
    SELECT field_id, farm_name, farmer_email, area_hectares, latitude, longitude,
           ROW_NUMBER() OVER (PARTITION BY field_id ORDER BY updated_at DESC) AS rn
    FROM `YOUR_PROJECT.smart_irrigation.field_registry`
  ) f USING (field_id)
  WHERE f.rn = 1
)
SELECT
  recommendation_id,
  field_id,
  farm_name,
  farmer_email,
  crop_type,
  generated_at,
  analysis_date,
  final_urgency,
  recommended_water_mm,
  cumulative_et0_mm,
  cumulative_rain_mm,
  net_water_deficit_mm,
  triggered_rules,
  area_hectares,
  latitude,
  longitude,
  summary
FROM ranked
WHERE rn = 1;
```

### View 2 – Urgency Distribution Summary

```sql
CREATE OR REPLACE VIEW `YOUR_PROJECT.smart_irrigation.v_urgency_summary` AS
SELECT
  final_urgency,
  COUNT(*)                        AS field_count,
  ROUND(AVG(recommended_water_mm), 2)  AS avg_water_mm,
  ROUND(SUM(recommended_water_mm), 2)  AS total_water_mm,
  ROUND(AVG(net_water_deficit_mm), 2)  AS avg_deficit_mm
FROM `YOUR_PROJECT.smart_irrigation.v_latest_recommendations`
GROUP BY final_urgency
ORDER BY
  CASE final_urgency
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH'     THEN 2
    WHEN 'MODERATE' THEN 3
    ELSE 4
  END;
```

### View 3 – 30-Day Weekly ET₀ vs Rain Trend

```sql
CREATE OR REPLACE VIEW `YOUR_PROJECT.smart_irrigation.v_weekly_et0_rain` AS
SELECT
  field_id,
  DATE_TRUNC(date, WEEK) AS week_start,
  ROUND(SUM(ALLSKY_SFC_SW_DWN), 2)   AS total_solar_mj,
  ROUND(AVG(T2M), 2)                  AS avg_temp_c,
  ROUND(AVG(RH2M), 2)                 AS avg_humidity_pct,
  ROUND(SUM(PRECTOTCORR), 2)          AS total_rain_mm,
  COUNT(*)                            AS days_with_data
FROM `YOUR_PROJECT.smart_irrigation.weather_data`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY field_id, week_start
ORDER BY field_id, week_start;
```

### View 4 – Field-Level Alert History

```sql
CREATE OR REPLACE VIEW `YOUR_PROJECT.smart_irrigation.v_alert_history` AS
SELECT
  field_id,
  crop_type,
  analysis_date,
  final_urgency,
  recommended_water_mm,
  net_water_deficit_mm,
  generated_at,
  CASE final_urgency
    WHEN 'CRITICAL' THEN 4
    WHEN 'HIGH'     THEN 3
    WHEN 'MODERATE' THEN 2
    ELSE 1
  END AS urgency_rank
FROM `YOUR_PROJECT.smart_irrigation.irrigation_recommendations`
ORDER BY field_id, analysis_date DESC;
```

---

## Looker Studio Dashboard Setup

### Step 1 – Create a New Report

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Click **+ Create → Report**
3. Select **BigQuery** as the data source

### Step 2 – Add Data Sources

Add each SQL view as a separate data source by selecting:
- **Project** → `YOUR_PROJECT`
- **Dataset** → `smart_irrigation`
- **Table** → select the view name

Repeat for all four views.

### Step 3 – Recommended Charts

| Chart Type | Data Source | Metric/Dimension |
|---|---|---|
| **Scorecard** | `v_urgency_summary` | `field_count` filtered by CRITICAL |
| **Scorecard** | `v_urgency_summary` | `avg_water_mm` |
| **Pie Chart** | `v_urgency_summary` | Dimension: `final_urgency`, Metric: `field_count` |
| **Table** | `v_latest_recommendations` | All columns, sorted by urgency_rank |
| **Bar Chart** | `v_weekly_et0_rain` | Breakdown: `week_start`, Value: `total_rain_mm` |
| **Geo Map** | `v_latest_recommendations` | Latitude/Longitude, Color: `final_urgency` |
| **Time Series** | `v_alert_history` | Date: `analysis_date`, Value: `urgency_rank` |

### Step 4 – Conditional Formatting (Urgency Colors)

Apply these colors to the urgency field:

| Urgency | Hex Color |
|---|---|
| CRITICAL | `#c0392b` (Red) |
| HIGH | `#e67e22` (Orange) |
| MODERATE | `#f1c40f` (Yellow) |
| NONE | `#27ae60` (Green) |

In Looker Studio: **Chart Settings → Style → Conditional Formatting**

### Step 5 – Filters & Date Range Controls

1. Add a **Date Range Control** linked to `analysis_date` on `v_alert_history`
2. Add a **Drop-down Listbox** for `final_urgency` as a dashboard-wide filter
3. Add a **Drop-down Listbox** for `field_id` to drill down on individual fields

### Step 6 – Auto-Refresh

Set the report to refresh every **4 hours**:
- **Resource → Report Settings → Data freshness → 4 hours**

---

## Useful Scheduled Queries

### Daily Irrigation Summary Email (BigQuery Scheduled Query)

```sql
-- Schedule this to run daily at 03:00 UTC
SELECT
  final_urgency,
  COUNT(*) AS fields,
  ROUND(AVG(recommended_water_mm), 1) AS avg_mm
FROM `YOUR_PROJECT.smart_irrigation.v_urgency_summary`
GROUP BY final_urgency
ORDER BY 1;
```

---

## Troubleshooting

| Issue | Solution |
|---|---|
| No data in BigQuery | Trigger `fetch-and-store-weather` Cloud Function manually |
| Views return 0 rows | Check field_registry has at least one active=TRUE row |
| Geo map not showing | Ensure latitude/longitude columns are set as Geo type in Looker Studio |
| Stale data | Verify Cloud Scheduler jobs are enabled and running |
