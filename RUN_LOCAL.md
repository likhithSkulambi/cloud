# Running the Smart Irrigation Advisor Locally

We have refactored the application to run entirely on your local machine, completely bypassing Google Cloud (BigQuery, Pub/Sub, Cloud Functions). This allows you to track fields and get recommendations using an embedded SQLite database and a local web dashboard.

## Prerequisites

1. Active Python Virtual Environment (`.venv\Scripts\Activate.ps1` in PowerShell)
2. Installed dependencies: `pip install -r requirements.txt fastapi uvicorn pydantic`

## Starting the Dashboard

Run the following command in the project root:

```powershell
python local_app.py
```

## Using the Dashboard

1. Open a browser and navigate to **http://127.0.0.1:8000/**.
2. The dashboard comes pre-loaded with **two demo fields**.
3. **Step 1:** Click **"Fetch Weather"** in the top right to download 7 days of NASA POWER data for the demo fields.
4. **Step 2:** Click **"Run Engine"** to process the weather data through the FAO-56 irrigation rules and generate recommendations.
5. The dashboard cards will automatically update, and you can see the results in the table below. (Use the dropdown to filter by urgency).

## How it works (Local Mode)

* **Database (`local_store.py`)**: Stores fields, weather data, and recommendations in a local `smart_irrigation.db` SQLite database instead of BigQuery.
* **Alerts (`local_alerts.py`)**: Replaces Pub/Sub and SendGrid emails with local console tracking. Check the terminal where you ran `python local_app.py` for alert outputs.
* **Server (`local_app.py`)**: A FastAPI server that exposes identical REST endpoints to the Google Cloud Functions payload and also serves the modern UI front-end located at `dashboard/public/index.html`.
