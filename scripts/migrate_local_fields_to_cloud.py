import json
import sqlite3
import ssl
import urllib.request

LOCAL_DB = "smart_irrigation.db"
CLOUD_BASE = "https://smart-irrigation-dashboard-fifv3j3oha-uc.a.run.app"
TARGET_EMAIL = "thenameisravana01@gmail.com"
SSL_CONTEXT = ssl._create_unverified_context()


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60, context=SSL_CONTEXT) as resp:
        body = resp.read().decode("utf-8", "ignore")
        return json.loads(body)


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=120, context=SSL_CONTEXT) as resp:
        return json.loads(resp.read().decode("utf-8", "ignore"))


def main() -> None:
    conn = sqlite3.connect(LOCAL_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT farm_name, crop_type, soil_type, latitude, longitude, area_hectares
        FROM field_registry
        WHERE active = 1 AND farmer_email = ?
        """,
        (TARGET_EMAIL,),
    ).fetchall()

    print(f"Found {len(rows)} local active fields for {TARGET_EMAIL}")

    added = 0
    failed = 0
    for row in rows:
        payload = {
            "farm_name": row["farm_name"],
            "crop_type": row["crop_type"],
            "soil_type": row["soil_type"],
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "area_hectares": float(row["area_hectares"]),
            "farmer_email": TARGET_EMAIL,
        }
        try:
            result = post_json(f"{CLOUD_BASE}/api/fields/add", payload)
            if result.get("status") == "ok":
                added += 1
            else:
                failed += 1
                print("ADD_FAIL", result)
        except Exception as exc:
            failed += 1
            print("ADD_ERROR", exc)

    print(f"Added: {added}, Failed: {failed}")

    weather = get_json(f"{CLOUD_BASE}/api/weather/fetch?email={TARGET_EMAIL}")
    print("Weather fetch:", weather)

    eval_result = get_json(f"{CLOUD_BASE}/api/recommendations/evaluate?email={TARGET_EMAIL}")
    print("Evaluate:", {k: eval_result.get(k) for k in ["status", "fields_processed", "auto_alert_msg"]})

    summary = get_json(f"{CLOUD_BASE}/api/summary?email={TARGET_EMAIL}")
    print("Summary:", summary)


if __name__ == "__main__":
    main()
