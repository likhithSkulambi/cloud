"""
fetch_nasa_data.py
------------------
NASA POWER API integration for the Smart Irrigation Advisor.

Fetches the following meteorological parameters for ET₀ (Reference
Evapotranspiration) calculation using the FAO-56 Penman-Monteith method:

    T2M_MAX     – Maximum air temperature at 2 m (°C)
    T2M_MIN     – Minimum air temperature at 2 m (°C)
    T2M         – Mean air temperature at 2 m (°C)
    RH2M        – Relative humidity at 2 m (%)
    WS2M        – Wind speed at 2 m (m/s)
    ALLSKY_SFC_SW_DWN – Solar radiation (MJ/m²/day)
    PRECTOTCORR – Precipitation (mm/day)

NASA POWER Daily API endpoint:
    https://power.larc.nasa.gov/api/temporal/daily/point
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NASA_POWER_BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

PARAMETERS = [
    "T2M_MAX",
    "T2M_MIN",
    "T2M",
    "RH2M",
    "WS2M",
    "ALLSKY_SFC_SW_DWN",
    "PRECTOTCORR",
]

# Default look-back window (days) when no explicit date range is provided
DEFAULT_DAYS_BACK = 7

# Retry configuration
_MAX_RETRIES = 3
_BACKOFF_FACTOR = 1.0
_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

# Timeout (seconds) for each HTTP request
REQUEST_TIMEOUT = 60


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """Return a requests.Session with retry logic pre-configured."""
    session = requests.Session()
    retry = Retry(
        total=_MAX_RETRIES,
        read=_MAX_RETRIES,
        connect=_MAX_RETRIES,
        backoff_factor=_BACKOFF_FACTOR,
        status_forcelist=_RETRY_STATUS_CODES,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _date_str(d: date) -> str:
    """Return YYYYMMDD string for the NASA POWER API."""
    return d.strftime("%Y%m%d")


def _parse_power_response(raw: dict[str, Any], req_lat: float = 0.0, req_lon: float = 0.0) -> list[dict[str, Any]]:
    """
    Parse the NASA POWER JSON response and return a list of daily records.
    ...
    """
    try:
        properties = raw["properties"]
        parameter_data: dict[str, dict] = properties["parameter"]
        header = raw.get("header", {})
        # If header coords are 0.0 (default), use requested coords instead
        lat = header.get("latitude", 0.0)
        lon = header.get("longitude", 0.0)
        if lat == 0.0 and lon == 0.0 and (req_lat != 0.0 or req_lon != 0.0):
            lat, lon = req_lat, req_lon
    except KeyError as exc:
        raise ValueError(f"Unexpected NASA POWER response structure: {exc}") from exc

    # All parameter dicts share the same date keys (YYYYMMDD strings)
    date_keys = sorted(next(iter(parameter_data.values())).keys())

    records: list[dict[str, Any]] = []
    for dk in date_keys:
        if len(dk) != 8 or not dk.isdigit():
            logger.warning("Skipping unexpected date key: %s", dk)
            continue

        iso_date = f"{dk[:4]}-{dk[4:6]}-{dk[6:]}"
        record: dict[str, Any] = {
            "date": iso_date,
            "latitude": float(lat),
            "longitude": float(lon),
        }
        for param in PARAMETERS:
            value = parameter_data.get(param, {}).get(dk, -999.0)
            # NASA POWER uses -999 as a fill/missing value
            record[param] = None if float(value) <= -999 else float(value)

        records.append(record)

    return records


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_weather_data(
    latitude: float,
    longitude: float,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch daily weather data from the NASA POWER API for the given location
    and date range.

    Parameters
    ----------
    latitude : float
        Decimal latitude of the field (-90 to 90).
    longitude : float
        Decimal longitude of the field (-180 to 180).
    start_date : date, optional
        First day of the requested period (inclusive).  Defaults to
        ``DEFAULT_DAYS_BACK`` days before today.
    end_date : date, optional
        Last day of the requested period (inclusive).  Defaults to yesterday
        (NASA POWER data has ~1-day latency).

    Returns
    -------
    list[dict]
        List of daily weather records sorted by date ascending.  Each dict
        contains the keys listed in ``_parse_power_response``.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx response after all retries.
    ValueError
        If the response structure is unexpected.
    """
    today = date.today()

    if end_date is None:
        end_date = today - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=DEFAULT_DAYS_BACK - 1)

    params = {
        "parameters": ",".join(PARAMETERS),
        "community": "AG",
        "longitude": longitude,
        "latitude": latitude,
        "start": _date_str(start_date),
        "end": _date_str(end_date),
        "format": "JSON",
    }

    logger.info(
        "Fetching NASA POWER data | lat=%s lon=%s %s → %s",
        latitude,
        longitude,
        _date_str(start_date),
        _date_str(end_date),
    )

    session = _build_session()
    response = session.get(NASA_POWER_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    raw = response.json()
    records = _parse_power_response(raw, req_lat=latitude, req_lon=longitude)

    logger.info("Retrieved %d daily records from NASA POWER", len(records))
    return records


def fetch_latest_weather(
    latitude: float,
    longitude: float,
    days: int = DEFAULT_DAYS_BACK,
) -> list[dict[str, Any]]:
    """
    Convenience wrapper: fetch the most recent ``days`` days of weather data.

    Parameters
    ----------
    latitude : float
    longitude : float
    days : int
        Number of recent days to retrieve (default: 7).

    Returns
    -------
    list[dict]
    """
    if days < 1:
        raise ValueError("'days' must be at least 1")

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    return fetch_weather_data(latitude, longitude, start_date, end_date)


def build_field_weather_payload(
    field_id: str,
    crop_type: str,
    latitude: float,
    longitude: float,
    days: int = DEFAULT_DAYS_BACK,
) -> dict[str, Any]:
    """
    High-level helper: fetch weather and attach field metadata.

    Returns a payload dict suitable for passing to the irrigation rule engine.

    Parameters
    ----------
    field_id : str
    crop_type : str
    latitude : float
    longitude : float
    days : int

    Returns
    -------
    dict with keys:
        field_id, crop_type, latitude, longitude, weather_records
    """
    records = fetch_latest_weather(latitude, longitude, days)
    return {
        "field_id": field_id,
        "crop_type": crop_type,
        "latitude": latitude,
        "longitude": longitude,
        "weather_records": records,
    }
