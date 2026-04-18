"""
fao_climwat.py
--------------
FAO CLIMWAT 2.0 climate dataset integration for the Smart Irrigation Advisor.

FAO CLIMWAT 2.0 is the official FAO climatic database designed specifically for
use with the FAO CROPWAT model and FAO-56 Penman-Monteith methodology. It
contains monthly climate data for approximately 5,000 meteorological stations
worldwide.

Reference:
    Smith, M. (1993). CLIMWAT for CROPWAT: A climatic database for irrigation
    planning and management. FAO Irrigation and Drainage Paper 49. Rome.
    https://www.fao.org/land-water/databases-and-software/climwat-for-cropwat/en/

This module provides:
    - load_climwat_stations()       Load all CLIMWAT stations into memory
    - find_nearest_station()        Haversine nearest-neighbour lookup
    - get_monthly_reference_eto()   Look up FAO monthly reference ET₀ for a location
    - validate_computed_eto()       Cross-validate NASA POWER ET₀ vs FAO reference
    - get_fao_climate_context()     Return full monthly climate context for a station

Data file: data/fao_climwat_stations.csv
    Columns: station_id, station_name, country, latitude, longitude, elevation_m,
             jan_eto … dec_eto (mm/day),
             jan_tmax … dec_tmax (°C),
             jan_tmin … dec_tmin (°C),
             jan_rh … dec_rh (%),
             jan_wind … dec_wind (m/s)
"""

from __future__ import annotations

import csv
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONTHS = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]

# Path to the bundled CLIMWAT CSV, relative to this module's directory
_DATA_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data", "fao_climwat_stations.csv",
)

# Validation thresholds
ET0_WARNING_DEVIATION_PCT = 30.0   # >30% deviation → warn
ET0_CRITICAL_DEVIATION_PCT = 60.0  # >60% deviation → critical data quality flag


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class ClimwatStation:
    """A single FAO CLIMWAT 2.0 weather station with monthly climate data."""
    station_id: str
    station_name: str
    country: str
    latitude: float
    longitude: float
    elevation_m: float

    # Monthly reference ET₀ (mm/day) — Jan=index 0, Dec=index 11
    monthly_eto: list[float]

    # Monthly Tmax, Tmin (°C)
    monthly_tmax: list[float]
    monthly_tmin: list[float]

    # Monthly relative humidity (%)
    monthly_rh: list[float]

    # Monthly wind speed at 2m (m/s)
    monthly_wind: list[float]

    def eto_for_month(self, month: int) -> float:
        """Return reference ET₀ for month (1=Jan … 12=Dec)."""
        return self.monthly_eto[month - 1]

    def tmax_for_month(self, month: int) -> float:
        return self.monthly_tmax[month - 1]

    def tmin_for_month(self, month: int) -> float:
        return self.monthly_tmin[month - 1]

    def rh_for_month(self, month: int) -> float:
        return self.monthly_rh[month - 1]

    def wind_for_month(self, month: int) -> float:
        return self.monthly_wind[month - 1]


@dataclass
class EToValidationResult:
    """Result of cross-validating computed ET₀ against FAO CLIMWAT reference."""
    station_id: str
    station_name: str
    station_country: str
    distance_km: float
    month: int

    # FAO CLIMWAT reference values
    fao_reference_eto_mm: float
    fao_tmax_c: float
    fao_tmin_c: float
    fao_rh_pct: float
    fao_wind_ms: float

    # Computed (NASA POWER-derived) ET₀
    computed_eto_mm: float

    # Deviation metrics
    deviation_mm: float
    deviation_pct: float
    data_quality: str   # "GOOD" | "WARNING" | "CRITICAL"
    quality_message: str

    # Additional FAO context
    fao_station_elevation_m: float


@dataclass
class FaoClimateContext:
    """
    Full FAO CLIMWAT climate context for a location and month.
    Used for dashboard display and report generation.
    """
    nearest_station: ClimwatStation
    distance_km: float
    month: int
    month_name: str
    validation: EToValidationResult
    annual_avg_eto_mm: float       # Annual average ET₀ (mm/day)
    wettest_month: str             # Month name with lowest ET₀
    driest_month: str              # Month name with highest ET₀
    annual_total_eto_mm: float     # Approx annual ET₀ total (mm)


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

_STATIONS_CACHE: list[ClimwatStation] | None = None


def _parse_float_list(row: dict, prefix: str) -> list[float]:
    """Extract 12 monthly float values for a given column prefix."""
    return [float(row[f"{m}_{prefix}"]) for m in MONTHS]


def load_climwat_stations(data_file: str | None = None) -> list[ClimwatStation]:
    """
    Load FAO CLIMWAT 2.0 stations from the bundled CSV file.
    Results are cached in memory after the first call.

    Parameters
    ----------
    data_file : str, optional
        Override the default data file path.

    Returns
    -------
    list[ClimwatStation]
    """
    global _STATIONS_CACHE
    if _STATIONS_CACHE is not None:
        return _STATIONS_CACHE

    path = data_file or _DATA_FILE
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"FAO CLIMWAT data file not found at: {path}. "
            "Ensure data/fao_climwat_stations.csv is present."
        )

    stations: list[ClimwatStation] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                station = ClimwatStation(
                    station_id=row["station_id"],
                    station_name=row["station_name"],
                    country=row["country"],
                    latitude=float(row["latitude"]),
                    longitude=float(row["longitude"]),
                    elevation_m=float(row["elevation_m"]),
                    monthly_eto=_parse_float_list(row, "eto"),
                    monthly_tmax=_parse_float_list(row, "tmax"),
                    monthly_tmin=_parse_float_list(row, "tmin"),
                    monthly_rh=_parse_float_list(row, "rh"),
                    monthly_wind=_parse_float_list(row, "wind"),
                )
                stations.append(station)
            except (KeyError, ValueError) as exc:
                logger.warning("Skipping malformed CLIMWAT row '%s': %s",
                               row.get("station_id", "?"), exc)

    _STATIONS_CACHE = stations
    logger.info("Loaded %d FAO CLIMWAT stations", len(stations))
    return stations


# ---------------------------------------------------------------------------
# Spatial lookup — Haversine nearest neighbour
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in kilometres between two lat/lon points."""
    R = 6371.0  # Earth radius (km)
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_nearest_station(
    latitude: float,
    longitude: float,
    stations: list[ClimwatStation] | None = None,
) -> tuple[ClimwatStation, float]:
    """
    Find the nearest FAO CLIMWAT station to the given coordinates.

    Parameters
    ----------
    latitude : float
    longitude : float
    stations : list[ClimwatStation], optional
        Pre-loaded station list (loads from disk if None).

    Returns
    -------
    tuple[ClimwatStation, float]
        (nearest_station, distance_km)
    """
    if stations is None:
        stations = load_climwat_stations()

    if not stations:
        raise ValueError("No CLIMWAT stations loaded.")

    best: ClimwatStation = stations[0]
    best_dist = _haversine_km(latitude, longitude, stations[0].latitude, stations[0].longitude)

    for s in stations[1:]:
        d = _haversine_km(latitude, longitude, s.latitude, s.longitude)
        if d < best_dist:
            best, best_dist = s, d

    logger.debug(
        "Nearest CLIMWAT station: %s (%s) at %.1f km",
        best.station_name, best.country, best_dist,
    )
    return best, best_dist


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_monthly_reference_eto(
    latitude: float,
    longitude: float,
    month: int,
) -> tuple[float, ClimwatStation, float]:
    """
    Return the FAO CLIMWAT monthly reference ET₀ for the given location.

    Parameters
    ----------
    latitude : float
    longitude : float
    month : int   Calendar month (1=Jan … 12=Dec)

    Returns
    -------
    tuple[float, ClimwatStation, float]
        (reference_eto_mm_per_day, nearest_station, distance_km)
    """
    station, dist_km = find_nearest_station(latitude, longitude)
    eto = station.eto_for_month(month)
    logger.info(
        "FAO CLIMWAT ET₀ for month %d at (%.2f, %.2f): %.2f mm/day "
        "[station: %s, %s, %.0f km away]",
        month, latitude, longitude, eto,
        station.station_name, station.country, dist_km,
    )
    return eto, station, dist_km


def validate_computed_eto(
    latitude: float,
    longitude: float,
    month: int,
    computed_eto_mm: float,
) -> EToValidationResult:
    """
    Cross-validate a computed ET₀ value (from NASA POWER + Penman-Monteith)
    against the FAO CLIMWAT reference for the nearest station.

    Parameters
    ----------
    latitude : float
    longitude : float
    month : int        Calendar month (1=Jan … 12=Dec)
    computed_eto_mm : float   Daily ET₀ to validate (mm/day)

    Returns
    -------
    EToValidationResult
    """
    fao_eto, station, dist_km = get_monthly_reference_eto(latitude, longitude, month)

    deviation_mm = computed_eto_mm - fao_eto
    deviation_pct = (abs(deviation_mm) / fao_eto * 100.0) if fao_eto > 0 else 0.0

    if deviation_pct <= ET0_WARNING_DEVIATION_PCT:
        quality = "GOOD"
        message = (
            f"Computed ET₀ ({computed_eto_mm:.2f} mm/day) is within "
            f"{deviation_pct:.1f}% of FAO CLIMWAT reference "
            f"({fao_eto:.2f} mm/day) — data quality is good."
        )
    elif deviation_pct <= ET0_CRITICAL_DEVIATION_PCT:
        quality = "WARNING"
        message = (
            f"Computed ET₀ ({computed_eto_mm:.2f} mm/day) deviates "
            f"{deviation_pct:.1f}% from FAO CLIMWAT reference "
            f"({fao_eto:.2f} mm/day). Verify sensor/API data."
        )
    else:
        quality = "CRITICAL"
        message = (
            f"Computed ET₀ ({computed_eto_mm:.2f} mm/day) deviates "
            f"{deviation_pct:.1f}% from FAO CLIMWAT reference "
            f"({fao_eto:.2f} mm/day). Possible data error — "
            f"check NASA POWER input parameters."
        )

    if quality != "GOOD":
        logger.warning("ET₀ validation %s: %s", quality, message)

    return EToValidationResult(
        station_id=station.station_id,
        station_name=station.station_name,
        station_country=station.country,
        distance_km=round(dist_km, 1),
        month=month,
        fao_reference_eto_mm=fao_eto,
        fao_tmax_c=station.tmax_for_month(month),
        fao_tmin_c=station.tmin_for_month(month),
        fao_rh_pct=station.rh_for_month(month),
        fao_wind_ms=station.wind_for_month(month),
        computed_eto_mm=round(computed_eto_mm, 3),
        deviation_mm=round(deviation_mm, 3),
        deviation_pct=round(deviation_pct, 1),
        data_quality=quality,
        quality_message=message,
        fao_station_elevation_m=station.elevation_m,
    )


def get_fao_climate_context(
    latitude: float,
    longitude: float,
    month: int,
    computed_eto_mm: float,
) -> FaoClimateContext:
    """
    Return a rich FAO CLIMWAT climate context for the given location and month.
    Includes nearest station info, ET₀ validation, annual stats.

    Parameters
    ----------
    latitude : float
    longitude : float
    month : int
    computed_eto_mm : float

    Returns
    -------
    FaoClimateContext
    """
    station, dist_km = find_nearest_station(latitude, longitude)
    validation = validate_computed_eto(latitude, longitude, month, computed_eto_mm)

    eto_values = station.monthly_eto
    annual_avg = round(sum(eto_values) / 12.0, 2)
    annual_total = round(sum(
        eto_values[m] * _days_in_month(m + 1) for m in range(12)
    ), 1)

    # Month with lowest and highest ET₀
    min_idx = eto_values.index(min(eto_values))
    max_idx = eto_values.index(max(eto_values))

    return FaoClimateContext(
        nearest_station=station,
        distance_km=round(dist_km, 1),
        month=month,
        month_name=_month_name(month),
        validation=validation,
        annual_avg_eto_mm=annual_avg,
        wettest_month=_month_name(min_idx + 1),
        driest_month=_month_name(max_idx + 1),
        annual_total_eto_mm=annual_total,
    )


def get_fao_validation_summary(
    latitude: float,
    longitude: float,
    weather_records: list[dict],
    et0_values: list[float],
) -> dict[str, Any]:
    """
    Produce a JSON-serialisable summary of FAO CLIMWAT validation across
    a list of weather records and their computed ET₀ values.

    Intended for inclusion in the API response and BigQuery storage.

    Parameters
    ----------
    latitude : float
    longitude : float
    weather_records : list[dict]   Records from NASA POWER (must have 'date' key)
    et0_values : list[float]       Computed ET₀ per record (parallel list)

    Returns
    -------
    dict with keys:
        nearest_station, distance_km, data_quality_overall,
        avg_computed_eto, avg_fao_reference_eto, avg_deviation_pct,
        monthly_validations (list)
    """
    if not weather_records or not et0_values:
        return {"error": "No records to validate"}

    from datetime import date as _date

    station, dist_km = find_nearest_station(latitude, longitude)
    monthly_results: dict[int, list[EToValidationResult]] = {}

    for rec, et0 in zip(weather_records, et0_values):
        try:
            d = _date.fromisoformat(str(rec["date"])[:10])
            m = d.month
        except (KeyError, ValueError):
            continue

        vr = validate_computed_eto(latitude, longitude, m, et0)
        monthly_results.setdefault(m, []).append(vr)

    if not monthly_results:
        return {"error": "Could not parse dates from records"}

    # Aggregate across all validated records
    all_results = [r for rs in monthly_results.values() for r in rs]
    avg_computed = round(sum(r.computed_eto_mm for r in all_results) / len(all_results), 3)
    avg_fao = round(sum(r.fao_reference_eto_mm for r in all_results) / len(all_results), 3)
    avg_dev_pct = round(sum(r.deviation_pct for r in all_results) / len(all_results), 1)

    # Overall quality = worst quality seen
    quality_rank = {"GOOD": 0, "WARNING": 1, "CRITICAL": 2}
    overall_quality = max(all_results, key=lambda r: quality_rank.get(r.data_quality, 0)).data_quality

    return {
        "nearest_station": station.station_name,
        "station_country": station.country,
        "station_id": station.station_id,
        "distance_km": round(dist_km, 1),
        "data_quality_overall": overall_quality,
        "avg_computed_eto_mm": avg_computed,
        "avg_fao_reference_eto_mm": avg_fao,
        "avg_deviation_pct": avg_dev_pct,
        "monthly_validations": [
            {
                "month": m,
                "month_name": _month_name(m),
                "fao_reference_eto_mm": rs[0].fao_reference_eto_mm,
                "avg_computed_eto_mm": round(
                    sum(r.computed_eto_mm for r in rs) / len(rs), 3
                ),
                "deviation_pct": round(
                    sum(r.deviation_pct for r in rs) / len(rs), 1
                ),
                "data_quality": max(rs, key=lambda r: quality_rank.get(r.data_quality, 0)).data_quality,
            }
            for m, rs in sorted(monthly_results.items())
        ],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _days_in_month(month: int) -> int:
    """Return approximate number of days in a month (non-leap year)."""
    return [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]


def _month_name(month: int) -> str:
    return ["January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"][month - 1]
