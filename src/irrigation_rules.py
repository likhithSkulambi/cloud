"""
irrigation_rules.py
-------------------
FAO-56 Penman-Monteith rule engine for the Smart Irrigation Advisor.

This module implements the Reference Evapotranspiration (ET₀) calculation
following the FAO-56 methodology (Allen et al., 1998), combined with a
rule-based irrigation decision engine.

URGENCY LEVELS
--------------
    CRITICAL  – Immediate irrigation required; crop stress imminent
    HIGH      – Irrigate within 24 hours
    MODERATE  – Irrigate within 48 hours
    NONE      – No irrigation needed; soil moisture is adequate

IRRIGATION RULES (6)
--------------------
    Rule 1  – High ET₀ & low rainfall / high temperature
    Rule 2  – Consecutive dry days exceed threshold
    Rule 3  – Cumulative ET₀ exceeds crop water need
    Rule 4  – Low relative humidity triggers water stress
    Rule 5  – High wind speed accelerates transpiration loss
    Rule 6  – Recent heavy rainfall offsets irrigation need
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

# FAO CLIMWAT 2.0 reference dataset — imported lazily to avoid hard dependency
try:
    from fao_climwat import get_fao_validation_summary
    _FAO_AVAILABLE = True
except ImportError:
    try:
        from src.fao_climwat import get_fao_validation_summary
        _FAO_AVAILABLE = True
    except ImportError:
        _FAO_AVAILABLE = False
        logger.warning("fao_climwat module not found; FAO validation disabled")


# ---------------------------------------------------------------------------
# Enumerations & Data Classes
# ---------------------------------------------------------------------------

class UrgencyLevel(str, Enum):
    """Four-level irrigation urgency classification."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MODERATE = "MODERATE"
    NONE = "NONE"

    def numeric(self) -> int:
        """Return an integer ranking for comparison (higher = more urgent)."""
        return {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "NONE": 1}[self.value]


@dataclass
class RuleResult:
    """Result produced by a single irrigation rule."""
    rule_id: int
    rule_name: str
    triggered: bool
    urgency: UrgencyLevel
    reason: str
    et0_mm: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class IrrigationRecommendation:
    """Aggregated irrigation recommendation for a single field."""
    field_id: str
    crop_type: str
    final_urgency: UrgencyLevel
    recommended_water_mm: float
    cumulative_et0_mm: float
    cumulative_rain_mm: float
    net_water_deficit_mm: float
    triggered_rules: list[RuleResult]
    all_rules: list[RuleResult]
    summary: str
    simulated_moisture_percent: float = 50.0
    fao_validation: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Crop Kc Coefficients (simplified)
# ---------------------------------------------------------------------------

# Map crop_type → Kc (mid-season) for ET_crop = Kc * ET₀
# Source: FAO-56 Table 12 (selected crops)
CROP_KC: dict[str, float] = {
    "wheat":       1.15,
    "maize":       1.20,
    "rice":        1.20,
    "sugarcane":   1.25,
    "cotton":      1.20,
    "soybean":     1.15,
    "tomato":      1.15,
    "potato":      1.15,
    "sunflower":   1.20,
    "groundnut":   1.15,
    "sorghum":     1.10,
    "barley":      1.15,
    "default":     1.00,
}

def get_kc(crop_type: str) -> float:
    """Return the Kc coefficient for the given crop type."""
    return CROP_KC.get(crop_type.lower(), CROP_KC["default"])

SOIL_PROPERTIES = {
    "sandy": {"fc": 0.10, "pwp": 0.04}, # Drains quickly
    "loam":  {"fc": 0.25, "pwp": 0.12}, # Balanced
    "clay":  {"fc": 0.40, "pwp": 0.20}, # Retains water
    "default": {"fc": 0.20, "pwp": 0.10},
}


# ---------------------------------------------------------------------------
# FAO-56 ET₀ Calculation – Penman-Monteith
# ---------------------------------------------------------------------------

def compute_et0(
    t_max: float,
    t_min: float,
    t_mean: float,
    rh_mean: float,
    wind_speed: float,
    solar_radiation: float,
    latitude_deg: float,
    day_of_year: int,
) -> float:
    """
    Calculate Reference Evapotranspiration (ET₀) using the
    FAO-56 Penman-Monteith equation.

    Parameters (all SI units as specified in FAO-56)
    ----------
    t_max : float        Maximum daily temperature (°C)
    t_min : float        Minimum daily temperature (°C)
    t_mean : float       Mean daily temperature (°C)
    rh_mean : float      Mean relative humidity (%)
    wind_speed : float   Wind speed at 2 m height (m/s)
    solar_radiation : float  Incoming solar radiation (MJ/m²/day)
    latitude_deg : float  Decimal latitude (degrees)
    day_of_year : int    Day of year (1-365)

    Returns
    -------
    float : ET₀ in mm/day  (clamped to ≥ 0)
    """
    # Psychrometric constant γ (kPa/°C) at standard pressure (≈1013 hPa)
    γ = 0.0665  # simplified constant for sea level

    # Slope of saturation vapour pressure curve (kPa/°C)
    Δ = (4098 * (0.6108 * math.exp((17.27 * t_mean) / (t_mean + 237.3)))) / ((t_mean + 237.3) ** 2)

    # Saturation vapour pressure (kPa)
    e_s_max = 0.6108 * math.exp((17.27 * t_max) / (t_max + 237.3))
    e_s_min = 0.6108 * math.exp((17.27 * t_min) / (t_min + 237.3))
    e_s = (e_s_max + e_s_min) / 2.0

    # Actual vapour pressure (kPa)
    e_a = (rh_mean / 100.0) * e_s

    # Vapour pressure deficit (kPa)
    vpd = e_s - e_a

    # Latitude in radians
    φ = math.radians(latitude_deg)

    # Solar geometry
    dr = 1 + 0.033 * math.cos((2 * math.pi / 365) * day_of_year)
    δ = 0.409 * math.sin((2 * math.pi / 365) * day_of_year - 1.39)
    ωs = math.acos(-math.tan(φ) * math.tan(δ))

    # Extraterrestrial radiation Ra (MJ/m²/day)
    Gsc = 0.0820  # solar constant MJ/m²/min
    Ra = (
        (24 * 60 / math.pi)
        * Gsc
        * dr
        * (ωs * math.sin(φ) * math.sin(δ) + math.cos(φ) * math.cos(δ) * math.sin(ωs))
    )

    # Clear-sky short-wave radiation Rso (MJ/m²/day)
    elevation = 0  # assume sea level; refine with DEM if available
    Rso = (0.75 + 2e-5 * elevation) * Ra

    # Net short-wave radiation Rns (MJ/m²/day)
    α = 0.23  # albedo for reference grass surface
    Rns = (1 - α) * solar_radiation

    # Stefan-Boltzmann constant (MJ/m²/day·K⁴)
    σ = 4.903e-9

    # Net long-wave radiation Rnl (MJ/m²/day)
    Tk_max = t_max + 273.16
    Tk_min = t_min + 273.16
    Rs_Rso = min(solar_radiation / Rso, 1.0) if Rso > 0 else 0.0
    Rnl = (
        σ
        * ((Tk_max**4 + Tk_min**4) / 2)
        * (0.34 - 0.14 * math.sqrt(e_a))
        * (1.35 * Rs_Rso - 0.35)
    )

    # Net radiation Rn (MJ/m²/day)
    Rn = Rns - Rnl

    # Soil heat flux G (MJ/m²/day) – assumed ≈ 0 for daily
    G = 0.0

    # Penman-Monteith ET₀ (mm/day)
    numerator = 0.408 * Δ * (Rn - G) + γ * (900 / (t_mean + 273)) * wind_speed * vpd
    denominator = Δ + γ * (1 + 0.34 * wind_speed)

    et0 = numerator / denominator
    return max(et0, 0.0)


# ---------------------------------------------------------------------------
# Rule Engine
# ---------------------------------------------------------------------------

def _rule_high_et0_low_rain(record: dict[str, Any], et0: float) -> RuleResult:
    """
    Rule 1 – High ET₀ combined with low rainfall / high temperature.

    Triggers when daily ET₀ > 6 mm and rain < 2 mm, or temperature > 38°C.
    """
    rain = record.get("PRECTOTCORR") or 0.0
    t_max = record.get("T2M_MAX") or 30.0

    high_et0 = et0 > 6.0
    low_rain = rain < 2.0
    extreme_heat = t_max > 38.0

    triggered = (high_et0 and low_rain) or extreme_heat

    if extreme_heat:
        urgency = UrgencyLevel.CRITICAL
        reason = f"Extreme heat ({t_max:.1f}°C) and high ET₀ ({et0:.1f} mm/day)"
    elif high_et0 and low_rain:
        urgency = UrgencyLevel.HIGH
        reason = f"High ET₀ ({et0:.1f} mm/day) with minimal rainfall ({rain:.1f} mm)"
    else:
        urgency = UrgencyLevel.NONE
        reason = "ET₀ and rainfall within acceptable range"

    return RuleResult(
        rule_id=1,
        rule_name="High ET₀ & Low Rainfall",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        et0_mm=et0,
        details={"et0_mm": et0, "rain_mm": rain, "t_max": t_max},
    )


def _rule_consecutive_dry_days(
    records: list[dict[str, Any]], dry_day_threshold_mm: float = 1.0
) -> RuleResult:
    """
    Rule 2 – Consecutive dry days exceed threshold.

    Counts trailing dry days (rain < threshold_mm).
    Urgency scales with streak length.
    """
    streak = 0
    for rec in reversed(records):
        rain = rec.get("PRECTOTCORR") or 0.0
        if rain < dry_day_threshold_mm:
            streak += 1
        else:
            break

    if streak >= 7:
        triggered, urgency, reason = True, UrgencyLevel.CRITICAL, f"{streak} consecutive dry days – critical soil depletion risk"
    elif streak >= 5:
        triggered, urgency, reason = True, UrgencyLevel.HIGH, f"{streak} consecutive dry days – high water stress risk"
    elif streak >= 3:
        triggered, urgency, reason = True, UrgencyLevel.MODERATE, f"{streak} consecutive dry days – moderate water stress"
    else:
        triggered, urgency, reason = False, UrgencyLevel.NONE, f"Only {streak} dry day(s) – soil moisture likely adequate"

    return RuleResult(
        rule_id=2,
        rule_name="Consecutive Dry Days",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        details={"consecutive_dry_days": streak, "threshold_mm": dry_day_threshold_mm},
    )


def _rule_soil_water_depletion(
    dr: float, raw: float, taw: float
) -> RuleResult:
    """
    Rule 3 – Soil Water Depletion exceeds threshold.
    Uses proper FAO water balance (Total Available Water and Depletion).
    """
    if dr >= taw * 0.9:
        triggered, urgency, reason = True, UrgencyLevel.CRITICAL, f"Critical depletion ({dr:.1f} mm)! Soil near wilting point (TAW={taw:.1f} mm)."
    elif dr >= raw:
        triggered, urgency, reason = True, UrgencyLevel.HIGH, f"Depletion ({dr:.1f} mm) exceeds readily available water ({raw:.1f} mm)."
    elif dr >= raw * 0.7:
        triggered, urgency, reason = True, UrgencyLevel.MODERATE, f"Depletion ({dr:.1f} mm) approaching readily available water."
    else:
        triggered, urgency, reason = False, UrgencyLevel.NONE, f"Depletion ({dr:.1f} mm) is well within safe limits."

    return RuleResult(
        rule_id=3,
        rule_name="Soil Water Depletion",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        details={
            "depletion_mm": dr,
            "raw_mm": raw,
            "taw_mm": taw,
        },
    )


def _rule_low_humidity_stress(record: dict[str, Any]) -> RuleResult:
    """
    Rule 4 – Low relative humidity triggers water stress.

    Very low RH increases transpiration and canopy moisture demand.
    """
    rh = record.get("RH2M") or 50.0

    if rh < 20:
        triggered, urgency, reason = True, UrgencyLevel.CRITICAL, f"Critically low humidity {rh:.0f}% – acute plant water stress"
    elif rh < 30:
        triggered, urgency, reason = True, UrgencyLevel.HIGH, f"Low humidity {rh:.0f}% – elevated transpiration"
    elif rh < 40:
        triggered, urgency, reason = True, UrgencyLevel.MODERATE, f"Below-average humidity {rh:.0f}% – watch for moisture stress"
    else:
        triggered, urgency, reason = False, UrgencyLevel.NONE, f"Relative humidity {rh:.0f}% – no stress indicator"

    return RuleResult(
        rule_id=4,
        rule_name="Low Relative Humidity",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        details={"rh_percent": rh},
    )


def _rule_high_wind_speed(record: dict[str, Any], et0: float) -> RuleResult:
    """
    Rule 5 – High wind speed accelerates transpiration losses.

    Wind > 5 m/s significantly increases ET demand.
    """
    ws = record.get("WS2M") or 0.0

    if ws > 8.0:
        triggered, urgency, reason = True, UrgencyLevel.HIGH, f"Very high wind speed {ws:.1f} m/s – rapid moisture loss"
    elif ws > 5.0:
        triggered, urgency, reason = True, UrgencyLevel.MODERATE, f"High wind speed {ws:.1f} m/s – elevated ET demand"
    else:
        triggered, urgency, reason = False, UrgencyLevel.NONE, f"Wind speed {ws:.1f} m/s – within normal range"

    return RuleResult(
        rule_id=5,
        rule_name="High Wind Speed",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        et0_mm=et0,
        details={"wind_speed_ms": ws, "et0_mm": et0},
    )


def _rule_heavy_recent_rainfall(
    records: list[dict[str, Any]],
    saturation_threshold_mm: float = 30.0,
    lookback_days: int = 2,
) -> RuleResult:
    """
    Rule 6 – Recent heavy rainfall offsets irrigation need.

    If cumulative rain over the last ``lookback_days`` days exceeds the
    saturation threshold, irrigation can be skipped.
    """
    recent_rain = sum(
        (r.get("PRECTOTCORR") or 0.0) for r in records[-lookback_days:]
    )

    if recent_rain >= saturation_threshold_mm:
        triggered = False
        urgency = UrgencyLevel.NONE
        reason = f"Heavy recent rainfall ({recent_rain:.1f} mm in {lookback_days} days) – soil well watered; skip irrigation"
    elif recent_rain >= saturation_threshold_mm * 0.5:
        triggered = False
        urgency = UrgencyLevel.NONE
        reason = f"Moderate recent rainfall ({recent_rain:.1f} mm) – partial offset; monitor soil"
    else:
        triggered = True
        urgency = UrgencyLevel.NONE  # this rule can only suppress, not elevate urgency
        reason = f"Insufficient recent rainfall ({recent_rain:.1f} mm in {lookback_days} days)"

    return RuleResult(
        rule_id=6,
        rule_name="Heavy Recent Rainfall",
        triggered=triggered,
        urgency=urgency,
        reason=reason,
        details={"recent_rain_mm": recent_rain, "lookback_days": lookback_days},
    )


# ---------------------------------------------------------------------------
# Recommended water amount calculation
# ---------------------------------------------------------------------------

def compute_recommended_water(
    net_deficit_mm: float,
    application_efficiency: float = 0.80,
) -> float:
    """
    Convert net water deficit to gross irrigation depth accounting for
    application efficiency.

    Parameters
    ----------
    net_deficit_mm : float
        Net crop water deficit (mm).
    application_efficiency : float
        Irrigation system efficiency (default 0.80 = 80% for drip/sprinkler).

    Returns
    -------
    float : Recommended irrigation depth in mm (≥ 0).
    """
    if net_deficit_mm <= 0:
        return 0.0
    return round(net_deficit_mm / application_efficiency, 1)


# ---------------------------------------------------------------------------
# Public API – Evaluate Rules
# ---------------------------------------------------------------------------

def evaluate_irrigation_rules(
    field_id: str,
    crop_type: str,
    weather_records: list[dict[str, Any]],
    latitude: float = 20.0,
    application_efficiency: float = 0.80,
    soil_type: str = "default",
) -> IrrigationRecommendation:
    """
    Evaluate all 6 irrigation rules for the given field and return a
    consolidated IrrigationRecommendation.
    """
    if not weather_records:
        raise ValueError("weather_records must contain at least one entry")

    kc = get_kc(crop_type)
    
    soil = SOIL_PROPERTIES.get(soil_type.lower(), SOIL_PROPERTIES["default"])
    fc = soil["fc"]
    pwp = soil["pwp"]
    zr = 600.0  # Assumed root depth 600 mm
    taw = (fc - pwp) * zr
    p = 0.50  # Allowable depletion fraction
    raw = p * taw

    dr = 0.0  # Depletion at start of period
    et0_values: list[float] = []
    
    for rec in weather_records:
        try:
            from datetime import date as _date
            rec_date = _date.fromisoformat(rec["date"])
            doy = rec_date.timetuple().tm_yday
        except (KeyError, ValueError):
            doy = 180

        et0 = compute_et0(
            t_max=rec.get("T2M_MAX") or 30.0,
            t_min=rec.get("T2M_MIN") or 20.0,
            t_mean=rec.get("T2M") or 25.0,
            rh_mean=rec.get("RH2M") or 60.0,
            wind_speed=rec.get("WS2M") or 2.0,
            solar_radiation=rec.get("ALLSKY_SFC_SW_DWN") or 15.0,
            latitude_deg=latitude,
            day_of_year=doy,
        )
        et0_values.append(et0)
        
        etc = kc * et0
        rain = rec.get("PRECTOTCORR") or 0.0
        
        # Daily root zone water balance
        dr = dr + etc - rain
        
        if dr < 0:
            dr = 0.0
        elif dr > taw:
            dr = taw

    cumulative_et0 = sum(et0_values)
    cumulative_rain = sum((r.get("PRECTOTCORR") or 0.0) for r in weather_records)
    
    # Calculate simulated soil moisture as volumetric percentage
    simulated_moisture_percent = round((fc - (dr / zr)) * 100, 1)

    latest = weather_records[-1]
    latest_et0 = et0_values[-1]

    rule_results: list[RuleResult] = [
        _rule_high_et0_low_rain(latest, latest_et0),
        _rule_consecutive_dry_days(weather_records),
        _rule_soil_water_depletion(dr, raw, taw),
        _rule_low_humidity_stress(latest),
        _rule_high_wind_speed(latest, latest_et0),
        _rule_heavy_recent_rainfall(weather_records),
    ]

    rule6 = rule_results[5]
    heavy_rain_suppress = (
        rule6.details.get("recent_rain_mm", 0.0) >= 30.0
    )

    triggered = [r for r in rule_results if r.triggered]

    if heavy_rain_suppress:
        final_urgency = UrgencyLevel.NONE
    elif not triggered:
        final_urgency = UrgencyLevel.NONE
    else:
        final_urgency = max(triggered, key=lambda r: r.urgency.numeric()).urgency

    if final_urgency == UrgencyLevel.NONE:
        recommended_water = 0.0
    else:
        # Recommend enough water to refill exactly the current depletion back to Field Capacity
        recommended_water = compute_recommended_water(dr, application_efficiency)

    # ---------------------------------------------------------------------------
    # FAO CLIMWAT cross-validation
    # ---------------------------------------------------------------------------
    fao_validation: dict[str, Any] = {}
    if _FAO_AVAILABLE:
        try:
            fao_validation = get_fao_validation_summary(
                latitude=latitude,
                longitude=weather_records[0].get("longitude", 0.0),
                weather_records=weather_records,
                et0_values=et0_values,
            )
            fao_quality = fao_validation.get("data_quality_overall", "GOOD")
            fao_station = fao_validation.get("nearest_station", "")
            fao_dist = fao_validation.get("distance_km", 0)
            fao_ref_eto = fao_validation.get("avg_fao_reference_eto_mm", 0)
            logger.info(
                "FAO CLIMWAT validation: quality=%s, nearest=%s (%.0f km), "
                "FAO ref ET₀=%.2f mm/day, computed=%.2f mm/day",
                fao_quality, fao_station, fao_dist,
                fao_ref_eto,
                cumulative_et0 / max(len(et0_values), 1),
            )
        except Exception as exc:
            logger.warning("FAO CLIMWAT validation failed: %s", exc)
            fao_validation = {"error": str(exc)}

    triggered_names = ", ".join(f"Rule {r.rule_id}" for r in triggered) or "None"
    # Build summary — include FAO quality flag if available
    fao_note = ""
    if fao_validation and "data_quality_overall" in fao_validation:
        q = fao_validation["data_quality_overall"]
        ref = fao_validation.get("avg_fao_reference_eto_mm", 0)
        dev = fao_validation.get("avg_deviation_pct", 0)
        station = fao_validation.get("nearest_station", "")
        fao_note = (
            f" FAO CLIMWAT validation: {q} "
            f"(ref ET₀={ref:.2f} mm/day, deviation={dev:.1f}%, "
            f"nearest station: {station})."
        )

    summary = (
        f"Field '{field_id}' ({crop_type}): urgency={final_urgency.value}, "
        f"recommend {recommended_water:.1f} mm irrigation. "
        f"Depletion: {dr:.1f} mm / TAW {taw:.1f} mm."
        f"{fao_note}"
    )

    logger.info(summary)

    return IrrigationRecommendation(
        field_id=field_id,
        crop_type=crop_type,
        final_urgency=final_urgency,
        recommended_water_mm=recommended_water,
        cumulative_et0_mm=round(cumulative_et0, 2),
        cumulative_rain_mm=round(cumulative_rain, 2),
        net_water_deficit_mm=round(dr, 2),
        triggered_rules=triggered,
        all_rules=rule_results,
        summary=summary,
        simulated_moisture_percent=simulated_moisture_percent,
        fao_validation=fao_validation,
    )
