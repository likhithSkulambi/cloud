"""
test_irrigation_rules.py
------------------------
15 unit tests for the FAO-56 irrigation rule engine.

Run with:
    pytest tests/test_irrigation_rules.py -v
"""

from __future__ import annotations

import math
import sys
import os
import unittest
from unittest.mock import patch

from src.irrigation_rules import (
    UrgencyLevel,
    compute_et0,
    evaluate_irrigation_rules,
    get_kc,
    compute_recommended_water,
    _rule_consecutive_dry_days,
    _rule_heavy_recent_rainfall,
    _rule_cumulative_et0_vs_crop_need,
    _rule_low_humidity_stress,
    _rule_high_wind_speed,
    _rule_high_et0_low_rain,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    date_str: str = "2024-06-15",
    t_max: float = 30.0,
    t_min: float = 18.0,
    t_mean: float = 24.0,
    rh: float = 60.0,
    ws: float = 2.0,
    solar: float = 20.0,
    rain: float = 0.0,
    lat: float = 20.0,
    lon: float = 78.0,
) -> dict:
    return {
        "date": date_str,
        "T2M_MAX": t_max,
        "T2M_MIN": t_min,
        "T2M": t_mean,
        "RH2M": rh,
        "WS2M": ws,
        "ALLSKY_SFC_SW_DWN": solar,
        "PRECTOTCORR": rain,
        "latitude": lat,
        "longitude": lon,
    }


def _records_for_days(n: int, rain: float = 0.0) -> list[dict]:
    """Generate n consecutive records all with the given rain value."""
    from datetime import date, timedelta
    base = date(2024, 6, 1)
    return [
        _make_record(
            date_str=(base + timedelta(days=i)).isoformat(),
            rain=rain,
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

class TestGetKc(unittest.TestCase):
    """Tests 1-2: Crop Kc coefficient lookup."""

    def test_known_crop(self):
        """Test 1 – Known crop returns correct Kc."""
        self.assertAlmostEqual(get_kc("wheat"), 1.15)
        self.assertAlmostEqual(get_kc("MAIZE"), 1.20)

    def test_unknown_crop_returns_default(self):
        """Test 2 – Unknown crop returns default Kc of 1.0."""
        self.assertAlmostEqual(get_kc("unknown_crop_xyz"), 1.00)


class TestComputeET0(unittest.TestCase):
    """Tests 3-5: ET₀ Penman-Monteith calculation."""

    def test_et0_positive_value(self):
        """Test 3 – ET₀ is strictly positive under normal sunny conditions."""
        et0 = compute_et0(35, 20, 27.5, 50, 3, 22, 20.0, 170)
        self.assertGreater(et0, 0.0)

    def test_et0_clamped_to_zero(self):
        """Test 4 – ET₀ is never negative (cold, humid, zero solar)."""
        et0 = compute_et0(-5, -15, -10, 95, 0.5, 2, 60.0, 355)
        self.assertGreaterEqual(et0, 0.0)

    def test_et0_higher_with_more_solar(self):
        """Test 5 – Higher solar radiation produces higher ET₀."""
        et0_low = compute_et0(30, 18, 24, 60, 2, 10, 20.0, 170)
        et0_high = compute_et0(30, 18, 24, 60, 2, 28, 20.0, 170)
        self.assertGreater(et0_high, et0_low)


class TestComputeRecommendedWater(unittest.TestCase):
    """Tests 6-7: Irrigation depth calculation."""

    def test_positive_deficit(self):
        """Test 6 – Positive deficit yields recommended water > 0."""
        water = compute_recommended_water(20.0, 0.80)
        self.assertAlmostEqual(water, 25.0)

    def test_zero_deficit(self):
        """Test 7 – Zero or negative deficit returns 0 recommended water."""
        self.assertEqual(compute_recommended_water(0.0), 0.0)
        self.assertEqual(compute_recommended_water(-5.0), 0.0)


class TestRuleConsecutiveDryDays(unittest.TestCase):
    """Tests 8-9: Rule 2 – Consecutive dry days."""

    def test_critical_streak(self):
        """Test 8 – 7+ consecutive dry days triggers CRITICAL urgency."""
        records = _records_for_days(7, rain=0.0)
        result = _rule_consecutive_dry_days(records)
        self.assertTrue(result.triggered)
        self.assertEqual(result.urgency, UrgencyLevel.CRITICAL)

    def test_short_streak_no_trigger(self):
        """Test 9 – 2 dry days does not trigger the rule."""
        records = _records_for_days(2, rain=0.0)
        result = _rule_consecutive_dry_days(records)
        self.assertFalse(result.triggered)
        self.assertEqual(result.urgency, UrgencyLevel.NONE)


class TestRuleHeavyRainfall(unittest.TestCase):
    """Tests 10-11: Rule 6 – Heavy recent rainfall suppression."""

    def test_heavy_rain_suppresses(self):
        """Test 10 – ≥ 30 mm rain in last 2 days returns NONE urgency."""
        records = _records_for_days(5, rain=15.0)  # 15 mm/day → 30 mm over last 2
        result = _rule_heavy_recent_rainfall(records, saturation_threshold_mm=30.0, lookback_days=2)
        self.assertEqual(result.urgency, UrgencyLevel.NONE)
        self.assertFalse(result.triggered)  # suppression rules have triggered=False

    def test_dry_recent_period_triggers(self):
        """Test 11 – Low recent rain marks the rule as triggered (insufficient offset)."""
        records = _records_for_days(5, rain=0.0)
        result = _rule_heavy_recent_rainfall(records, saturation_threshold_mm=30.0, lookback_days=2)
        self.assertTrue(result.triggered)


class TestRuleLowHumidity(unittest.TestCase):
    """Test 12: Rule 4 – Low relative humidity."""

    def test_critical_humidity(self):
        """Test 12 – RH < 20% triggers CRITICAL urgency."""
        rec = _make_record(rh=15.0)
        result = _rule_low_humidity_stress(rec)
        self.assertTrue(result.triggered)
        self.assertEqual(result.urgency, UrgencyLevel.CRITICAL)


class TestRuleHighWindSpeed(unittest.TestCase):
    """Test 13: Rule 5 – High wind speed."""

    def test_high_wind_triggers_moderate(self):
        """Test 13 – Wind speed 6 m/s triggers MODERATE urgency."""
        rec = _make_record(ws=6.0)
        result = _rule_high_wind_speed(rec, et0=5.0)
        self.assertTrue(result.triggered)
        self.assertEqual(result.urgency, UrgencyLevel.MODERATE)


class TestEvaluateIrrigationRules(unittest.TestCase):
    """Tests 14-15: Full evaluate_irrigation_rules integration."""

    def test_dry_hot_conditions_yield_critical(self):
        """Test 14 – Hot, dry, low-humidity 7-day window yields CRITICAL recommendation."""
        records = [
            _make_record(
                date_str=f"2024-06-{i+1:02d}",
                t_max=42.0,
                t_min=28.0,
                t_mean=35.0,
                rh=18.0,
                ws=7.0,
                solar=25.0,
                rain=0.0,
            )
            for i in range(7)
        ]
        rec = evaluate_irrigation_rules("field-test", "wheat", records, latitude=20.0)
        self.assertEqual(rec.final_urgency, UrgencyLevel.CRITICAL)
        self.assertGreater(rec.recommended_water_mm, 0)

    def test_rainy_conditions_yield_none(self):
        """Test 15 – Heavy rainfall throughout week yields NONE urgency."""
        records = [
            _make_record(
                date_str=f"2024-07-{i+1:02d}",
                t_max=27.0,
                t_min=20.0,
                t_mean=23.5,
                rh=85.0,
                ws=1.5,
                solar=10.0,
                rain=20.0,  # Abundant rain every day
            )
            for i in range(7)
        ]
        rec = evaluate_irrigation_rules("field-wet", "rice", records, latitude=15.0)
        self.assertEqual(rec.final_urgency, UrgencyLevel.NONE)
        self.assertEqual(rec.recommended_water_mm, 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
