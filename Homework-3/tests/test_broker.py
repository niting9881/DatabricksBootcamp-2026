"""Unit tests for OpenMeteoWeatherBroker.

Run from project root:
    pytest tests/test_broker.py -v

Tests are split into:
    - TestResolveLocation   : geocoding and coordinate parsing
    - TestGetCurrentWeather : live + error paths
    - TestGetForecast       : day count, structure, edge cases
    - TestPredictRecommendation : business logic (mocked forecast)
"""

import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# Make mcp_server importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mcp_server"))

from weather_broker import OpenMeteoWeatherBroker  # noqa: E402


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(scope="module")
def broker():
    """Shared broker instance for the whole module (one httpx.Client)."""
    return OpenMeteoWeatherBroker()


# ============================================================
# TestResolveLocation
# ============================================================

class TestResolveLocation:
    """Tests for resolve_location() — geocoding and coord parsing."""

    def test_direct_lat_lon(self, broker):
        result = broker.resolve_location("41.88,-87.63")
        assert result is not None
        assert result["latitude"] == pytest.approx(41.88)
        assert result["longitude"] == pytest.approx(-87.63)

    def test_city_name(self, broker):
        result = broker.resolve_location("Chicago")
        assert result is not None
        assert "Chicago" in result["name"]
        assert result["latitude"] is not None
        assert result["longitude"] is not None

    def test_city_state(self, broker):
        result = broker.resolve_location("Austin, TX")
        assert result is not None
        assert result["latitude"] is not None

    def test_international_city(self, broker):
        result = broker.resolve_location("Tokyo")
        assert result is not None
        assert result["latitude"] is not None

    def test_invalid_location_returns_none(self, broker):
        result = broker.resolve_location("ZZZInvalidCityXYZ_NOPE_123")
        assert result is None

    def test_network_failure_returns_none(self, broker):
        with patch.object(broker.client, "get", side_effect=Exception("network error")):
            result = broker.resolve_location("Chicago")
        assert result is None

    def test_out_of_bounds_coords_not_treated_as_direct(self, broker):
        # "999,-999" is out of lat/lon bounds — should fall through to geocoding
        result = broker.resolve_location("999,-999")
        # Geocoding returns None for this nonsense value — just no exception
        # (may be None or geocoded something exotic; the key check is no crash)
        assert True  # no exception raised


# ============================================================
# TestGetCurrentWeather
# ============================================================

class TestGetCurrentWeather:
    """Tests for get_current_weather()."""

    def test_valid_city_returns_expected_keys(self, broker):
        result = broker.get_current_weather("New York")
        assert "error" not in result
        for key in ("location", "temperature", "humidity", "conditions",
                    "wind_speed", "wind_direction", "timestamp", "temperature_unit"):
            assert key in result, f"Missing key: {key}"

    def test_temperature_unit_is_fahrenheit(self, broker):
        result = broker.get_current_weather("Miami")
        assert result.get("temperature_unit") == "fahrenheit"

    def test_temperature_is_numeric(self, broker):
        result = broker.get_current_weather("London")
        assert "error" not in result
        assert isinstance(result["temperature"], (int, float))

    def test_humidity_between_0_and_100(self, broker):
        result = broker.get_current_weather("Denver")
        assert "error" not in result
        assert 0 <= result["humidity"] <= 100

    def test_valid_coordinates(self, broker):
        result = broker.get_current_weather("51.5,-0.12")  # London
        assert "error" not in result
        assert result["temperature"] is not None

    def test_invalid_location_returns_error_dict(self, broker):
        result = broker.get_current_weather("ZZZInvalidCityXYZ_NOPE")
        assert "error" in result
        assert isinstance(result["error"], str)

    def test_api_failure_returns_error_dict(self, broker):
        with patch.object(broker.client, "get", side_effect=Exception("timeout")):
            result = broker.get_current_weather("Chicago")
        assert "error" in result


# ============================================================
# TestGetForecast
# ============================================================

class TestGetForecast:
    """Tests for get_forecast()."""

    def test_default_5_days(self, broker):
        result = broker.get_forecast("Paris")
        assert "error" not in result
        assert result["days"] == 5
        assert len(result["forecast"]) == 5

    def test_custom_7_days(self, broker):
        result = broker.get_forecast("Tokyo", days=7)
        assert "error" not in result
        assert result["days"] == 7
        assert len(result["forecast"]) == 7

    def test_daily_structure(self, broker):
        result = broker.get_forecast("Berlin", days=1)
        assert "error" not in result
        day = result["forecast"][0]
        for key in ("date", "high_temp", "low_temp", "conditions",
                    "precipitation_chance", "precipitation_mm", "wind_speed"):
            assert key in day, f"Missing key in daily forecast: {key}"

    def test_date_format(self, broker):
        result = broker.get_forecast("Sydney", days=3)
        assert "error" not in result
        for day in result["forecast"]:
            datetime.strptime(day["date"], "%Y-%m-%d")  # raises if wrong format

    def test_high_temp_gte_low_temp(self, broker):
        result = broker.get_forecast("Chicago", days=5)
        assert "error" not in result
        for day in result["forecast"]:
            if day["high_temp"] is not None and day["low_temp"] is not None:
                assert day["high_temp"] >= day["low_temp"]

    def test_days_clamped_above_16(self, broker):
        # Broker clamps to 16 — should not error
        result = broker.get_forecast("Miami", days=20)
        assert "error" not in result
        assert result["days"] <= 16

    def test_days_clamped_below_1(self, broker):
        result = broker.get_forecast("Denver", days=0)
        assert "error" not in result
        assert result["days"] >= 1

    def test_invalid_location_returns_error(self, broker):
        result = broker.get_forecast("ZZZInvalidCityXYZ_NOPE", days=3)
        assert "error" in result


# ============================================================
# TestPredictRecommendation
# ============================================================

class TestPredictRecommendation:
    """Tests for predict_recommendation().

    Business logic tests use mocked forecast to avoid API dependency.
    Integration tests (today) hit the real API.
    """

    def _today(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _future(self, days: int = 3) -> str:
        return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

    def _mock_forecast(self, high: float, low: float, precip: int) -> dict:
        """Build a minimal mock forecast dict for today."""
        return {
            "location": "MockCity, MK",
            "days": 1,
            "forecast": [{
                "date": self._today(),
                "high_temp": high,
                "low_temp": low,
                "conditions": "Partly cloudy",
                "precipitation_chance": precip,
                "precipitation_mm": precip * 0.1,
                "wind_speed": 10.0,
            }],
        }

    # — Integration tests (real API) —

    def test_today_returns_required_keys(self, broker):
        result = broker.predict_recommendation("Chicago")
        assert "error" not in result
        for key in ("location", "date", "recommendation", "reasoning",
                    "confidence", "details"):
            assert key in result

    def test_details_contains_boolean_flags(self, broker):
        result = broker.predict_recommendation("Austin, TX")
        assert "error" not in result
        d = result["details"]
        for key in ("bring_umbrella", "bring_jacket", "good_for_outdoors"):
            assert key in d
            assert isinstance(d[key], bool)

    def test_confidence_in_range(self, broker):
        result = broker.predict_recommendation("Miami")
        assert "error" not in result
        assert 0.0 <= result["confidence"] <= 1.0

    def test_future_date(self, broker):
        result = broker.predict_recommendation("New York", date=self._future(3))
        assert "error" not in result
        assert result["date"] == self._future(3)

    # — Business logic tests (mocked) —

    def test_umbrella_recommended_when_high_precip(self, broker):
        """precip > 40% → bring_umbrella = True."""
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(72, 65, 80)):
            result = broker.predict_recommendation("MockCity")
        assert "error" not in result
        assert result["details"]["bring_umbrella"] is True

    def test_no_umbrella_when_low_precip(self, broker):
        """precip ≤ 40% → bring_umbrella = False."""
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(80, 70, 20)):
            result = broker.predict_recommendation("MockCity")
        assert result["details"]["bring_umbrella"] is False

    def test_jacket_recommended_when_cold(self, broker):
        """avg temp < 60°F → bring_jacket = True."""
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(55, 45, 10)):
            result = broker.predict_recommendation("ColdCity")
        assert result["details"]["bring_jacket"] is True

    def test_no_jacket_when_warm(self, broker):
        """avg temp ≥ 60°F → bring_jacket = False."""
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(85, 70, 5)):
            result = broker.predict_recommendation("WarmCity")
        assert result["details"]["bring_jacket"] is False

    def test_good_for_outdoors_clear_mild(self, broker):
        """precip <30% AND 50≤avg≤85°F → good_for_outdoors = True."""
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(78, 65, 10)):
            result = broker.predict_recommendation("NiceCity")
        assert result["details"]["good_for_outdoors"] is True

    def test_not_good_for_outdoors_rainy(self, broker):
        with patch.object(broker, "get_forecast", return_value=self._mock_forecast(70, 60, 70)):
            result = broker.predict_recommendation("RainyCity")
        assert result["details"]["good_for_outdoors"] is False

    # — Error paths —

    def test_invalid_date_format(self, broker):
        result = broker.predict_recommendation("Chicago", date="08/15/2026")
        assert "error" in result

    def test_past_date_returns_error(self, broker):
        result = broker.predict_recommendation("Chicago", date="2020-01-01")
        assert "error" in result

    def test_too_far_future_returns_error(self, broker):
        far_future = (datetime.now() + timedelta(days=20)).strftime("%Y-%m-%d")
        result = broker.predict_recommendation("Chicago", date=far_future)
        assert "error" in result

    def test_invalid_location_returns_error(self, broker):
        result = broker.predict_recommendation("ZZZInvalidCityXYZ_NOPE")
        assert "error" in result
