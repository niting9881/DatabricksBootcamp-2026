"""Unit tests for Weather MCP Server tools.

Run from project root:
    pytest tests/test_mcp_server.py -v

Tests verify:
    - All tools return a string (required by MCP)
    - All strings are valid JSON
    - Successful responses contain expected fields
    - Error paths return {"error": "..."} JSON (never raise)
    - Invalid inputs are rejected cleanly
"""

import json
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

# Make mcp_server importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mcp_server"))

from weather_mcp_server import (  # noqa: E402
    broker,
    format_error,
    format_response,
    get_current_weather,
    get_forecast,
    predict_weather_recommendation,
)


# ============================================================
# Utility helpers
# ============================================================

def parse(json_str: str) -> dict:
    """Parse a JSON string, failing the test with a clear message if invalid."""
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as exc:
        pytest.fail(f"Response is not valid JSON: {exc}\nResponse: {json_str!r}")


# ============================================================
# TestUtilityFunctions
# ============================================================

class TestUtilityFunctions:

    def test_format_response_is_valid_json(self):
        result = format_response({"key": "value", "num": 42})
        data = parse(result)
        assert data["key"] == "value"
        assert data["num"] == 42

    def test_format_error_has_error_key(self):
        result = format_error("Something went wrong")
        data = parse(result)
        assert "error" in data
        assert data["error"] == "Something went wrong"

    def test_format_response_handles_none_values(self):
        result = format_response({"a": None, "b": 1.5})
        data = parse(result)
        assert data["a"] is None


# ============================================================
# TestGetCurrentWeatherTool
# ============================================================

class TestGetCurrentWeatherTool:

    def test_returns_string(self):
        assert isinstance(get_current_weather("Chicago"), str)

    def test_returns_valid_json(self):
        parse(get_current_weather("Chicago"))

    def test_successful_response_contains_fields(self):
        data = parse(get_current_weather("New York"))
        assert "error" not in data
        for key in ("location", "temperature", "conditions", "humidity"):
            assert key in data

    def test_invalid_location_returns_error_json(self):
        data = parse(get_current_weather("ZZZInvalidCityXYZ_NOPE"))
        assert "error" in data
        assert isinstance(data["error"], str)

    def test_broker_exception_returns_error_json(self):
        with patch.object(broker, "get_current_weather", side_effect=RuntimeError("API down")):
            data = parse(get_current_weather("Chicago"))
        assert "error" in data

    def test_response_never_raises(self):
        # Should return error JSON, not raise
        try:
            result = get_current_weather("")
            parse(result)  # must still be valid JSON
        except Exception as exc:
            pytest.fail(f"Tool raised an exception instead of returning error JSON: {exc}")


# ============================================================
# TestGetForecastTool
# ============================================================

class TestGetForecastTool:

    def test_returns_string(self):
        assert isinstance(get_forecast("London"), str)

    def test_returns_valid_json(self):
        parse(get_forecast("London"))

    def test_default_5_days(self):
        data = parse(get_forecast("Paris"))
        assert "error" not in data
        assert data.get("days") == 5

    def test_custom_3_days(self):
        data = parse(get_forecast("Tokyo", days=3))
        assert "error" not in data
        assert data.get("days") == 3

    def test_days_zero_returns_error_json(self):
        """days=0 is out of range; tool should return error JSON."""
        data = parse(get_forecast("Chicago", days=0))
        assert "error" in data

    def test_days_17_returns_error_json(self):
        data = parse(get_forecast("Chicago", days=17))
        assert "error" in data

    def test_days_1_valid(self):
        data = parse(get_forecast("Denver", days=1))
        assert "error" not in data
        assert data.get("days") == 1

    def test_days_16_valid(self):
        data = parse(get_forecast("Miami", days=16))
        assert "error" not in data
        assert data.get("days") == 16

    def test_invalid_location(self):
        data = parse(get_forecast("ZZZInvalidCityXYZ_NOPE", days=3))
        assert "error" in data

    def test_broker_exception_returns_error_json(self):
        with patch.object(broker, "get_forecast", side_effect=RuntimeError("API down")):
            data = parse(get_forecast("Chicago", days=5))
        assert "error" in data


# ============================================================
# TestPredictWeatherRecommendationTool
# ============================================================

class TestPredictWeatherRecommendationTool:

    def _future(self, days: int = 2) -> str:
        return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")

    def test_returns_string(self):
        assert isinstance(predict_weather_recommendation("Denver"), str)

    def test_returns_valid_json(self):
        parse(predict_weather_recommendation("Denver"))

    def test_today_required_fields(self):
        data = parse(predict_weather_recommendation("Austin, TX"))
        assert "error" not in data
        for key in ("recommendation", "reasoning", "confidence", "details"):
            assert key in data

    def test_details_fields(self):
        data = parse(predict_weather_recommendation("Chicago"))
        assert "error" not in data
        d = data["details"]
        for key in ("bring_umbrella", "bring_jacket", "good_for_outdoors"):
            assert key in d

    def test_with_future_date(self):
        data = parse(predict_weather_recommendation("Chicago", date=self._future(3)))
        assert "error" not in data
        assert data["date"] == self._future(3)

    def test_invalid_location_returns_error(self):
        data = parse(predict_weather_recommendation("ZZZInvalidCityXYZ_NOPE"))
        assert "error" in data

    def test_past_date_returns_error(self):
        data = parse(predict_weather_recommendation("Chicago", date="2020-01-01"))
        assert "error" in data

    def test_broker_exception_returns_error_json(self):
        with patch.object(broker, "predict_recommendation", side_effect=RuntimeError("boom")):
            data = parse(predict_weather_recommendation("Chicago"))
        assert "error" in data

    def test_response_never_raises(self):
        try:
            result = predict_weather_recommendation("")
            parse(result)
        except Exception as exc:
            pytest.fail(f"Tool raised instead of returning error JSON: {exc}")
