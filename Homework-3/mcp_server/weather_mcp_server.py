"""Weather Prediction MCP Server

FastMCP 2.x server exposing three weather intelligence tools for use
with Databricks Agent Bricks (AI Playground MCP integration).

Tools:
    get_current_weather           — current conditions
    get_forecast                  — multi-day forecast
    predict_weather_recommendation — smart recommendations via business logic

MLflow Tracing: each tool call is wrapped in a root span so every
Agent Bricks invocation is fully observable in the MLflow Tracking UI.

Start server:
    python weather_mcp_server.py

Default port: 8000 (override with PORT or APP_PORT env var).
"""

import json
import logging
import os
import sys
from typing import Optional

import mlflow
from fastmcp import FastMCP

# Ensure mcp_server/ is on path when invoked directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from weather_broker import OpenMeteoWeatherBroker  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MLflow experiment
# ---------------------------------------------------------------------------
_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "/Shared/weather-mcp-tracing")
try:
    mlflow.set_experiment(_EXPERIMENT_NAME)
    logger.info("MLflow experiment: %s", _EXPERIMENT_NAME)
except Exception as exc:  # pragma: no cover
    logger.warning("MLflow setup skipped: %s", exc)

# ---------------------------------------------------------------------------
# Broker + FastMCP server
# ---------------------------------------------------------------------------
broker: OpenMeteoWeatherBroker = OpenMeteoWeatherBroker()

mcp = FastMCP(
    name="weather-prediction-server",
    instructions=(
        "Weather intelligence tools providing current conditions, multi-day forecasts, "
        "and smart recommendations. All data sourced from Open-Meteo (no API key needed)."
    ),
)

logger.info("FastMCP server initialised: weather-prediction-server")


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def format_response(data: dict) -> str:
    """Serialise a successful result dict to an indented JSON string."""
    return json.dumps(data, indent=2, default=str)


def format_error(message: str) -> str:
    """Serialise an error message to a JSON string."""
    return json.dumps({"error": message}, indent=2)


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def get_current_weather(location: str) -> str:
    """Get current weather conditions for a location.

    Fetches real-time temperature, humidity, wind speed and conditions
    from the Open-Meteo API. No API key required.

    Args:
        location (str): City name ("Chicago"), city+state ("Chicago, IL"),
                        or coordinates ("41.88,-87.63").

    Returns:
        str: JSON string with fields:
             location, temperature (°F), temperature_unit, humidity (%),
             conditions, wind_speed (mph), wind_direction, timestamp (ISO 8601).
             Returns {"error": "..."} if location cannot be found.

    Example:
        get_current_weather("Chicago, IL")
        get_current_weather("51.5,-0.12")
    """
    with mlflow.start_span(name="tool.get_current_weather", span_type="TOOL") as span:
        span.set_inputs({"location": location})
        try:
            result = broker.get_current_weather(location)
            span.set_outputs({"success": "error" not in result})
            logger.info("Tool get_current_weather('%s') → success=%s", location, "error" not in result)
            return format_response(result)
        except Exception as exc:
            logger.error("Unhandled error in get_current_weather: %s", exc)
            span.set_outputs({"success": False, "error": str(exc)})
            return format_error(str(exc))


@mcp.tool()
def get_forecast(location: str, days: int = 5) -> str:
    """Get a multi-day weather forecast for a location.

    Returns daily high/low temperatures, precipitation chances, and
    conditions for the requested number of days.

    Args:
        location (str): City name, city+state, or "latitude,longitude".
        days (int):     Number of forecast days, 1–16 inclusive. Default 5.
                        Values outside this range return an error.

    Returns:
        str: JSON string with fields:
             location (str), days (int), forecast (list of daily objects):
               date (YYYY-MM-DD), high_temp (°F), low_temp (°F),
               conditions (str), precipitation_chance (%), precipitation_mm (mm),
               wind_speed (mph).
             Returns {"error": "..."} on invalid input or API failure.

    Example:
        get_forecast("New York", days=7)
        get_forecast("Austin, TX")          # defaults to 5 days
    """
    with mlflow.start_span(name="tool.get_forecast", span_type="TOOL") as span:
        span.set_inputs({"location": location, "days": days})
        try:
            if not isinstance(days, int) or days < 1 or days > 16:
                msg = f"'days' must be an integer between 1 and 16 (received {days!r})"
                span.set_outputs({"success": False})
                return format_error(msg)
            result = broker.get_forecast(location, days)
            span.set_outputs({"success": "error" not in result, "days": result.get("days", 0)})
            logger.info(
                "Tool get_forecast('%s', days=%d) → success=%s",
                location, days, "error" not in result,
            )
            return format_response(result)
        except Exception as exc:
            logger.error("Unhandled error in get_forecast: %s", exc)
            span.set_outputs({"success": False, "error": str(exc)})
            return format_error(str(exc))


@mcp.tool()
def predict_weather_recommendation(location: str, date: Optional[str] = None) -> str:
    """Get a smart weather recommendation for a location and date.

    Applies business logic to forecast data. Does NOT echo raw API output.

    Decision thresholds:
        Bring umbrella  : precipitation chance > 40 %
        Wear a jacket   : average temperature < 60 °F
        Good for outdoors: precipitation < 30 % AND temperature 50–85 °F

    Args:
        location (str): City name, city+state, or "latitude,longitude".
        date (str):     Target date in YYYY-MM-DD format.
                        Defaults to today if omitted.
                        Must be within the next 16 days.

    Returns:
        str: JSON string with fields:
             location, date, recommendation (str), reasoning (str),
             confidence (float 0.0–1.0 — decreases 5 % per day out),
             details: bring_umbrella, bring_jacket, good_for_outdoors,
                      high_temp_f, low_temp_f, precipitation_chance_pct, conditions.
             Returns {"error": "..."} for past dates or invalid inputs.

    Example:
        predict_weather_recommendation("Chicago, IL")
        predict_weather_recommendation("Austin, TX", date="2026-08-15")
    """
    with mlflow.start_span(name="tool.predict_weather_recommendation", span_type="TOOL") as span:
        span.set_inputs({"location": location, "date": date})
        try:
            result = broker.predict_recommendation(location, date)
            span.set_outputs(
                {
                    "success": "error" not in result,
                    "recommendation": result.get("recommendation", ""),
                }
            )
            logger.info(
                "Tool predict_weather_recommendation('%s', date=%s) → success=%s",
                location, date, "error" not in result,
            )
            return format_response(result)
        except Exception as exc:
            logger.error("Unhandled error in predict_weather_recommendation: %s", exc)
            span.set_outputs({"success": False, "error": str(exc)})
            return format_error(str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("APP_PORT", "8000")))
    logger.info("Starting Weather MCP server on 0.0.0.0:%d", port)
    logger.info("Tools: get_current_weather | get_forecast | predict_weather_recommendation")
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
