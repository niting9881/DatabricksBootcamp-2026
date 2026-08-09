"""Weather API Adapter Module

Provides weather data via Open-Meteo API (free, no API key, global coverage).
All broker methods are instrumented with MLflow Tracing spans for full
LLMOps observability (as per portfolio best-practices).

Temperatures returned in Fahrenheit. Wind speed in mph.

API reference: https://open-meteo.com/en/docs
"""

import logging
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import mlflow

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# MLflow experiment (graceful fallback if tracking server is unavailable)
_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "/Shared/weather-mcp-tracing")
try:
    mlflow.set_experiment(_EXPERIMENT_NAME)
    logger.info("MLflow experiment set: %s", _EXPERIMENT_NAME)
except Exception as exc:  # pragma: no cover
    logger.warning("MLflow experiment setup skipped: %s", exc)


# ---------------------------------------------------------------------------
# No-op span for graceful MLflow fallback in non-Databricks environments
# ---------------------------------------------------------------------------
class _NoOpSpan:
    """Silent substitute when MLflow tracing is unavailable."""

    def set_inputs(self, *_a, **_kw) -> None:  # noqa: D401
        pass

    def set_outputs(self, *_a, **_kw) -> None:
        pass

    def set_attributes(self, *_a, **_kw) -> None:
        pass


@contextmanager
def _safe_span(name: str, span_type: str = "RETRIEVAL"):
    """Context manager that yields an MLflow span, falling back to _NoOpSpan."""
    try:
        with mlflow.start_span(name=name, span_type=span_type) as span:
            yield span
    except Exception:  # pragma: no cover
        yield _NoOpSpan()


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------
class WeatherBroker(ABC):
    """Abstract base class for weather API adapters."""

    @abstractmethod
    def get_current_weather(self, location: str) -> Dict[str, Any]:
        """Return current weather conditions for *location*."""

    @abstractmethod
    def get_forecast(self, location: str, days: int = 5) -> Dict[str, Any]:
        """Return a multi-day weather forecast for *location*."""

    @abstractmethod
    def predict_recommendation(
        self, location: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return a business-logic-derived weather recommendation."""


# ---------------------------------------------------------------------------
# Open-Meteo implementation
# ---------------------------------------------------------------------------
class OpenMeteoWeatherBroker(WeatherBroker):
    """Weather adapter using the Open-Meteo API.

    Rationale for Open-Meteo:
    * No API key or signup required
    * Global coverage
    * 10,000+ free daily calls
    * Supports current conditions + 16-day forecast
    """

    GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

    # WMO weather interpretation codes → human-readable strings
    WMO_CODES: Dict[int, str] = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Slight showers",
        81: "Moderate showers",
        82: "Violent showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with hail",
        99: "Thunderstorm with heavy hail",
    }

    WIND_DIRS = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    ]

    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout
        self.client = httpx.Client(timeout=timeout)
        logger.info("OpenMeteoWeatherBroker initialised (timeout=%.1fs)", timeout)

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.client.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ helpers

    def _decode_wmo(self, code: int) -> str:
        return self.WMO_CODES.get(int(code), f"Unknown (code {code})")

    def _decode_wind_dir(self, degrees: Optional[float]) -> str:
        if degrees is None:
            return "Unknown"
        return self.WIND_DIRS[round(float(degrees) / 22.5) % 16]

    def _location_str(self, coords: Dict[str, Any]) -> str:
        """Build a readable location string from resolved coordinates dict."""
        name = coords.get("name", "")
        suffix = coords.get("admin1") or coords.get("country", "")
        return f"{name}, {suffix}" if suffix else name

    # -------------------------------------------------------------- public API

    def resolve_location(self, location: str) -> Optional[Dict[str, Any]]:
        """Convert a location string to lat/lon via Open-Meteo geocoding.

        Args:
            location: City name ("Chicago"), city+state ("Chicago, IL"),
                      or "latitude,longitude" string ("41.88,-87.63").

        Returns:
            dict with keys latitude, longitude, name, country, admin1
            or None if the location cannot be resolved.

        Examples:
            >>> broker.resolve_location("Chicago, IL")
            {"latitude": 41.85, "longitude": -87.65, "name": "Chicago", ...}
            >>> broker.resolve_location("41.88,-87.63")
            {"latitude": 41.88, "longitude": -87.63, ...}
        """
        with _safe_span("resolve_location", "RETRIEVAL") as span:
            span.set_inputs({"location": location})
            try:
                # ── Try direct lat/lon ──────────────────────────────────────
                if "," in location:
                    parts = location.split(",", 1)
                    try:
                        lat, lon = float(parts[0].strip()), float(parts[1].strip())
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            result = {
                                "latitude": lat,
                                "longitude": lon,
                                "name": f"{lat},{lon}",
                                "country": "N/A",
                                "admin1": "",
                            }
                            span.set_outputs({"resolved": True, "method": "direct_coords"})
                            return result
                    except ValueError:
                        pass  # fall through to geocoding

                # ── Geocode via Open-Meteo ──────────────────────────────────
                resp = self.client.get(
                    self.GEOCODING_URL,
                    params={"name": location, "count": 1, "language": "en", "format": "json"},
                )
                resp.raise_for_status()
                data = resp.json()

                if data.get("results"):
                    r = data["results"][0]
                    result = {
                        "latitude": r["latitude"],
                        "longitude": r["longitude"],
                        "name": r.get("name", location),
                        "country": r.get("country", ""),
                        "admin1": r.get("admin1", ""),
                    }
                    span.set_outputs(
                        {"resolved": True, "method": "geocoding", "name": result["name"]}
                    )
                    logger.info("Resolved '%s' → %s", location, self._location_str(result))
                    return result

                logger.warning("Could not resolve location: '%s'", location)
                span.set_outputs({"resolved": False})
                return None

            except Exception as exc:
                logger.error("Error resolving location '%s': %s", location, exc)
                span.set_outputs({"resolved": False, "error": str(exc)})
                return None

    def get_current_weather(self, location: str) -> Dict[str, Any]:
        """Get current weather conditions for a location.

        Args:
            location: City name, city+state, zip code, or "latitude,longitude".

        Returns:
            dict with:
                location (str), temperature (float °F), temperature_unit (str),
                humidity (int %), conditions (str), wind_speed (float mph),
                wind_direction (str), timestamp (ISO 8601).
            On failure: {"error": "<message>"}.

        Examples:
            >>> broker.get_current_weather("Chicago, IL")
            {"location": "Chicago, Illinois", "temperature": 78.2, ...}
        """
        with _safe_span("get_current_weather", "RETRIEVAL") as span:
            span.set_inputs({"location": location})
            try:
                coords = self.resolve_location(location)
                if not coords:
                    err = {"error": f"Could not resolve location: '{location}'"}
                    span.set_outputs(err)
                    return err

                resp = self.client.get(
                    self.FORECAST_URL,
                    params={
                        "latitude": coords["latitude"],
                        "longitude": coords["longitude"],
                        "current": (
                            "temperature_2m,relative_humidity_2m,"
                            "weather_code,wind_speed_10m,wind_direction_10m"
                        ),
                        "temperature_unit": "fahrenheit",
                        "wind_speed_unit": "mph",
                        "timezone": "auto",
                    },
                )
                resp.raise_for_status()
                c = resp.json().get("current", {})

                result = {
                    "location": self._location_str(coords),
                    "temperature": c.get("temperature_2m"),
                    "temperature_unit": "fahrenheit",
                    "humidity": c.get("relative_humidity_2m"),
                    "conditions": self._decode_wmo(c.get("weather_code", 0)),
                    "wind_speed": c.get("wind_speed_10m"),
                    "wind_direction": self._decode_wind_dir(c.get("wind_direction_10m")),
                    "timestamp": c.get("time"),
                }
                span.set_outputs(
                    {
                        "location": result["location"],
                        "temperature_f": result["temperature"],
                        "conditions": result["conditions"],
                    }
                )
                logger.info(
                    "Current weather for %s: %.1f°F, %s",
                    result["location"],
                    result["temperature"] or 0,
                    result["conditions"],
                )
                return result

            except Exception as exc:
                logger.error("Error fetching current weather for '%s': %s", location, exc)
                err = {"error": str(exc)}
                span.set_outputs(err)
                return err

    def get_forecast(
        self, location: str, days: int = 5
    ) -> Dict[str, Any]:
        """Get a multi-day weather forecast.

        Args:
            location: City name, city+state, zip code, or "latitude,longitude".
            days:     Number of forecast days (1–16). Clamped automatically.

        Returns:
            dict with:
                location (str), days (int), forecast (list of daily dicts):
                    date (YYYY-MM-DD), high_temp (°F), low_temp (°F),
                    conditions (str), precipitation_chance (%), precipitation_mm (mm),
                    wind_speed (mph).
            On failure: {"error": "<message>"}.

        Examples:
            >>> broker.get_forecast("New York", days=7)
        """
        days = max(1, min(16, int(days)))  # clamp to valid range
        with _safe_span("get_forecast", "RETRIEVAL") as span:
            span.set_inputs({"location": location, "days": days})
            try:
                coords = self.resolve_location(location)
                if not coords:
                    err = {"error": f"Could not resolve location: '{location}'"}
                    span.set_outputs(err)
                    return err

                resp = self.client.get(
                    self.FORECAST_URL,
                    params={
                        "latitude": coords["latitude"],
                        "longitude": coords["longitude"],
                        "daily": (
                            "weather_code,temperature_2m_max,temperature_2m_min,"
                            "precipitation_sum,precipitation_probability_max,"
                            "wind_speed_10m_max"
                        ),
                        "temperature_unit": "fahrenheit",
                        "wind_speed_unit": "mph",
                        "timezone": "auto",
                        "forecast_days": days,
                    },
                )
                resp.raise_for_status()
                daily = resp.json().get("daily", {})

                dates = daily.get("time", [])

                def _get(key: str, i: int, default=None):
                    return (daily.get(key) or [default] * len(dates))[i]

                forecast: List[Dict[str, Any]] = [
                    {
                        "date": dates[i],
                        "high_temp": _get("temperature_2m_max", i),
                        "low_temp": _get("temperature_2m_min", i),
                        "conditions": self._decode_wmo(_get("weather_code", i, 0) or 0),
                        "precipitation_chance": _get("precipitation_probability_max", i, 0),
                        "precipitation_mm": _get("precipitation_sum", i, 0.0),
                        "wind_speed": _get("wind_speed_10m_max", i),
                    }
                    for i in range(len(dates))
                ]

                result = {
                    "location": self._location_str(coords),
                    "days": len(forecast),
                    "forecast": forecast,
                }
                span.set_outputs({"location": result["location"], "days_returned": len(forecast)})
                logger.info("Forecast fetched for %s: %d days", result["location"], len(forecast))
                return result

            except Exception as exc:
                logger.error("Error fetching forecast for '%s': %s", location, exc)
                err = {"error": str(exc)}
                span.set_outputs(err)
                return err

    def predict_recommendation(
        self, location: str, date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate a weather recommendation using business logic.

        Applies derived thresholds — does NOT echo raw API data.

        Thresholds:
            * Umbrella     : precipitation_chance > 40 %
            * Jacket       : average temperature < 60 °F
            * Good outdoors: precipitation_chance < 30 % AND 50 °F ≤ avg_temp ≤ 85 °F

        Args:
            location: City name, city+state, zip code, or "latitude,longitude".
            date:     Target date in YYYY-MM-DD format.
                      Defaults to today. Must be within the next 16 days.

        Returns:
            dict with:
                location (str), date (str), recommendation (str),
                reasoning (str), confidence (float 0.0–1.0),
                details (dict: bring_umbrella, bring_jacket, good_for_outdoors,
                               high_temp_f, low_temp_f, precipitation_chance_pct, conditions).
            On failure: {"error": "<message>"}.

        Examples:
            >>> broker.predict_recommendation("Chicago, IL")
            >>> broker.predict_recommendation("Austin, TX", date="2026-08-15")
        """
        with _safe_span("predict_recommendation", "LLM") as span:
            span.set_inputs({"location": location, "date": date})
            try:
                # ── Resolve target date ─────────────────────────────────────
                today = datetime.now().date()

                if date:
                    try:
                        target_dt = datetime.strptime(date, "%Y-%m-%d").date()
                    except ValueError:
                        err = {"error": f"Invalid date format: '{date}'. Use YYYY-MM-DD."}
                        span.set_outputs(err)
                        return err
                else:
                    target_dt = today

                target_date = target_dt.strftime("%Y-%m-%d")
                days_ahead = (target_dt - today).days

                if days_ahead < 0:
                    err = {"error": f"Cannot forecast past dates (requested: {target_date})."}
                    span.set_outputs(err)
                    return err
                if days_ahead > 15:
                    err = {"error": f"Forecast only available up to 16 days ahead (requested: {target_date})."}
                    span.set_outputs(err)
                    return err

                # ── Fetch forecast ──────────────────────────────────────────
                forecast = self.get_forecast(location, days=max(days_ahead + 1, 1))
                if "error" in forecast:
                    span.set_outputs(forecast)
                    return forecast

                # Find the matching day (fall back to first day)
                day_data = next(
                    (d for d in forecast["forecast"] if d["date"] == target_date),
                    forecast["forecast"][0] if forecast["forecast"] else None,
                )

                if not day_data:
                    err = {"error": f"No forecast data available for {target_date}."}
                    span.set_outputs(err)
                    return err

                # ── Apply business logic ────────────────────────────────────
                high_f: float = day_data.get("high_temp") or 70.0
                low_f: float = day_data.get("low_temp") or 60.0
                avg_f = (high_f + low_f) / 2.0
                precip_pct: int = day_data.get("precipitation_chance") or 0
                conditions: str = day_data.get("conditions", "Unknown")

                bring_umbrella = precip_pct > 40
                bring_jacket = avg_f < 60.0
                good_for_outdoors = (precip_pct < 30) and (50.0 <= avg_f <= 85.0)

                # Confidence decreases by 5 % per day ahead (floor 0.50)
                confidence = round(max(0.50, 1.0 - days_ahead * 0.05), 2)

                # Build recommendation text
                advisories = []
                if bring_umbrella:
                    advisories.append(f"bring an umbrella ({precip_pct}% precipitation chance)")
                if bring_jacket:
                    advisories.append(f"wear a jacket (avg {avg_f:.0f}°F)")
                if not advisories:
                    advisories.append("no special gear needed")

                activity_note = (
                    "Great day for outdoor activities!"
                    if good_for_outdoors
                    else (
                        "Consider indoor plans."
                        if precip_pct >= 60
                        else "Outdoor activities possible with caution."
                    )
                )

                # Use cap_first to preserve inner casing (e.g. "°F" not "°f")
                def _cap(s: str) -> str: return s[0].upper() + s[1:] if s else s
                recommendation = ", ".join(_cap(a) for a in advisories) + f". {activity_note}"
                reasoning = (
                    f"Forecast for {target_date}: High {high_f:.0f}°F / Low {low_f:.0f}°F, "
                    f"{conditions}, {precip_pct}% precipitation chance. "
                    "Thresholds — umbrella: precip >40%; jacket: avg temp <60°F; "
                    "outdoors: precip <30% and temp 50–85°F."
                )

                result = {
                    "location": forecast["location"],
                    "date": target_date,
                    "recommendation": recommendation,
                    "reasoning": reasoning,
                    "confidence": confidence,
                    "details": {
                        "bring_umbrella": bring_umbrella,
                        "bring_jacket": bring_jacket,
                        "good_for_outdoors": good_for_outdoors,
                        "high_temp_f": high_f,
                        "low_temp_f": low_f,
                        "precipitation_chance_pct": precip_pct,
                        "conditions": conditions,
                    },
                }
                span.set_outputs(
                    {
                        "recommendation": recommendation,
                        "confidence": confidence,
                        "bring_umbrella": bring_umbrella,
                        "bring_jacket": bring_jacket,
                    }
                )
                logger.info(
                    "Recommendation for %s on %s: %s (confidence %.2f)",
                    location,
                    target_date,
                    recommendation,
                    confidence,
                )
                return result

            except Exception as exc:
                logger.error("Error generating recommendation for '%s': %s", location, exc)
                err = {"error": str(exc)}
                span.set_outputs(err)
                return err
