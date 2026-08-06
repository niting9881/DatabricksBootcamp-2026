"""
weather_client.py — National Weather Service (NWS) API client

Harvests unstructured weather data from api.weather.gov (no API key required).

Data sources:
  - GET /alerts/active?area={state}     Active weather alerts (free-text description + instruction)
  - GET /points/{lat},{lon}             Resolve location to NWS grid
  - GET /gridpoints/{office}/{x},{y}/forecast  Multi-day narrative forecasts

Location formats accepted:
  - "Chicago, IL"    (city, 2-letter state code)
  - "41.88,-87.63"   (lat,lon)
  - "IL"             (state code only — alerts only)

Geocoding: Nominatim / OpenStreetMap (free, no API key)
"""

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

NWS_BASE    = "https://api.weather.gov"
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"

# NWS requires a descriptive User-Agent
_HEADERS = {
    "User-Agent": "DatabricksBootcampWeatherApp/1.0 (learndatabricks31@gmail.com)",
    "Accept": "application/geo+json",
}


# ── HTTP helpers ───────────────────────────────────────────────────

def _get(url: str, params: dict = None, retries: int = 3) -> dict:
    """HTTP GET with exponential-backoff retry and NWS rate-limit handling."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=_HEADERS, params=params, timeout=15)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited by NWS; waiting %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("GET %s failed (attempt %d/%d): %s", url, attempt + 1, retries, exc)
            if attempt < retries - 1:
                time.sleep(1.5 ** attempt)
    return {}


# ── Geocoding ────────────────────────────────────────────────────────────

def _geocode(location: str) -> tuple[float, float]:
    """Convert city/state string to (lat, lon) via Nominatim OSM."""
    resp = requests.get(
        GEOCODE_URL,
        headers={"User-Agent": _HEADERS["User-Agent"]},
        params={"q": location, "format": "json", "limit": 1, "countrycodes": "us"},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise ValueError(f"Location not found via geocoding: {location!r}")
    return float(results[0]["lat"]), float(results[0]["lon"])


def _parse_location(location: str) -> tuple[float, float, str]:
    """
    Parse location string into (lat, lon, state_code).
    Handles: "Chicago, IL" | "41.88,-87.63" | "IL"
    """
    loc = location.strip()

    # Pure lat,lon  e.g. "41.88,-87.63"
    m = re.match(r"^(-?\d+\.?\d*),\s*(-?\d+\.?\d*)$", loc)
    if m:
        return float(m.group(1)), float(m.group(2)), ""

    # Pure 2-letter state code e.g. "IL"
    if re.match(r"^[A-Za-z]{2}$", loc):
        return 0.0, 0.0, loc.upper()  # alerts-only mode

    # "City, ST" — extract state code and geocode
    state_code = ""
    m = re.search(r",\s*([A-Za-z]{2})$", loc)
    if m:
        state_code = m.group(1).upper()

    lat, lon = _geocode(location)
    return lat, lon, state_code


# ── NWS grid resolution ───────────────────────────────────────────────────

def resolve_location_to_grid(location: str) -> dict:
    """
    Resolve a location string to NWS grid metadata.
    Returns dict with keys: office, gridX, gridY, lat, lon, state_code, forecast_url
    Raises ValueError for invalid/non-US locations.
    """
    lat, lon, state_code = _parse_location(location)
    if lat == 0.0 and lon == 0.0:
        # State-code only — no grid available
        return {"office": None, "gridX": None, "gridY": None,
                "lat": None, "lon": None, "state_code": state_code,
                "forecast_url": None}

    data = _get(f"{NWS_BASE}/points/{lat:.4f},{lon:.4f}")
    props = data.get("properties", {})
    if not props:
        raise ValueError(f"NWS /points returned no data for {lat},{lon}")

    rel_loc = props.get("relativeLocation", {}).get("properties", {})
    resolved_state = state_code or rel_loc.get("state", "")

    return {
        "office":       props.get("gridId"),
        "gridX":        props.get("gridX"),
        "gridY":        props.get("gridY"),
        "lat":          lat,
        "lon":          lon,
        "state_code":   resolved_state,
        "city":         rel_loc.get("city", ""),
        "forecast_url": props.get("forecast"),
        "hourly_url":   props.get("forecastHourly"),
    }


# ── ID helpers ──────────────────────────────────────────────────────────────

def _stable_id(raw: str) -> str:
    """32-char hex ID from any string (stable across re-runs)."""
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _make_forecast_id(location: str, updated: str) -> str:
    date_part = (updated or "")[:10]   # YYYY-MM-DD keeps one doc per location per day
    return _stable_id(f"forecast|{location}|{date_part}")


# ── Alerts ───────────────────────────────────────────────────────────────────

def fetch_active_alerts(locations: list[str], limit: int = 50) -> list[dict]:
    """
    Fetch active NWS weather alerts for the given locations.
    Uses state codes from location strings ("/alerts/active?area=STATE").
    Returns list of normalized document dicts.
    """
    documents = []
    seen_ids: set[str] = set()
    fetched = 0

    for location in locations:
        if fetched >= limit:
            break
        try:
            _, _, state_code = _parse_location(location)
            if not state_code:
                logger.warning("No state code extracted from %r; skipping alerts", location)
                continue

            data = _get(f"{NWS_BASE}/alerts/active", params={"area": state_code})
            features = data.get("features", [])
            logger.info("Fetched %d alerts for %s (%s)", len(features), location, state_code)

            for feature in features:
                if fetched >= limit:
                    break
                props = feature.get("properties", {})

                # Build a stable ID from the NWS alert id
                raw_id = feature.get("id") or props.get("id") or ""
                doc_id = _stable_id(raw_id) if raw_id else _stable_id(
                    f"alert|{location}|{props.get('sent', '')}"
                )

                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                # Combine description + instruction for richer narrative
                narrative = " ".join(filter(None, [
                    props.get("description", ""),
                    props.get("instruction", ""),
                ])).strip()

                if not narrative:
                    continue

                documents.append({
                    "id":             doc_id,
                    "location":       location,
                    "source_type":    "alert",
                    "headline":       props.get("event") or props.get("headline") or "Weather Alert",
                    "narrative_text": narrative,
                    "issued_at":      props.get("sent"),
                    "effective_at":   props.get("effective") or props.get("sent"),
                    "expires_at":     props.get("expires"),
                    "payload":        props,
                    "synced_at":      datetime.now(timezone.utc).isoformat(),
                })
                fetched += 1

        except Exception as exc:
            logger.error("Error fetching alerts for %r: %s", location, exc)

    return documents


# ── Forecasts ────────────────────────────────────────────────────────────────

def fetch_forecasts(locations: list[str]) -> list[dict]:
    """
    Fetch multi-day NWS forecast narratives for the given locations.
    Combines all forecast periods into one document per location per day.
    Returns list of normalized document dicts.
    """
    documents = []

    for location in locations:
        try:
            grid = resolve_location_to_grid(location)
            if not grid.get("forecast_url"):
                logger.warning("No forecast URL for %r (lat/lon required); skipping", location)
                continue

            data = _get(grid["forecast_url"])
            props  = data.get("properties", {})
            periods = props.get("periods", [])
            updated = props.get("updated", "")

            if not periods:
                logger.warning("No forecast periods returned for %r", location)
                continue

            # Concatenate all period narratives into one rich document
            narrative_parts = [
                f"{p.get('name', 'Period')}: {p.get('detailedForecast', '')}"
                for p in periods
                if p.get("detailedForecast")
            ]
            narrative = "\n\n".join(narrative_parts)

            if not narrative.strip():
                continue

            doc_id = _make_forecast_id(location, updated)
            documents.append({
                "id":             doc_id,
                "location":       location,
                "source_type":    "forecast",
                "headline":       f"Multi-Day Forecast — {grid.get('city', location)}",
                "narrative_text": narrative,
                "issued_at":      updated or None,
                "effective_at":   periods[0].get("startTime") if periods else None,
                "expires_at":     periods[-1].get("endTime") if periods else None,
                "payload":        {
                    "location":      location,
                    "office":        grid.get("office"),
                    "gridX":         grid.get("gridX"),
                    "gridY":         grid.get("gridY"),
                    "periods_count": len(periods),
                },
                "synced_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("Fetched forecast for %r (%d periods)", location, len(periods))

        except Exception as exc:
            logger.error("Error fetching forecast for %r: %s", location, exc)

    return documents


# ── Main entry point ───────────────────────────────────────────────────────────

def fetch_weather_documents(locations: list[str], limit: int = 50) -> list[dict]:
    """
    Harvest alerts + forecasts for all locations.
    Returns combined list of normalized document dicts, ready for Lakebase upsert.
    """
    per_source = limit // 2 if limit > 1 else limit
    alerts    = fetch_active_alerts(locations, limit=per_source)
    forecasts = fetch_forecasts(locations)
    all_docs  = alerts + forecasts
    logger.info(
        "Harvested %d total documents (%d alerts, %d forecasts) for %d locations",
        len(all_docs), len(alerts), len(forecasts), len(locations),
    )
    return all_docs
