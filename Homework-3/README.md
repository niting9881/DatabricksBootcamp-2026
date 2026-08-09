# Weather Prediction MCP Server + Databricks Agent

**Databricks Bootcamp 2026 — Homework 3**

An end-to-end weather intelligence system built on FastMCP 2.x, deployed as a
Databricks App, and integrated with Databricks Agent Bricks (AI Playground).
Full LLMOps observability via MLflow Tracing.

---

## 1. Overview

This project implements the MCP Server + Agent Bricks pattern from Day 3
(`databricks-lakebase-app-day-3`). A FastMCP server exposes three weather tools
that an Agent Bricks agent uses to answer natural language weather questions:

- "Will it rain in Chicago tomorrow?"
- "Should I bring a jacket to Austin this weekend?"
- "Which city has better weather: Denver or Miami right now?"

All data is sourced from the Open-Meteo API (free, no key required). Every tool
call is traced in MLflow for full LLMOps observability.

### Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full diagram.

```
User → Agent Bricks → FastMCP Server → Adapter Module → Open-Meteo API
                             ↓
                       MLflow Tracing (/Shared/weather-mcp-tracing)
```

---

## 2. Weather API Selection

**Chosen: Open-Meteo** (`https://api.open-meteo.com`)

| Criterion | Open-Meteo | NWS | WeatherAPI.com |
|---|---|---|---|
| API key | Not required | Not required | Required (signup) |
| Coverage | Global | US only | Global |
| Forecast range | 16 days | 7 days | 14 days |
| Rate limits | 10,000+ calls/day | Generous | 100k/month (free) |
| Historical data | Yes (ERA5) | Limited | Yes |

**Rationale:** Open-Meteo requires zero setup (no API key, no account), has
global coverage, supports a 16-day forecast, and has generous free rate limits.
Temperatures are requested in Fahrenheit via the `temperature_unit=fahrenheit`
API parameter.

---

## 3. Tools Documentation

### Tool 1: `get_current_weather`

| Field | Detail |
|---|---|
| Signature | `get_current_weather(location: str) -> str` |
| Returns | JSON: location, temperature (°F), humidity (%), conditions, wind_speed (mph), wind_direction, timestamp |
| Error | `{"error": "..."}` if location not found |

**Example:**
```
get_current_weather("Chicago, IL")
→ {"location": "Chicago, Illinois", "temperature": 78.2, "humidity": 62, "conditions": "Partly cloudy", ...}
```

### Tool 2: `get_forecast`

| Field | Detail |
|---|---|
| Signature | `get_forecast(location: str, days: int = 5) -> str` |
| days range | 1–16 (out-of-range returns error JSON) |
| Returns | JSON: location, days, forecast[] (date, high_temp, low_temp, conditions, precipitation_chance, precipitation_mm, wind_speed) |

**Example:**
```
get_forecast("Austin, TX", days=7)
→ {"location": "Austin, Texas", "days": 7, "forecast": [{"date": "2026-08-08", "high_temp": 98.6, ...}]}
```

### Tool 3: `predict_weather_recommendation`

| Field | Detail |
|---|---|
| Signature | `predict_weather_recommendation(location: str, date: str = None) -> str` |
| date format | YYYY-MM-DD (defaults to today; max 16 days ahead) |
| Returns | JSON: location, date, recommendation (str), reasoning (str), confidence (0.0–1.0), details (flags + raw values) |

**Business Logic Thresholds:**
- **Bring umbrella**: precipitation_chance > 40%
- **Wear jacket**: avg temperature < 60°F
- **Good for outdoors**: precipitation < 30% AND 50°F ≤ avg_temp ≤ 85°F

**Example:**
```
predict_weather_recommendation("Chicago, IL")
→ {"recommendation": "Bring an umbrella (65% precipitation chance). Consider indoor plans.",
   "reasoning": "Forecast for 2026-08-09: High 74°F / Low 68°F, Moderate rain, 65% precip...",
   "confidence": 1.0, "details": {"bring_umbrella": true, ...}}
```

---

## 4. Agent Bricks System Prompt

Use this system prompt when registering the agent in Databricks AI Playground:

```
You are a helpful weather assistant. Your job is to answer natural language
questions about weather and provide actionable recommendations backed by
real forecast data.

You have access to three tools:
1. get_current_weather(location) — Get current conditions
2. get_forecast(location, days) — Get multi-day forecast (1-16 days)
3. predict_weather_recommendation(location, date) — Smart recommendation

Guidelines:
- ALWAYS use tools to get real weather data. NEVER fabricate weather information.
- Be specific with locations: prefer "Chicago, IL" over "Chicago".
- Include concrete numbers in responses: temperatures, percentages, wind speed.
- If a tool returns an error, explain the issue and ask for clarification.
- For multi-city comparisons, call get_current_weather for each city.
- If a location name is ambiguous (e.g. "Springfield"), ask for the state/country.

Example flow for "Should I bring an umbrella to Chicago this weekend?":
1. get_forecast("Chicago, IL", days=3) — check precipitation
2. predict_weather_recommendation("Chicago, IL", date="<Saturday date>") — get recommendation
3. Synthesize: mention exact precipitation chance, temperature, and final advice.

Scope: Only answer weather-related questions. Politely redirect off-topic queries.
```

---

## 5. Setup & Deployment

### Prerequisites

```bash
python --version  # 3.10+
pip install -r mcp_server/requirements.txt
```

### Local Testing

```bash
# From project root
cd mcp_server
python weather_mcp_server.py
# Server starts on http://0.0.0.0:8000
```

### Run Unit Tests

```bash
# From project root
pip install pytest
pytest tests/ -v
```

### Deploy to Databricks Apps

```bash
# Upload files to Databricks workspace
databricks workspace import-dir mcp_server /Workspace/Apps/weather-mcp --overwrite

# Deploy the app
databricks apps deploy weather-prediction-mcp-server \
  --source-code-path /Workspace/Apps/weather-mcp

# Check status
databricks apps get weather-prediction-mcp-server

# View logs
databricks apps logs weather-prediction-mcp-server
```

### Register with Agent Bricks

1. Open Databricks AI Playground
2. Add External Tool → MCP Server
3. Enter the deployed app URL (e.g. `https://<workspace>/apps/weather-prediction-mcp-server`)
4. All 3 tools will appear automatically
5. Paste the system prompt from Section 4 above
6. Save as agent "weather-assistant"

---

## 6. Testing Examples

### Query 1: Current Weather

**Query:** "What's the weather in New York City today?"

**Tools called:** `get_current_weather("New York City")`

**Expected response:**
> It's currently 76°F in New York, partly cloudy with 58% humidity and 12 mph winds from the SW.

---

### Query 2: Umbrella Recommendation

**Query:** "Should I bring an umbrella to Chicago this weekend?"

**Tools called:**
1. `get_forecast("Chicago, IL", days=3)`
2. `predict_weather_recommendation("Chicago, IL", date="<Saturday>")`

**Expected response:**
> Yes, Saturday shows a 65% chance of rain with temperatures around 74°F. I'd bring an umbrella and maybe a light layer. Sunday looks clearer at only 15% precipitation.

---

### Query 3: Multi-City Comparison

**Query:** "Which city has better weather: Denver or Miami right now?"

**Tools called:**
1. `get_current_weather("Denver, CO")`
2. `get_current_weather("Miami, FL")`

**Expected response:**
> Denver is currently 82°F with clear skies and low humidity (35%), while Miami is 90°F with 78% humidity and thunderstorm activity. Denver has significantly more comfortable conditions right now.

---

## 7. Known Limitations

- **Forecast accuracy**: Open-Meteo accuracy degrades beyond ~7 days; confidence
  score reflects this (decreases 5% per day, floor at 50%).
- **No severe weather alerts**: Open-Meteo free tier does not include NOAA/NWS
  severe weather alert data.
- **Temperature unit**: All temperatures are in Fahrenheit; international users
  may prefer Celsius (configurable via API parameter).
- **Location ambiguity**: The geocoder picks the most populated match; e.g.,
  "Springfield" returns Springfield, IL. Use city+state for precision.
- **Rate limits**: 10,000 free daily calls shared across all broker instances.
  Production deployments should add caching.

---

## 8. Future Improvements

- **Caching layer**: Redis/Databricks cache for geocoding results (locations
  don't change) and recent weather (cache 10 minutes).
- **Stretch tools**: UV index, air quality index, severe weather alerts, hourly
  forecast, historical weather lookup.
- **Multi-language support**: Pass `language` param to Open-Meteo geocoding.
- **Celsius mode**: Add `units` parameter to all tools.
- **MLflow evaluation notebook**: Automated quality scoring of agent responses
  against a golden test set using MLflow Evaluate.
- **CI/CD**: GitHub Actions pipeline running `pytest tests/` on every push.

---

## Project Structure

```
Homework-3/
├── mcp_server/
│   ├── __init__.py
│   ├── weather_broker.py        # Open-Meteo adapter with MLflow tracing
│   ├── weather_mcp_server.py    # FastMCP 2.x server (3 tools)
│   ├── requirements.txt
│   └── app.yaml                 # Databricks Apps config
├── tests/
│   ├── __init__.py
│   ├── test_broker.py           # 25+ unit tests
│   └── test_mcp_server.py       # 20+ unit tests
├── .gitignore
├── README.md                    # This file
└── ARCHITECTURE.md              # System diagram + design decisions
```
