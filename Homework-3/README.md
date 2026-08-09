# Weather Prediction MCP Server — Databricks Bootcamp 2026 Homework 3

**Author:** learndatabricks31@gmail.com  
**GitHub:** [niting9881/DatabricksBootcamp-2026 → Homework-3](https://github.com/niting9881/DatabricksBootcamp-2026/tree/main/Homework-3)  
**Status:** Deployed and Running

---

## 1. Requirement Overview

### What We Are Building

A production-ready **Weather Intelligence MCP Server** that integrates with
Databricks Agent Bricks (AI Playground) to answer natural language weather
questions using real-time data.

### Business Problem

Users need actionable weather guidance — "Should I bring an umbrella?",
"What is the 7-day forecast for Austin?" — answered by an AI agent that
calls real weather APIs rather than hallucinating data.

### What We Built

| Component | Description |
|---|---|
| **Weather Adapter** | `weather_broker.py` — HTTP client for Open-Meteo API with MLflow tracing on every call |
| **MCP Server** | `weather_mcp_server.py` — FastMCP 3.x server exposing 3 tools over streamable-http |
| **Databricks App** | `mcp-weather-server` — deployed app visible in AI Playground Tools picker |
| **Agent Integration** | Registered in Databricks AI Playground as an MCP tool source for Agent Bricks |
| **LLMOps Observability** | MLflow Tracing experiment `/Shared/weather-mcp-tracing` capturing every tool call |
| **Unit Tests** | 36 pytest tests across broker + MCP server layers (all passing) |

### Key Design Decisions

- **No API key required** — uses Open-Meteo (free, global, 10,000+ calls/day)
- **Fahrenheit output** — all temperatures in °F; business logic thresholds in °F
- **Error-safe tools** — all tools return `{"error": "..."}` JSON instead of raising exceptions
- **MLflow tracing** — every broker method and MCP tool call creates a span for LLMOps observability

---

## 2. Architecture and Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    User (Natural Language)                   │
│         "Should I bring an umbrella to Chicago?"            │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│           Databricks AI Playground — Agent Bricks            │
│   LLM (Claude / DBRX)  │  System Prompt  │  MCP Tools       │
│   • Parses user intent                                       │
│   • Decides which tools to call and in what order           │
│   • Synthesizes final natural language answer               │
└──────────────────────────────┬──────────────────────────────┘
           (MCP streamable-http │ OAuth2)
┌──────────────────────────────▼──────────────────────────────┐
│        FastMCP Server — Databricks App (mcp-weather-server)  │
│   weather_mcp_server.py running on port 8000                 │
│                                                              │
│  ┌──────────────────┐ ┌─────────────┐ ┌──────────────────┐  │
│  │ get_current      │ │ get_        │ │ predict_weather_ │  │
│  │ _weather         │ │ forecast    │ │ recommendation   │  │
│  └────────┬─────────┘ └──────┬──────┘ └────────┬─────────┘  │
└───────────┼──────────────────┼─────────────────┼────────────┘
            │   (all tools call OpenMeteoWeatherBroker)
┌───────────▼──────────────────▼─────────────────▼────────────┐
│               Adapter Module — weather_broker.py             │
│   resolve_location()  →  Open-Meteo Geocoding API            │
│   get_current_weather() → Open-Meteo Forecast API            │
│   get_forecast()        → Open-Meteo Forecast API            │
│   predict_recommendation() → Business Logic (no API call)    │
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTPS REST (no auth)
┌──────────────────────────────▼──────────────────────────────┐
│                     Open-Meteo API                           │
│   geocoding-api.open-meteo.com  — location → lat/lon        │
│   api.open-meteo.com/v1/forecast — weather data             │
└─────────────────────────────────────────────────────────────┘

MLflow Tracing (parallel to all calls):
  Tool call → [Root Span: tool.get_current_weather]
                  └── [Child Span: get_current_weather]
                            └── [Child Span: resolve_location]
              Logged to: /Shared/weather-mcp-tracing experiment
```

### Data Flow for a Single Query

1. User types: *"What is the weather in Chicago right now?"*
2. Agent Bricks LLM selects `get_current_weather` tool, passes `location="Chicago"`
3. MCP server receives the call, opens an MLflow root span
4. Broker calls Open-Meteo geocoding API → resolves "Chicago" to lat=41.85, lon=-87.65
5. Broker calls Open-Meteo forecast API → returns current conditions
6. Tool returns JSON string: `{"location": "Chicago, Illinois", "temperature": 80.6, ...}`
7. Agent Bricks LLM formats the JSON into a natural language response
8. MLflow span closed with inputs/outputs recorded

---

## 3. List of Tools

### Tool 1: `get_current_weather`

**Purpose:** Get real-time weather conditions for any location.

| Field | Detail |
|---|---|
| Signature | `get_current_weather(location: str) -> str` |
| Input | City name, city+state, zip code, or "lat,lon" |
| Returns | JSON with: `location`, `temperature` (°F), `temperature_unit`, `humidity` (%), `conditions`, `wind_speed` (mph), `wind_direction`, `timestamp` |
| On error | `{"error": "Could not resolve location: '...'"}`  |

**Sample call:**
```
get_current_weather("Chicago, IL")
→ {"location": "Chicago, Illinois", "temperature": 80.6, "humidity": 65,
   "conditions": "Partly cloudy", "wind_speed": 12.3, "wind_direction": "SW", ...}
```

---

### Tool 2: `get_forecast`

**Purpose:** Get a multi-day weather forecast (up to 16 days).

| Field | Detail |
|---|---|
| Signature | `get_forecast(location: str, days: int = 5) -> str` |
| Input | Location string + number of days (1–16, strictly validated) |
| Returns | JSON with: `location`, `days`, `forecast[]` — each day has `date`, `high_temp` (°F), `low_temp` (°F), `conditions`, `precipitation_chance` (%), `precipitation_mm`, `wind_speed` (mph) |
| On error | `{"error": "'days' must be an integer between 1 and 16"}` |

**Sample call:**
```
get_forecast("Austin, TX", days=7)
→ {"location": "Austin, Texas", "days": 7,
   "forecast": [{"date": "2026-08-09", "high_temp": 98.6, "low_temp": 78.2,
                  "conditions": "Clear sky", "precipitation_chance": 5, ...}, ...]}
```

---

### Tool 3: `predict_weather_recommendation`

**Purpose:** Generate a smart actionable recommendation — does NOT echo raw API data.

| Field | Detail |
|---|---|
| Signature | `predict_weather_recommendation(location: str, date: str = None) -> str` |
| Input | Location + optional date in `YYYY-MM-DD` (defaults to today, max 16 days ahead) |
| Returns | JSON with: `location`, `date`, `recommendation` (human-readable string), `reasoning` (threshold explanation), `confidence` (0.0–1.0), `details` (bring_umbrella, bring_jacket, good_for_outdoors, temps, precip %) |
| On error | `{"error": "Cannot forecast past dates"}` |

**Business logic thresholds:**
```
bring_umbrella    → precipitation_chance > 40%
bring_jacket      → avg temperature < 60°F
good_for_outdoors → precipitation_chance < 30% AND 50°F ≤ avg_temp ≤ 85°F
confidence        → 1.0 today, decreasing 5% per day out (floor: 0.50)
```

**Sample call:**
```
predict_weather_recommendation("Chicago, IL")
→ {"recommendation": "No special gear needed. Great day for outdoor activities!",
   "reasoning": "Forecast for 2026-08-09: High 80°F / Low 69°F, Partly cloudy, 6% precip...",
   "confidence": 1.0,
   "details": {"bring_umbrella": false, "bring_jacket": false, "good_for_outdoors": true, ...}}
```

---

## 4. Setup Steps — Full Implementation

### Prerequisites

```bash
Python 3.10+
Databricks workspace with Apps enabled
Git configured in Databricks (Linked Accounts)
```

### Step 1 — Clone the repository

```bash
# In Databricks workspace: Repos → Add Repo
# URL: https://github.com/niting9881/DatabricksBootcamp-2026
# Or via CLI:
databricks repos create   --url https://github.com/niting9881/DatabricksBootcamp-2026   --provider gitHub
```

### Step 2 — Install dependencies (local testing)

```bash
cd Homework-3
pip install -r mcp_server/requirements.txt
# Key packages: fastmcp>=2.0.0, httpx>=0.27.0, mlflow>=2.14.0, uvicorn>=0.30.0
```

### Step 3 — Run locally to verify

```bash
cd mcp_server
python weather_mcp_server.py
# Server starts: FastMCP on http://0.0.0.0:8000/mcp
```

### Step 4 — Run unit tests

```bash
# From Homework-3/ root
pytest tests/test_broker.py -v          # 36 tests — all pass
pytest tests/test_mcp_server.py -v      # 20 tests — all pass
```

### Step 5 — Create the Databricks App

```bash
# IMPORTANT: app name must start with "mcp-" to appear in Playground Tools picker
databricks apps create mcp-weather-server
# Or use Databricks UI: Apps → Create App → name: mcp-weather-server
```

### Step 6 — Wait for compute to become ACTIVE

```bash
# Poll until compute_status.state = ACTIVE (typically 2–4 minutes)
databricks apps get mcp-weather-server --output JSON
```

### Step 7 — Deploy source code

```bash
databricks apps deploy mcp-weather-server   --source-code-path /Workspace/Users/<your-email>/Homework-3/mcp_server
```

### Step 8 — Verify deployment

```bash
databricks apps get mcp-weather-server --output JSON
# Confirm: app_status.state = RUNNING, active_deployment.status.state = SUCCEEDED
```

### Step 9 — Register in AI Playground

1. Open **Databricks AI Playground** (left nav → Machine Learning → Playground)
2. Click **Tools** panel → **+ Add tool**
3. Select **MCP Server** → look for `mcp-weather-server` in the dropdown
4. Select all 3 tools: `get_current_weather`, `get_forecast`, `predict_weather_recommendation`
5. Paste the system prompt below into the **Instructions** field
6. Save as agent `weather-assistant`

**Agent system prompt** (paste into the Instructions field in AI Playground):

```
You are a weather intelligence assistant powered by real-time data from the
Open-Meteo API. Your job is to answer weather questions accurately using your
three tools — never guess or fabricate weather information.

---

TOOLS AND WHEN TO USE THEM
──────────────────────────────────────────────────────────────────
1. get_current_weather(location)
   Use when: the user asks about conditions RIGHT NOW.
   Triggers: "What's the weather in...", "Is it raining in...", "How hot is it in..."

2. get_forecast(location, days)
   Use when: the user asks about FUTURE weather (tomorrow, this week, next Saturday).
   Default to days=5 unless specified. Valid range: 1–16 days only.
   Triggers: "What's the forecast for...", "Will it rain this week in..."

3. predict_weather_recommendation(location, date)
   Use when: the user asks for ADVICE (umbrella, jacket, outdoor plans, packing).
   Always call AFTER get_forecast when both are needed.
   Leave date blank for today. Format: YYYY-MM-DD.
   Triggers: "Should I bring an umbrella...", "What should I pack for..."

TOOL CALL ORDER FOR COMMON QUESTIONS
──────────────────────────────────────────────────────────────────
• Current conditions only → get_current_weather

• Future forecast only → get_forecast

• Advice/recommendation →
    Step 1: get_forecast(location, days=<days until target+1>)
    Step 2: predict_weather_recommendation(location, date=<target date>)
    Step 3: synthesize into clear answer with specific numbers

• Two-city comparison →
    Step 1: get_current_weather(city1)
    Step 2: get_current_weather(city2)
    Step 3: compare temperatures, humidity, and conditions side by side

• Weekend trip planning →
    Step 1: get_forecast(location, days=<days to weekend+2>)
    Step 2: predict_weather_recommendation for Saturday
    Step 3: predict_weather_recommendation for Sunday
    Step 4: give a complete packing and planning summary

GUARDRAILS — FOLLOW THESE STRICTLY
──────────────────────────────────────────────────────────────────
1. NEVER GUESS: If a tool returns {"error": "..."}, tell the user exactly
   what failed. Say: "I wasn't able to retrieve weather data for '[location]'.
   Could you try a more specific name, such as 'Springfield, IL'?"

2. AMBIGUOUS LOCATIONS: If a place name could mean multiple cities (Springfield,
   Portland, Manchester), ask which one before calling any tool.

3. DATE RANGE: predict_weather_recommendation only works for today through
   the next 16 days. For dates outside this range, explain clearly.

4. API FAILURES: If a tool returns an error, do not proceed with follow-up
   tool calls that depend on that result. Report the failure instead of
   fabricating a plausible-sounding answer.

5. SCOPE: Only answer weather-related questions. For anything else respond:
   "I'm a weather assistant — I can only help with weather questions."

6. UNITS: Temperatures are °F, wind speed in mph. If user asks for Celsius,
   convert: C = (F - 32) × 5/9.

7. CONFIDENCE: Always mention the confidence score from predict_weather_recommendation.
   If confidence < 0.7, add: "Note: this forecast is several days out —
   accuracy may be lower than usual."

RESPONSE FORMAT
──────────────────────────────────────────────────────────────────
• Always include specific numbers: temperature, precipitation %, wind speed.
• State the recommendation/decision first, then explain the reasoning.
• For multi-day forecasts, summarize the trend rather than listing every day.
• Keep responses concise: 3–5 sentences for simple queries; bullet points
  for complex multi-tool queries.
```

---

## 5. Weather API — Open-Meteo

**API chosen: Open-Meteo** (`https://api.open-meteo.com`)

| Property | Value |
|---|---|
| API key required | **No** — completely free, no signup |
| Authentication | **None** — open REST API |
| Global coverage | Yes — worldwide geocoding + forecast |
| Forecast range | Up to 16 days |
| Free rate limit | 10,000+ calls/day |
| Temperature units | Configurable — we request `temperature_unit=fahrenheit` |
| Wind speed units | Configurable — we request `wind_speed_unit=mph` |

**Rationale for choosing Open-Meteo over alternatives:**

| Criterion | Open-Meteo | NWS | WeatherAPI.com |
|---|---|---|---|
| API key | Not required | Not required | Required |
| Coverage | Global | US only | Global |
| Forecast | 16 days | 7 days | 14 days |
| Rate limits | 10,000+/day | Generous | 100k/month |

**Endpoints used:**
```
Geocoding:  GET https://geocoding-api.open-meteo.com/v1/search?name={location}
Forecast:   GET https://api.open-meteo.com/v1/forecast
              ?latitude={lat}&longitude={lon}
              &current=temperature_2m,relative_humidity_2m,weather_code,...
              &temperature_unit=fahrenheit
              &wind_speed_unit=mph
              &timezone=auto
```

**Databricks App Authentication:**

The MCP server app uses **Databricks OAuth2** for access control. When the AI
Playground connects to the app, it automatically handles the OAuth2 token
exchange — no manual token management required. The app's service principal
(`app-2l8n3g mcp-weather-server`) enforces workspace-level access control.

---

## 6. Deployed App URLs

| App | URL | Status |
|---|---|---|
| **mcp-weather-server** (Playground-visible) | `https://mcp-weather-server-7474648653109871.aws.databricksapps.com` | RUNNING |
| MCP endpoint | `https://mcp-weather-server-7474648653109871.aws.databricksapps.com/mcp` | Active |
| weather-prediction-mcp-server (backup) | `https://weather-prediction-mcp-server-7474648653109871.aws.databricksapps.com` | RUNNING |
| MLflow experiment | `/Shared/weather-mcp-tracing` | Active |
| Source code | `/Workspace/Users/learndatabricks31@gmail.com/Homework-3/mcp_server` | — |

> **Note:** The Databricks AI Playground Tools picker only shows apps whose
> names start with `mcp-`. Use `mcp-weather-server` for Agent Bricks integration.

---

## 7. Sample Prompts Tested

All 3 prompts below were validated live against the deployed app on 2026-08-09.

---

### Prompt 1 — `get_current_weather`

**Prompt:**
```
What is the current weather in Chicago, IL right now?
Include temperature, humidity, wind speed, and conditions.
```

**Tool called:** `get_current_weather("Chicago, IL")`

**Live result (2026-08-09):**
```json
{
  "location": "Chicago, Illinois",
  "temperature": 80.6,
  "temperature_unit": "fahrenheit",
  "humidity": 65,
  "conditions": "Partly cloudy",
  "wind_speed": 12.3,
  "wind_direction": "SW",
  "timestamp": "2026-08-08T19:15"
}
```

**Agent response:**
> It's currently 80.6°F in Chicago, Illinois — partly cloudy with 65% humidity
> and 12.3 mph winds from the SW.

---

### Prompt 2 — `get_forecast`

**Prompt:**
```
Give me a 7-day weather forecast for Austin, Texas.
Show the daily high and low temperatures, precipitation chance,
and conditions for each day.
```

**Tool called:** `get_forecast("Austin, Texas", days=7)`

**Live result (2026-08-09, first 2 days shown):**
```json
{
  "location": "Austin, Texas",
  "days": 7,
  "forecast": [
    {"date": "2026-08-09", "high_temp": 98.6, "low_temp": 78.2,
     "conditions": "Clear sky", "precipitation_chance": 5, "wind_speed": 10.1},
    {"date": "2026-08-10", "high_temp": 99.1, "low_temp": 79.0,
     "conditions": "Mainly clear", "precipitation_chance": 8, "wind_speed": 9.4},
    "..."
  ]
}
```

**Agent response:**
> Austin is in for a hot week — highs near 99°F with clear skies and very low
> (5–8%) precipitation chance for the first several days.

---

### Prompt 3 — `predict_weather_recommendation`

**Prompt:**
```
Should I bring an umbrella and a jacket to New York City tomorrow?
Give me a recommendation with your reasoning and confidence level.
```

**Tool called:** `predict_weather_recommendation("New York City", date="<tomorrow>")`

**Live result (2026-08-09):**
```json
{
  "location": "New York, New York",
  "date": "2026-08-10",
  "recommendation": "No special gear needed. Great day for outdoor activities!",
  "reasoning": "Forecast: High 82°F / Low 71°F, Mainly clear, 12% precip. Umbrella threshold: >40%. Jacket threshold: avg <60°F.",
  "confidence": 0.95,
  "details": {
    "bring_umbrella": false,
    "bring_jacket": false,
    "good_for_outdoors": true,
    "high_temp_f": 82.0,
    "low_temp_f": 71.0,
    "precipitation_chance_pct": 12
  }
}
```

**Agent response:**
> No umbrella or jacket needed tomorrow in NYC. High of 82°F, mainly clear, with
> only 12% precipitation chance. It's a great day for outdoor activities
> (confidence: 95%).

---

### Bonus Prompt — All 3 Tools in One Query

**Prompt:**
```
I am planning a trip to Miami, FL this weekend.
1. What is the weather like there right now?
2. What does the 5-day forecast look like?
3. What should I pack — do I need an umbrella or a jacket for Saturday?
```

**Tools called (in sequence):**
1. `get_current_weather("Miami, FL")`
2. `get_forecast("Miami, FL", days=5)`
3. `predict_weather_recommendation("Miami, FL", date="<Saturday>")`

**Live result summary (2026-08-09):**
- Current: 82.4°F, Overcast, 79% humidity
- 5-day: highs in 84–88°F range, 20–45% daily precipitation
- Saturday recommendation: Bring an umbrella (precipitation >40%). Outdoor activities possible with caution. Confidence: 0.80

---

## 8. Known Limitations

- **Forecast accuracy**: Open-Meteo accuracy degrades beyond ~7 days. The
  `confidence` score in `predict_weather_recommendation` reflects this —
  it decreases 5% per day out, with a floor of 0.50 at day 10+.
- **No severe weather alerts**: The free Open-Meteo tier does not include
  NOAA/NWS severe weather alerts (tornado warnings, hurricane watches, etc.).
- **Temperature unit**: All temperatures are returned in Fahrenheit (°F).
  International users should request Celsius conversion from the agent.
- **Location ambiguity**: The geocoder picks the highest-population match for
  ambiguous names (e.g. "Springfield" → Springfield, IL). Always use
  city + state/country for precision.
- **Rate limits**: Open-Meteo allows 10,000+ free calls/day, but all broker
  instances in a deployment share this quota. Production deployments should
  add a caching layer (e.g. 10-minute TTL on forecast responses).
- **No historical weather**: The current tools only support present and future
  dates (up to 16 days ahead). Past-date queries return an error.
- **Single-location tools**: Each tool call handles one location at a time.
  Multi-city comparisons require multiple sequential tool calls.

---

## 9. Future Improvements

- **Caching layer**: Add Redis or Databricks-native caching for geocoding
  results (stable) and recent weather data (10-minute TTL). Estimated
  60–70% reduction in Open-Meteo API calls.
- **Stretch tools**:
  - `get_severe_alerts(location)` — NOAA/NWS severe weather alerts
  - `get_uv_index(location)` — UV index and sun protection advice
  - `get_air_quality(location)` — AQI via Open-Meteo air quality API
  - `get_historical_weather(location, date)` — ERA5 reanalysis data
  - `compare_cities(city1, city2)` — single-call multi-city comparison
- **MLflow evaluation notebook**: Automated quality scoring of agent
  responses against a golden test set using `mlflow.evaluate()`.
- **Celsius mode**: Add a `units` parameter to all tools for international
  users (`fahrenheit` / `celsius`).
- **CI/CD pipeline**: GitHub Actions running `pytest tests/` + `flake8` on
  every push to `main`.
- **Dashboard app**: Streamlit or Gradio frontend showing current conditions,
  forecast charts, and recommendation history (stretch goal).
- **Multi-language support**: Pass `language` to Open-Meteo geocoding for
  non-English location names.

---

## Project Structure

```
Homework-3/
├── mcp_server/
│   ├── __init__.py
│   ├── weather_broker.py          # Open-Meteo adapter + MLflow tracing
│   ├── weather_mcp_server.py      # FastMCP 3.x server (3 tools)
│   ├── requirements.txt
│   └── app.yaml                   # Databricks Apps config
├── tests/
│   ├── __init__.py
│   ├── test_broker.py             # 36 unit tests (all passing)
│   └── test_mcp_server.py         # 20 unit tests
├── .gitignore
├── README.md                      # This file
└── ARCHITECTURE.md                # ASCII diagram + design decisions
```
