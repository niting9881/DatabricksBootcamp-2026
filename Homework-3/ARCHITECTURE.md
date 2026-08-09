# Architecture: Weather Prediction MCP Server + Agent Bricks

## System Overview

```
┌───────────────────────────────────────────────────┐
│              User / Natural Language Query               │
│     "Will it rain in Chicago tomorrow?"                  │
└────────────────────────┬─────────────────────────┘
                         │
┌────────────────────────┴─────────────────────────┐
│       Databricks AI Playground (Agent Bricks)           │
│   Claude 3.5 Sonnet  │  System Prompt  │  MCP Tools     │
│   - Parses intent                                        │
│   - Selects + calls tools                                │
│   - Synthesizes final answer                             │
└────────────────────────┬─────────────────────────┘
          (MCP streamable-http)│
┌────────────────────────┴─────────────────────────┐
│         FastMCP Server (Databricks App)                  │
│   weather_mcp_server.py                                  │
│                                                          │
│  ┌─────────────┐  ┌────────────┐  ┌────────────────┐ │
│  │ Tool 1       │  │ Tool 2     │  │ Tool 3         │ │
│  │ get_current  │  │ get_       │  │ predict_       │ │
│  │ _weather     │  │ forecast   │  │ weather_       │ │
│  │              │  │            │  │ recommendation │ │
│  └──────┬──────┘  └─────┬──────┘  └────────┬───────┘ │
└──────────────┬───────────────────────┬─────────────────┘
               │ (all tools call)│
┌──────────────┴─────────────────┴─────────────────┐
│       Adapter Module (weather_broker.py)                 │
│   OpenMeteoWeatherBroker                                 │
│   resolve_location() → geocoding API                     │
│   get_current_weather() → forecast API                   │
│   get_forecast() → forecast API                          │
│   predict_recommendation() → business logic              │
└────────────────────────┬─────────────────────────┘
                         │ (HTTP REST)
┌────────────────────────┴─────────────────────────┐
│               Open-Meteo API                            │
│   Geocoding: geocoding-api.open-meteo.com                │
│   Forecast:  api.open-meteo.com/v1/forecast              │
│   Free tier: no API key, 10,000+ calls/day               │
└──────────────────────────────────────────────────┘
```

## MLflow Observability Layer

```
Agent Bricks tool call
        │
        ▼
[MLflow Root Span: tool.get_current_weather]
        │
        ├─► [Child Span: get_current_weather]  ← broker method
        │       │
        │       └─► [Child Span: resolve_location] ← geocoding call
        │
        └─► Traced in /Shared/weather-mcp-tracing MLflow experiment
```

All spans capture:
* **Inputs**: location, date, days parameters
* **Outputs**: resolved location, conditions, success/error flag
* **Latency**: automatically captured by MLflow Tracing

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Weather API | Open-Meteo | Free, no auth, global, 16-day forecast |
| Temperatures | Fahrenheit | Spec thresholds in °F; US-primary audience |
| MCP Transport | streamable-http | Modern FastMCP 2.x standard; works with Databricks Apps |
| MLflow Tracing | `mlflow.start_span` | Per-call observability; portfolio LLMOps standard |
| Error handling | Return `{"error": ...}` dict | MCP tools must never raise; agent gets clean error message |
| Days clamping | Silent clamp in broker | Spec allows 1-16; tool layer validates strictly |

## Business Logic Thresholds

```
predict_recommendation() — hardcoded, not from API:

  bring_umbrella   = precipitation_chance > 40%
  bring_jacket     = avg_temperature      < 60°F
  good_for_outdoors= precipitation_chance < 30%
                     AND 50°F ≤ avg_temp ≤ 85°F

  confidence = max(0.50, 1.00 - days_ahead × 0.05)
               (100% today → 50% floor at day 10+)
```
