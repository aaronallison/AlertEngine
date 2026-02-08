# Weather Alert Agent - System Overview

## How It Works

A Python script checks the weather forecast for Portland, OR (97231) once per day and sends you a text message if any alert conditions are met.

```
Open-Meteo API  -->  Python Script  -->  Zapier Webhook  -->  SMS to Phone
 (weather data)     (GitHub Actions)     (hooks.zapier.com)   (503-481-5324)
```

---

## Where It Runs

| Component | Location | Details |
|-----------|----------|---------|
| **Script** | GitHub Actions (cloud) | `aaronallison/AlertEngine` repo |
| **Schedule** | Cron: `0 3 * * *` (UTC) | **Daily at 7 PM Pacific** |
| **Weather API** | Open-Meteo (free, no key) | 16-day forecast, temps in F, rain in inches |
| **SMS Delivery** | Zapier webhook | Posts JSON to catch hook, Zapier sends SMS |
| **Secrets** | GitHub repo settings | `ZAPIER_WEBHOOK_URL` and `ALERT_PHONE_NUMBER` |

**Manual trigger:** Go to Actions tab on GitHub > Weather Alert Check > Run workflow

**Local commands:**
- `python weather_alert_agent.py --status` - See forecast and what alerts would fire
- `python weather_alert_agent.py --once` - Run one check cycle
- `python weather_alert_agent.py --test` - Send a test text

---

## Alert Types

### 1. FREEZE WATCH

**Checks:** Is the low temperature below 32F on any day in the next 10 days?

```
IF any day in next 10 days has min_temp < 32F
AND this alert has not been sent in the last 24 hours
THEN send alert
```

**Example text:** `FREEZE WATCH: Below 32F in 6 days - Saturday, Sunday and Monday coming up`

---

### 2. URGENT FREEZE

**Checks:** Is the low temperature below 32F in the next 2 days?

```
IF any day in next 2 days has min_temp < 32F
AND this alert has not been sent in the last 24 hours
THEN send alert
```

**Example text:** `URGENT FREEZE: 28F expected Friday night - protect plants!`

---

### 3. RAIN INCOMING

**Checks:** Is it clear/sunny today AND will it rain within 7 days?

```
IF today's weather code is Clear (0), Mainly Clear (1), Partly Cloudy (2), or Overcast (3)
AND any day in next 7 days has a rain/drizzle/thunderstorm weather code
AND that day has precipitation > 0 inches
AND this alert has not been sent in the last 24 hours
THEN send alert
```

**Rain weather codes:** Drizzle (51-55), Freezing Drizzle (56-57), Rain (61-65), Freezing Rain (66-67), Rain Showers (80-82), Thunderstorm (95-99)

**Example text:** `RAIN INCOMING: Clear now, rain starting Wednesday (3 days out)`

---

### 4. HEAVY RAIN

**Checks:** Will there be 2 or more inches of total rain in the next 10 days?

```
IF sum of all daily precipitation over next 10 days >= 2.0 inches
AND this alert has not been sent in the last 24 hours
THEN send alert (includes which day is heaviest)
```

**Example text:** `HEAVY RAIN: 3.2 in expected over next 10 days - heaviest on Saturday`

---

## Thresholds Summary

| Alert | Threshold | Look-ahead Window |
|-------|-----------|-------------------|
| Freeze Watch | Low temp < 32F | 10 days |
| Urgent Freeze | Low temp < 32F | 2 days |
| Rain Incoming | Today clear + rain ahead | 7 days |
| Heavy Rain | Total rain >= 2.0 inches | 10 days |

---

## Deduplication (Don't Send Duplicates)

Each alert has a unique key that includes the triggering date:
- `freeze_10day_2026-02-12` - Freeze watch for a specific first-freeze date
- `freeze_urgent_2026-02-07` - Urgent freeze for a specific date
- `rain_change_2026-02-09` - Rain change for a specific first-rain date
- `heavy_rain_2026-02-07` - Heavy rain keyed to today's date

**Rule:** Same alert key will not be sent again within 24 hours. If the forecast changes (different freeze date, different rain date), a new alert fires because it's a different key.

State is stored in `weather_alerts_state.json`. Entries older than 7 days are automatically cleaned up.

---

## Files

| File | Purpose |
|------|---------|
| `weather_alert_agent.py` | Main script - all logic, alerts, and SMS sending |
| `.github/workflows/weather-check.yml` | GitHub Actions schedule (7 PM Pacific daily) |
| `requirements.txt` | Python dependency: `requests` |
| `.gitignore` | Excludes log files, state files, cache |
| `weather_alerts_state.json` | Auto-created at runtime for dedup tracking |
| `weather_alert_agent.log` | Auto-created at runtime for logging |

---

## Weather Data Source

**API:** Open-Meteo (`https://api.open-meteo.com/v1/forecast`)
- Free, no API key required
- Provides 16-day forecasts
- Data requested: daily high/low temps (F), precipitation (inches), weather codes
- Location: lat 45.62, lon -122.82 (Portland, OR 97231)
