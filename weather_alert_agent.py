"""
Weather Alert Agent
===================
Monitors weather forecasts for Portland, OR (97231) and sends SMS text alerts
via Zapier webhook when specific conditions are detected.

Alert Conditions:
  1. Freeze Watch:  Temp <= 31°F within next 10 days
  2. Urgent Freeze: Temp <= 28°F within next 10 days
  3. Rain Incoming:  Currently clear, rain >= 0.25in expected within 7 days
  4. Heavy Rain:     2+ inches cumulative rain in next 10 days
  5. High Winds:    Wind >= 30 mph within next 10 days

Usage:
  python weather_alert_agent.py            # Continuous loop (every 6 hours)
  python weather_alert_agent.py --once     # Single check, then exit
  python weather_alert_agent.py --test     # Send a test SMS
  python weather_alert_agent.py --status   # Show forecast + alert state
"""

import sys
import os
import time
import json
import logging
from datetime import datetime, timedelta

# Add local python_libs to path (following project convention)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'python_libs'))

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("WARNING: 'requests' library not found. Install with: pip install requests")


# =============================================================================
# CONFIGURATION
# =============================================================================

# --- Location ---
LOCATION = {
    "name": "Portland, OR (97231)",
    "latitude": 45.62,
    "longitude": -122.82,
    "timezone": "America/Los_Angeles",
}

# --- Zapier Webhook for SMS ---
# In cloud mode (GitHub Actions), these come from environment variables.
# For local use, the defaults below are used as fallback.
ZAPIER_CONFIG = {
    "webhook_url": os.environ.get("ZAPIER_WEBHOOK_URL", ""),
    "phone_number": os.environ.get("ALERT_PHONE_NUMBER", ""),
}

# --- Alert Thresholds ---
ALERT_THRESHOLDS = {
    "freeze_watch_temp_f": 31.0,         # Freeze Watch: <= 31F
    "freeze_urgent_temp_f": 28.0,        # Urgent Freeze: <= 28F
    "freeze_warning_days": 10,           # Both freeze alerts look 10 days out
    "heavy_rain_inches": 2.0,            # Heavy Rain: >= 2.0 inches cumulative
    "heavy_rain_days": 10,               # Heavy Rain look-ahead window
    "rain_change_days": 7,               # Rain Incoming look-ahead window
    "rain_change_min_inches": 0.25,      # Rain Incoming: >= 0.25 inches per day
    "high_wind_mph": 30.0,               # High Winds: >= 30 mph
    "high_wind_days": 10,                # High Winds look-ahead window
}

# --- Operational ---
OPERATIONAL = {
    "check_interval_minutes": 360,       # 6 hours between checks in loop mode
    "dedup_cooldown_hours": 24,          # Don't re-send same alert within N hours
    "state_file": "weather_alerts_state.json",
    "log_file": "weather_alert_agent.log",
    "api_base_url": "https://api.open-meteo.com/v1/forecast",
    "forecast_days": 16,
    "max_sms_length": 160,
}

# --- Open-Meteo WMO Weather Codes ---
WEATHER_CODES = {
    "clear": [0, 1, 2, 3],
    "fog": [45, 48],
    "drizzle": [51, 53, 55],
    "freezing_drizzle": [56, 57],
    "rain": [61, 63, 65],
    "freezing_rain": [66, 67],
    "snow": [71, 73, 75, 77],
    "rain_showers": [80, 81, 82],
    "snow_showers": [85, 86],
    "thunderstorm": [95, 96, 99],
}

# Build a flat set of all "rainy" codes for quick lookup
RAIN_CODES = set(
    WEATHER_CODES["drizzle"] + WEATHER_CODES["rain"] +
    WEATHER_CODES["freezing_drizzle"] + WEATHER_CODES["freezing_rain"] +
    WEATHER_CODES["rain_showers"] + WEATHER_CODES["thunderstorm"]
)

CLEAR_CODES = set(WEATHER_CODES["clear"])


def weather_code_description(code):
    """Return a human-readable description for a WMO weather code."""
    descriptions = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Fog", 48: "Rime fog",
        51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
        56: "Light freezing drizzle", 57: "Dense freezing drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        66: "Light freezing rain", 67: "Heavy freezing rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow", 77: "Snow grains",
        80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
        85: "Slight snow showers", 86: "Heavy snow showers",
        95: "Thunderstorm", 96: "Thunderstorm w/ slight hail", 99: "Thunderstorm w/ heavy hail",
    }
    return descriptions.get(code, f"Unknown ({code})")


# =============================================================================
# WEATHER ALERT AGENT
# =============================================================================

class WeatherAlertAgent:
    """
    Fetches weather forecasts from Open-Meteo and sends SMS alerts
    via Zapier webhook when configured conditions are met.
    """

    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = self._setup_logging()

        if not REQUESTS_AVAILABLE:
            self.logger.error("Cannot run without 'requests' library.")
            sys.exit(1)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WeatherAlertAgent/1.0',
            'Accept': 'application/json',
        })

        self.state = self._load_state()

    # -------------------------------------------------------------------------
    # Setup helpers
    # -------------------------------------------------------------------------

    def _setup_logging(self):
        """Configure logging to both file and console."""
        logger = logging.getLogger("WeatherAlertAgent")
        logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers on re-init
        if logger.handlers:
            return logger

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

        # File handler
        log_path = os.path.join(self.script_dir, OPERATIONAL["log_file"])
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger

    # -------------------------------------------------------------------------
    # Deduplication state management
    # -------------------------------------------------------------------------

    def _state_path(self):
        return os.path.join(self.script_dir, OPERATIONAL["state_file"])

    def _load_state(self):
        """Load alert deduplication state from JSON file."""
        path = self._state_path()
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    state = json.load(f)
                # Prune entries older than 7 days
                self._prune_state(state)
                return state
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning("Could not load state file: %s. Starting fresh.", e)
        return {"sent_alerts": {}}

    def _save_state(self):
        """Persist alert state to JSON file."""
        try:
            with open(self._state_path(), 'w') as f:
                json.dump(self.state, f, indent=2)
        except IOError as e:
            self.logger.error("Could not save state file: %s", e)

    def _prune_state(self, state):
        """Remove alert entries older than 7 days."""
        cutoff = datetime.now() - timedelta(days=7)
        to_remove = []
        for key, timestamp_str in state.get("sent_alerts", {}).items():
            try:
                ts = datetime.fromisoformat(timestamp_str)
                if ts < cutoff:
                    to_remove.append(key)
            except (ValueError, TypeError):
                to_remove.append(key)
        for key in to_remove:
            del state["sent_alerts"][key]

    def _is_alert_suppressed(self, alert_key):
        """Check if this alert was already sent within the cooldown window."""
        timestamp_str = self.state.get("sent_alerts", {}).get(alert_key)
        if not timestamp_str:
            return False
        try:
            sent_time = datetime.fromisoformat(timestamp_str)
            cooldown = timedelta(hours=OPERATIONAL["dedup_cooldown_hours"])
            return datetime.now() - sent_time < cooldown
        except (ValueError, TypeError):
            return False

    def _record_alert_sent(self, alert_key):
        """Record that an alert was sent."""
        self.state.setdefault("sent_alerts", {})[alert_key] = datetime.now().isoformat()
        self._save_state()

    # -------------------------------------------------------------------------
    # Weather API
    # -------------------------------------------------------------------------

    def fetch_forecast(self):
        """Fetch forecast from Open-Meteo API. Returns parsed JSON or None."""
        params = {
            "latitude": LOCATION["latitude"],
            "longitude": LOCATION["longitude"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode,windspeed_10m_max",
            "hourly": "windspeed_10m",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "windspeed_unit": "mph",
            "timezone": LOCATION["timezone"],
            "forecast_days": OPERATIONAL["forecast_days"],
        }

        try:
            response = self.session.get(
                OPERATIONAL["api_base_url"], params=params, timeout=30
            )
            response.raise_for_status()
            data = response.json()

            # Validate response structure
            daily = data.get("daily", {})
            required_keys = [
                "time", "temperature_2m_max", "temperature_2m_min",
                "precipitation_sum", "weathercode", "windspeed_10m_max"
            ]
            for key in required_keys:
                if key not in daily:
                    self.logger.error("Missing key '%s' in API response", key)
                    return None

            self.logger.debug("Forecast fetched: %d days of data", len(daily["time"]))
            return data

        except requests.RequestException as e:
            self.logger.error("Failed to fetch forecast: %s", e)
            return None
        except (ValueError, KeyError) as e:
            self.logger.error("Failed to parse forecast response: %s", e)
            return None

    # -------------------------------------------------------------------------
    # Date formatting helpers
    # -------------------------------------------------------------------------

    def _days_away(self, date_str):
        """Return number of days from today to a date string like '2026-02-12'."""
        target = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = datetime.now().date()
        return (target - today).days

    def _day_name(self, date_str):
        """Return day name like 'Saturday' from a date string."""
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")

    def _short_day_name(self, date_str):
        """Return short day name like 'Mon' from a date string."""
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")[:3]

    def _friendly_day_list(self, date_strings):
        """Turn a list of date strings into 'Saturday, Sunday and Monday'."""
        names = [self._day_name(d) for d in date_strings]
        if len(names) == 1:
            return names[0]
        elif len(names) == 2:
            return f"{names[0]} and {names[1]}"
        else:
            return ", ".join(names[:-1]) + " and " + names[-1]

    # -------------------------------------------------------------------------
    # Alert checks
    # -------------------------------------------------------------------------

    def check_freeze_alerts(self, forecast):
        """
        Check for freezing temperatures.
        - Freeze Watch: <= 31F within 10 days
        - Urgent Freeze: <= 28F within 10 days
        Returns list of (alert_key, message) tuples.
        """
        alerts = []
        daily = forecast["daily"]
        dates = daily["time"]
        min_temps = daily["temperature_2m_min"]
        warning_days = min(ALERT_THRESHOLDS["freeze_warning_days"], len(dates))

        # --- Freeze Watch: <= 31F in next 10 days ---
        watch_threshold = ALERT_THRESHOLDS["freeze_watch_temp_f"]
        freeze_watch_days = []
        for i in range(warning_days):
            if min_temps[i] is not None and min_temps[i] <= watch_threshold:
                freeze_watch_days.append((dates[i], min_temps[i]))

        if freeze_watch_days:
            first_freeze = freeze_watch_days[0]
            alert_key = f"freeze_watch_{first_freeze[0]}"
            if not self._is_alert_suppressed(alert_key):
                days_away = self._days_away(first_freeze[0])
                day_list = " / ".join(
                    f"{self._day_name(d)} {t:.0f}F" for d, t in freeze_watch_days
                )
                msg = f"FREEZE WATCH: {watch_threshold:.0f}F or below in {days_away} days\n{day_list}"
                alerts.append((alert_key, msg))

        # --- Urgent Freeze: <= 28F in next 10 days ---
        urgent_threshold = ALERT_THRESHOLDS["freeze_urgent_temp_f"]
        freeze_urgent_days = []
        for i in range(warning_days):
            if min_temps[i] is not None and min_temps[i] <= urgent_threshold:
                freeze_urgent_days.append((dates[i], min_temps[i]))

        if freeze_urgent_days:
            first_freeze = freeze_urgent_days[0]
            alert_key = f"freeze_urgent_{first_freeze[0]}"
            if not self._is_alert_suppressed(alert_key):
                # Find the lowest temp across all urgent freeze days
                lowest_temp = min(t for _, t in freeze_urgent_days)
                day_list = "\n".join(
                    f"{self._day_name(d)} {t:.0f}F" for d, t in freeze_urgent_days
                )
                msg = f"URGENT FREEZE: Low of {lowest_temp:.0f}F\n\n{day_list}"
                alerts.append((alert_key, msg))

        return alerts

    def check_rain_change_alert(self, forecast):
        """
        Check if today is clear and significant rain (>= 0.25in) is coming within 7 days.
        Shows each rainy day with amounts.
        Returns list of (alert_key, message) tuples.
        """
        alerts = []
        daily = forecast["daily"]
        dates = daily["time"]
        weather_codes = daily["weathercode"]
        precip = daily["precipitation_sum"]

        if not weather_codes or weather_codes[0] is None:
            return alerts

        # Is today clear?
        today_code = weather_codes[0]
        if today_code not in CLEAR_CODES:
            return alerts  # Not clear today — no alert

        # Find all rainy days >= 0.25 inches in next 7 days
        check_days = min(ALERT_THRESHOLDS["rain_change_days"], len(dates))
        min_rain = ALERT_THRESHOLDS["rain_change_min_inches"]
        rainy_days = []
        total_rain = 0.0

        for i in range(1, check_days):  # Skip today
            if (weather_codes[i] is not None and weather_codes[i] in RAIN_CODES
                    and precip[i] is not None and precip[i] >= min_rain):
                rainy_days.append((dates[i], precip[i]))
                total_rain += precip[i]

        if rainy_days:
            first_rain = rainy_days[0][0]
            alert_key = f"rain_change_{first_rain}"
            if not self._is_alert_suppressed(alert_key):
                day_list = "\n".join(
                    f"{self._day_name(d)} {p:.1f}" for d, p in rainy_days
                )
                msg = f"RAIN INCOMING: {total_rain:.1f} In\n\n{day_list}"
                alerts.append((alert_key, msg))

        return alerts

    def check_heavy_rain_alert(self, forecast):
        """
        Check if cumulative precipitation over next 10 days exceeds 2.0 inches.
        Shows daily breakdown and when rain stops / sun returns.
        Returns list of (alert_key, message) tuples.
        """
        alerts = []
        daily = forecast["daily"]
        precip = daily["precipitation_sum"]
        dates = daily["time"]
        weather_codes = daily["weathercode"]

        check_days = min(ALERT_THRESHOLDS["heavy_rain_days"], len(dates))
        total_rain = sum(p for p in precip[:check_days] if p is not None)
        threshold = ALERT_THRESHOLDS["heavy_rain_inches"]

        if total_rain >= threshold:
            alert_key = f"heavy_rain_{dates[0]}"
            if not self._is_alert_suppressed(alert_key):
                # Get rainy days sorted by date
                day_pairs = [
                    (dates[i], precip[i]) for i in range(check_days)
                    if precip[i] is not None and precip[i] > 0
                ]
                day_pairs.sort(key=lambda x: x[0])

                # Daily breakdown with line breaks
                rain_day_list = "\n".join(
                    f"{self._day_name(d)} {p:.1f}" for d, p in day_pairs
                )

                # Find first clear/sunny day
                sun_comes_out_day = None
                all_days = len(dates)
                for i in range(1, all_days):
                    if (weather_codes[i] is not None
                            and weather_codes[i] in CLEAR_CODES):
                        sun_comes_out_day = dates[i]
                        break

                # Short sun outlook line
                if sun_comes_out_day:
                    outlook = f"Sun {self._day_name(sun_comes_out_day)}"
                else:
                    outlook = "No sun on the horizon"

                msg = (f"HEAVY RAIN: {total_rain:.1f} in.\n\n"
                       f"{rain_day_list}\n"
                       f"{outlook}")
                alerts.append((alert_key, msg))

        return alerts

    def _format_hour(self, hour):
        """Format 24h hour as '12 AM', '2 PM', etc."""
        if hour == 0:
            return "12 AM"
        elif hour < 12:
            return f"{hour} AM"
        elif hour == 12:
            return "12 PM"
        else:
            return f"{hour - 12} PM"

    def _get_wind_time_ranges(self, forecast, date_str):
        """
        Given hourly wind data and a date, find contiguous time ranges
        where wind >= threshold. Returns list of (start_hour, end_hour, max_speed).
        """
        hourly = forecast.get("hourly", {})
        hourly_times = hourly.get("time", [])
        hourly_winds = hourly.get("windspeed_10m", [])
        threshold = ALERT_THRESHOLDS["high_wind_mph"]

        if not hourly_times or not hourly_winds:
            return []

        # Find all hours for this date that exceed threshold
        windy_hours = []
        for i, t in enumerate(hourly_times):
            if t.startswith(date_str) and i < len(hourly_winds):
                speed = hourly_winds[i]
                if speed is not None and speed >= threshold:
                    hour = int(t[11:13])  # Extract hour from "2026-02-07T14:00"
                    windy_hours.append((hour, speed))

        if not windy_hours:
            return []

        # Group into contiguous ranges
        ranges = []
        range_start = windy_hours[0][0]
        range_max = windy_hours[0][1]
        prev_hour = windy_hours[0][0]

        for hour, speed in windy_hours[1:]:
            if hour == prev_hour + 1:
                # Contiguous - extend range
                range_max = max(range_max, speed)
                prev_hour = hour
            else:
                # Gap - close current range, start new one
                ranges.append((range_start, prev_hour + 1, range_max))
                range_start = hour
                range_max = speed
                prev_hour = hour

        # Close the last range
        ranges.append((range_start, prev_hour + 1, range_max))

        return ranges

    def check_high_wind_alert(self, forecast):
        """
        Check if max wind speed >= 30 mph on any day in next 10 days.
        Shows each windy day with hourly time ranges and peak speed.
        Returns list of (alert_key, message) tuples.
        """
        alerts = []
        daily = forecast["daily"]
        dates = daily["time"]
        wind_speeds = daily["windspeed_10m_max"]
        threshold = ALERT_THRESHOLDS["high_wind_mph"]
        check_days = min(ALERT_THRESHOLDS["high_wind_days"], len(dates))

        windy_days = []
        for i in range(check_days):
            if wind_speeds[i] is not None and wind_speeds[i] >= threshold:
                windy_days.append((dates[i], wind_speeds[i]))

        if windy_days:
            first_windy = windy_days[0]
            alert_key = f"high_wind_{first_windy[0]}"
            if not self._is_alert_suppressed(alert_key):
                day_lines = []
                for date_str, max_speed in windy_days:
                    day_name = self._day_name(date_str)
                    ranges = self._get_wind_time_ranges(forecast, date_str)
                    if ranges:
                        # Show each time range for this day
                        for start_h, end_h, peak in ranges:
                            start_str = self._format_hour(start_h)
                            end_str = self._format_hour(end_h if end_h < 24 else 0)
                            day_lines.append(
                                f"{day_name} {start_str} - {end_str} : {peak:.0f} mph"
                            )
                    else:
                        # Fallback if no hourly data available
                        day_lines.append(f"{day_name} {max_speed:.0f} mph")

                msg = f"HIGH WINDS ALERT\n\n" + "\n".join(day_lines)
                alerts.append((alert_key, msg))

        return alerts

    # -------------------------------------------------------------------------
    # SMS sending
    # -------------------------------------------------------------------------

    def _truncate_sms(self, message):
        """Truncate message to SMS character limit."""
        max_len = OPERATIONAL["max_sms_length"]
        if len(message) <= max_len:
            return message
        return message[:max_len - 3] + "..."

    def send_alert(self, message):
        """Send an SMS via Zapier webhook."""
        try:
            response = self.session.post(
                ZAPIER_CONFIG["webhook_url"],
                json={
                    "message": message,
                    "phone": ZAPIER_CONFIG["phone_number"],
                },
                timeout=15,
            )
            response.raise_for_status()
            self.logger.info("Alert sent via Zapier: %s", message[:80])
            return True

        except requests.RequestException as e:
            self.logger.error("Failed to send alert via Zapier: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------

    def run_checks(self):
        """Run all weather checks and send any triggered alerts."""
        self.logger.info("Starting weather check for %s", LOCATION["name"])
        sent_messages = []

        forecast = self.fetch_forecast()
        if forecast is None:
            self.logger.error("Could not fetch forecast. Skipping this check.")
            return sent_messages

        # Gather all alerts
        all_alerts = []
        all_alerts.extend(self.check_freeze_alerts(forecast))
        all_alerts.extend(self.check_rain_change_alert(forecast))
        all_alerts.extend(self.check_heavy_rain_alert(forecast))
        all_alerts.extend(self.check_high_wind_alert(forecast))

        if not all_alerts:
            self.logger.info("No alert conditions detected.")
            return sent_messages

        # Send each alert
        for alert_key, message in all_alerts:
            self.logger.info("Alert triggered [%s]: %s", alert_key, message)
            success = self.send_alert(message)
            if success:
                self._record_alert_sent(alert_key)
                sent_messages.append(message)
            else:
                self.logger.warning("Failed to send alert [%s]", alert_key)

        self.logger.info("Check complete. %d alert(s) sent.", len(sent_messages))
        return sent_messages

    def run_loop(self):
        """Run weather checks in a continuous loop."""
        interval_sec = OPERATIONAL["check_interval_minutes"] * 60
        self.logger.info(
            "Starting continuous monitoring. Check every %d minutes.",
            OPERATIONAL["check_interval_minutes"]
        )

        while True:
            try:
                self.run_checks()
            except Exception as e:
                self.logger.error("Unhandled error during check: %s", e, exc_info=True)

            self.logger.info(
                "Next check in %d minutes.", OPERATIONAL["check_interval_minutes"]
            )
            try:
                time.sleep(interval_sec)
            except KeyboardInterrupt:
                self.logger.info("Interrupted by user. Shutting down.")
                break

    def send_test_sms(self):
        """Send a test SMS to verify the Zapier webhook works."""
        test_msg = f"Weather Alert Agent test. SMS delivery is working! ({LOCATION['name']})"
        self.logger.info("Sending test SMS via Zapier webhook...")
        success = self.send_alert(test_msg)
        if success:
            self.logger.info("Test SMS sent successfully!")
        else:
            self.logger.error("Test SMS failed. Check configuration and logs.")
        return success

    def show_status(self):
        """Print forecast summary and alert state to console."""
        print(f"\n{'='*60}")
        print(f"  Weather Alert Agent - Status")
        print(f"  Location: {LOCATION['name']}")
        print(f"  Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        # Webhook config status
        print(f"  Webhook: {ZAPIER_CONFIG['webhook_url'][:50]}...")
        print(f"  Phone:   {ZAPIER_CONFIG['phone_number']}")
        print()

        # Fetch forecast
        forecast = self.fetch_forecast()
        if forecast is None:
            print("  ERROR: Could not fetch forecast from Open-Meteo API.\n")
            return

        daily = forecast["daily"]
        dates = daily["time"]
        max_temps = daily["temperature_2m_max"]
        min_temps = daily["temperature_2m_min"]
        precip = daily["precipitation_sum"]
        codes = daily["weathercode"]

        # Display forecast table
        print(f"  {'Date':<12} {'Lo':>5} {'Hi':>5} {'Rain':>6}  {'Conditions'}")
        print(f"  {'-'*12} {'-'*5} {'-'*5} {'-'*6}  {'-'*20}")

        total_precip = 0.0
        for i in range(min(ALERT_THRESHOLDS["heavy_rain_days"], len(dates))):
            lo = f"{min_temps[i]:.0f}F" if min_temps[i] is not None else "  N/A"
            hi = f"{max_temps[i]:.0f}F" if max_temps[i] is not None else "  N/A"
            rain = f"{precip[i]:.2f}\"" if precip[i] is not None else "  N/A"
            cond = weather_code_description(codes[i]) if codes[i] is not None else "N/A"

            # Flag freeze days
            freeze_flag = " ***" if (min_temps[i] is not None
                                     and min_temps[i] < ALERT_THRESHOLDS["freeze_watch_temp_f"]) else ""

            print(f"  {dates[i]:<12} {lo:>5} {hi:>5} {rain:>6}  {cond}{freeze_flag}")

            if precip[i] is not None:
                total_precip += precip[i]

        print(f"\n  Total precipitation (10 days): {total_precip:.2f} inches")

        # Show what alerts would fire
        print(f"\n  --- Alert Analysis ---")
        alerts = []
        alerts.extend(self.check_freeze_alerts(forecast))
        alerts.extend(self.check_rain_change_alert(forecast))
        alerts.extend(self.check_heavy_rain_alert(forecast))
        alerts.extend(self.check_high_wind_alert(forecast))

        if alerts:
            for key, msg in alerts:
                suppressed = self._is_alert_suppressed(key)
                status = "(SUPPRESSED - already sent)" if suppressed else "(WOULD SEND)"
                print(f"  {status} {msg}")
        else:
            print("  No alert conditions detected.")

        # Show dedup state
        sent = self.state.get("sent_alerts", {})
        if sent:
            print(f"\n  --- Recently Sent Alerts ---")
            for key, ts in sorted(sent.items(), key=lambda x: x[1], reverse=True):
                print(f"  {ts}  {key}")

        print()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Weather Alert Agent - SMS alerts for freeze and rain conditions"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single check and exit (for Task Scheduler / cron)"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Send a test SMS to verify configuration"
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current forecast and alert state without sending"
    )
    args = parser.parse_args()

    agent = WeatherAlertAgent()

    if args.test:
        success = agent.send_test_sms()
        sys.exit(0 if success else 1)
    elif args.status:
        agent.show_status()
    elif args.once:
        agent.run_checks()
    else:
        agent.run_loop()


if __name__ == "__main__":
    main()
