"""
Degree Day Spray for Weeds Alert
==================================
Monitors Growing Degree Days (GDD) and weather conditions to alert when
weed emergence is likely and when to spray.

Location: 45.662917, -122.815922 (Portland, OR 97231 / Sauvie Island)
Data Source: Open-Meteo (free, no API key)

How GDD is Calculated:
  daily_mean = (T_max + T_min) / 2
  GDD = max(0, daily_mean - base_temp)
  Cumulative GDD = sum of daily GDD from season start date

Trigger Seasons:
  FALL (Oct-Dec):
    - Winter annual weeds (chickweed, henbit, mustards, Poa annua)
    - Triggered by: avg daily temp dropping below 70F + rain arriving
    - Action: PRE-emergent on clean soil before germination
    - Spray window: within 7-14 days of trigger

  LATE WINTER (Feb-Mar):
    - Winter annuals + rosettes actively growing
    - Triggered by: consecutive days above 45F + weeds resuming growth
    - Action: POST-emergent spot spray while weeds small (<6 inches)
    - Spray window: 7-21 days after first emergence signs

  SPRING (Mar-May):
    - Warm-season annual grasses (crabgrass, foxtail)
    - Triggered by: cumulative GDD50 reaching ~125 (heads-up), ~150 (apply-by)
    - Action: PRE-emergent before ~200 GDD50
    - Also: Spring broadleaf flush (lambsquarters, pigweed, ragweed)

  LATE SPRING (May-Jun):
    - Warm-season broadleaves accelerate
    - Triggered by: cumulative GDD50 reaching ~300-500
    - Action: POST-emergent on small seedlings

Database: SQLite (gdd_sis.db) tracks daily GDD accumulation

Usage:
  python gdd_weed_alert.py              # Daily check - fetch weather, calc GDD, send alerts
  python gdd_weed_alert.py --status     # Show current GDD accumulation and upcoming triggers
  python gdd_weed_alert.py --test       # Send test alerts for all trigger types
  python gdd_weed_alert.py --backfill   # Backfill GDD data from Jan 1 of current year
"""

import sys
import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta

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

LOCATION = {
    "name": "Sauvie Island / Portland, OR 97231",
    "latitude": 45.662917,
    "longitude": -122.815922,
    "timezone": "America/Los_Angeles",
}

ZAPIER_CONFIG = {
    "webhook_url": os.environ.get(
        "ZAPIER_WEBHOOK_URL",
        "https://hooks.zapier.com/hooks/catch/23257298/ueog4uk/"
    ),
    "phone_number": os.environ.get("ALERT_PHONE_NUMBER", "5034815324"),
}

# GDD Base Temperatures
GDD_BASES = {
    "GDD50": 50.0,   # Warm-season annuals (crabgrass, foxtail)
    "GDD32": 32.0,   # General plant activity
}

# =============================================================================
# ALERT TRIGGER THRESHOLDS
# =============================================================================

TRIGGERS = {
    # --- FALL PRE-EMERGENT (Oct-Dec) ---
    # Winter annuals germinate when temps cool + first rains arrive
    "fall_pre": {
        "name": "FALL PRE-EMERGENT",
        "season_months": [9, 10, 11, 12],  # Active Sep-Dec
        "conditions": {
            "avg_temp_below": 70.0,        # 5-day avg temp drops below 70F
            "rain_2day_min": 0.25,          # At least 0.25 inches rain in 2 days
            "avg_temp_window": 5,           # 5-day rolling average
        },
        "weeds": "chickweed, henbit, mustards, Poa annua, deadnettle",
        "action": "Apply PRE-emergent on clean soil. Rainfall will activate.",
        "spray_window_days": 14,
    },

    # --- LATE WINTER POST-EMERGENT (Feb-Mar) ---
    # Winter annuals resume growth, rosettes visible
    "late_winter_post": {
        "name": "LATE WINTER SCOUT & SPRAY",
        "season_months": [2, 3],
        "conditions": {
            "consecutive_warm_days": 5,     # 5 consecutive days with avg > 45F
            "warm_day_threshold": 45.0,     # What counts as a "warm" day
            "gdd32_min": 200,               # Minimum cumulative GDD32 for activity
        },
        "weeds": "winter annual rosettes, chickweed, henbit, shepherd's purse",
        "action": "Scout fields. Spot-spray POST while weeds < 6 inches, before bolting.",
        "spray_window_days": 21,
    },

    # --- SPRING PRE-EMERGENT (Mar-May) ---
    # Warm-season annual grasses about to germinate
    "spring_pre": {
        "name": "SPRING PRE-EMERGENT",
        "season_months": [3, 4, 5],
        "conditions": {
            "gdd50_headsup": 125,           # Heads-up: getting close
            "gdd50_apply_by": 150,          # Apply PRE by this point
            "gdd50_germination": 200,       # Germination onset - too late for PRE
        },
        "weeds": "crabgrass, foxtail, other warm-season annual grasses",
        "action": "Apply PRE-emergent before GDD50 hits 200. Earlier if history of early germination.",
        "spray_window_days": 14,
    },

    # --- SPRING BROADLEAF FLUSH (Apr-Jun) ---
    # Warm-season broadleaf annuals emerging
    "spring_broadleaf": {
        "name": "SPRING BROADLEAF FLUSH",
        "season_months": [4, 5, 6],
        "conditions": {
            "gdd50_emergence": 300,         # Broadleaf annuals emerging
            "gdd50_spray_by": 500,          # Spray POST while still small
        },
        "weeds": "lambsquarters, pigweed, ragweed, spotted spurge, groundsel",
        "action": "POST-emergent spray while seedlings small (2-6 leaf stage). 7-21 days after emergence.",
        "spray_window_days": 21,
    },

    # --- PERENNIAL ROSETTE WINDOWS (Fall & Spring) ---
    "perennial_fall": {
        "name": "PERENNIAL FALL ROSETTE",
        "season_months": [9, 10, 11],
        "conditions": {
            "avg_temp_below": 65.0,
            "avg_temp_window": 7,
        },
        "weeds": "dandelion, dock, thistle, plantain, buttercup, blackberry regrowth",
        "action": "Target rosettes and active regrowth. Best uptake during active growth before dormancy.",
        "spray_window_days": 21,
    },

    "perennial_spring": {
        "name": "PERENNIAL SPRING ROSETTE",
        "season_months": [3, 4, 5],
        "conditions": {
            "consecutive_warm_days": 7,
            "warm_day_threshold": 50.0,
            "gdd50_min": 100,
        },
        "weeds": "dandelion, dock, thistle, plantain, buttercup, burdock, bindweed",
        "action": "Spray rosettes/regrowth before bolting. Repeat apps may be needed for perennials.",
        "spray_window_days": 21,
    },
}

# =============================================================================
# DATABASE
# =============================================================================

DB_NAME = "gdd_sis.db"


def get_db_path(script_dir):
    return os.path.join(script_dir, DB_NAME)


def init_database(db_path):
    """Create the GDD-SIS database tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            date TEXT PRIMARY KEY,
            tmin_f REAL,
            tmax_f REAL,
            tmean_f REAL,
            precip_in REAL,
            gdd50 REAL,
            gdd32 REAL,
            cum_gdd50 REAL,
            cum_gdd32 REAL,
            avg_temp_5day REAL,
            rain_2day_sum REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts_sent (
            alert_key TEXT PRIMARY KEY,
            sent_at TEXT,
            message TEXT
        )
    """)

    conn.commit()
    return conn


# =============================================================================
# GDD WEED ALERT AGENT
# =============================================================================

class GDDWeedAlert:
    """
    Fetches weather data, calculates Growing Degree Days, tracks accumulation
    in SQLite, and sends SMS alerts when weed emergence triggers are hit.
    """

    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = self._setup_logging()

        if not REQUESTS_AVAILABLE:
            self.logger.error("Cannot run without 'requests' library.")
            sys.exit(1)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "GDDWeedAlert/1.0",
            "Accept": "application/json",
        })

        self.db_path = get_db_path(self.script_dir)
        self.conn = init_database(self.db_path)

    def _setup_logging(self):
        logger = logging.getLogger("GDDWeedAlert")
        logger.setLevel(logging.DEBUG)
        if logger.handlers:
            return logger

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

        log_path = os.path.join(self.script_dir, "gdd_weed_alert.log")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        return logger

    # -------------------------------------------------------------------------
    # Weather Data Fetching
    # -------------------------------------------------------------------------

    def fetch_historical(self, start_date, end_date):
        """Fetch historical daily weather from Open-Meteo Archive API."""
        params = {
            "latitude": LOCATION["latitude"],
            "longitude": LOCATION["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": LOCATION["timezone"],
        }

        try:
            response = self.session.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params=params, timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error("Failed to fetch historical data: %s", e)
            return None

    def fetch_recent_and_forecast(self):
        """Fetch recent days + forecast from Open-Meteo."""
        params = {
            "latitude": LOCATION["latitude"],
            "longitude": LOCATION["longitude"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": LOCATION["timezone"],
            "past_days": 14,
            "forecast_days": 16,
        }

        try:
            response = self.session.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params, timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error("Failed to fetch forecast data: %s", e)
            return None

    # -------------------------------------------------------------------------
    # GDD Calculation & Storage
    # -------------------------------------------------------------------------

    def calculate_and_store(self, weather_data):
        """Calculate GDD from weather data and store in database."""
        daily = weather_data.get("daily", {})
        dates = daily.get("time", [])
        tmaxs = daily.get("temperature_2m_max", [])
        tmins = daily.get("temperature_2m_min", [])
        precips = daily.get("precipitation_sum", [])

        if not dates:
            self.logger.warning("No data in weather response")
            return 0

        c = self.conn.cursor()
        rows_added = 0

        for i, date_str in enumerate(dates):
            tmax = tmaxs[i] if i < len(tmaxs) else None
            tmin = tmins[i] if i < len(tmins) else None
            precip = precips[i] if i < len(precips) else None

            if tmax is None or tmin is None:
                continue

            tmean = (tmax + tmin) / 2.0
            gdd50 = max(0.0, tmean - GDD_BASES["GDD50"])
            gdd32 = max(0.0, tmean - GDD_BASES["GDD32"])

            c.execute("""
                INSERT OR REPLACE INTO daily_weather
                (date, tmin_f, tmax_f, tmean_f, precip_in, gdd50, gdd32)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (date_str, tmin, tmax, tmean, precip, gdd50, gdd32))
            rows_added += 1

        self.conn.commit()
        self.logger.debug("Stored %d days of weather data", rows_added)

        # Now compute cumulative GDD and rolling averages
        self._compute_cumulative_gdd()
        self._compute_rolling_averages()

        return rows_added

    def _compute_cumulative_gdd(self):
        """Compute cumulative GDD from Jan 1 of each year."""
        c = self.conn.cursor()

        # Get all years in the data
        c.execute("SELECT DISTINCT substr(date, 1, 4) FROM daily_weather ORDER BY 1")
        years = [row[0] for row in c.fetchall()]

        for year in years:
            c.execute("""
                SELECT date, gdd50, gdd32 FROM daily_weather
                WHERE date >= ? AND date <= ?
                ORDER BY date
            """, (f"{year}-01-01", f"{year}-12-31"))

            cum50 = 0.0
            cum32 = 0.0
            for date_str, gdd50, gdd32 in c.fetchall():
                cum50 += (gdd50 or 0)
                cum32 += (gdd32 or 0)
                c.execute("""
                    UPDATE daily_weather SET cum_gdd50 = ?, cum_gdd32 = ?
                    WHERE date = ?
                """, (cum50, cum32, date_str))

        self.conn.commit()

    def _compute_rolling_averages(self):
        """Compute 5-day rolling avg temp and 2-day rain sum."""
        c = self.conn.cursor()
        c.execute("SELECT date, tmean_f, precip_in FROM daily_weather ORDER BY date")
        rows = c.fetchall()

        temps = []
        precips_list = []

        for date_str, tmean, precip in rows:
            temps.append((date_str, tmean or 0))
            precips_list.append((date_str, precip or 0))

        # 5-day avg temp
        for i in range(len(temps)):
            window = temps[max(0, i-4):i+1]
            avg5 = sum(t for _, t in window) / len(window)
            c.execute("UPDATE daily_weather SET avg_temp_5day = ? WHERE date = ?",
                      (avg5, temps[i][0]))

        # 2-day rain sum
        for i in range(len(precips_list)):
            window = precips_list[max(0, i-1):i+1]
            rain2 = sum(p for _, p in window)
            c.execute("UPDATE daily_weather SET rain_2day_sum = ? WHERE date = ?",
                      (rain2, precips_list[i][0]))

        self.conn.commit()

    # -------------------------------------------------------------------------
    # Alert Deduplication
    # -------------------------------------------------------------------------

    def _is_alert_sent(self, alert_key):
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM alerts_sent WHERE alert_key = ?", (alert_key,))
        return c.fetchone() is not None

    def _record_alert(self, alert_key, message):
        c = self.conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO alerts_sent (alert_key, sent_at, message)
            VALUES (?, ?, ?)
        """, (alert_key, datetime.now().isoformat(), message))
        self.conn.commit()

    # -------------------------------------------------------------------------
    # Trigger Checks
    # -------------------------------------------------------------------------

    def _get_recent_data(self, days=14):
        """Get the most recent N days of weather data."""
        c = self.conn.cursor()
        c.execute("""
            SELECT date, tmin_f, tmax_f, tmean_f, precip_in, gdd50, gdd32,
                   cum_gdd50, cum_gdd32, avg_temp_5day, rain_2day_sum
            FROM daily_weather
            ORDER BY date DESC
            LIMIT ?
        """, (days,))
        rows = c.fetchall()
        rows.reverse()  # Chronological order
        return rows

    def _get_today_data(self):
        """Get today's data row."""
        today = datetime.now().strftime("%Y-%m-%d")
        c = self.conn.cursor()
        c.execute("SELECT * FROM daily_weather WHERE date = ?", (today,))
        return c.fetchone()

    def _get_latest_data(self):
        """Get the most recent data row we have."""
        c = self.conn.cursor()
        c.execute("SELECT * FROM daily_weather ORDER BY date DESC LIMIT 1")
        return c.fetchone()

    def _day_name(self, date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")

    def check_fall_pre(self):
        """
        FALL PRE-EMERGENT trigger:
        5-day avg temp drops below 70F AND 2-day rain sum >= 0.25 inches.
        Active Sep-Dec.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["fall_pre"]["season_months"]:
            return None

        recent = self._get_recent_data(7)
        if len(recent) < 5:
            return None

        latest = recent[-1]
        date_str = latest[0]
        avg_temp_5day = latest[9]   # avg_temp_5day
        rain_2day = latest[10]      # rain_2day_sum

        trigger = TRIGGERS["fall_pre"]
        conds = trigger["conditions"]

        if (avg_temp_5day is not None and avg_temp_5day <= conds["avg_temp_below"]
                and rain_2day is not None and rain_2day >= conds["rain_2day_min"]):

            year = datetime.now().year
            alert_key = f"fall_pre_{year}"
            if self._is_alert_sent(alert_key):
                return None

            msg = (
                f"WEED ALERT: Fall Pre-Emergent Window Open\n\n"
                f"5-Day Avg Temp: {avg_temp_5day:.0f}F (below {conds['avg_temp_below']:.0f}F)\n"
                f"2-Day Rain: {rain_2day:.2f} in\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}\n\n"
                f"Spray within {trigger['spray_window_days']} days for best results."
            )
            return (alert_key, msg)

        return None

    def check_late_winter_post(self):
        """
        LATE WINTER POST-EMERGENT trigger:
        5+ consecutive days with avg temp > 45F AND cumulative GDD32 >= 200.
        Active Feb-Mar.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["late_winter_post"]["season_months"]:
            return None

        recent = self._get_recent_data(14)
        if len(recent) < 5:
            return None

        trigger = TRIGGERS["late_winter_post"]
        conds = trigger["conditions"]

        # Check consecutive warm days
        consecutive = 0
        max_consecutive = 0
        for row in recent:
            tmean = row[3]
            if tmean is not None and tmean >= conds["warm_day_threshold"]:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        latest = recent[-1]
        cum_gdd32 = latest[8]

        if (max_consecutive >= conds["consecutive_warm_days"]
                and cum_gdd32 is not None and cum_gdd32 >= conds["gdd32_min"]):

            year = datetime.now().year
            alert_key = f"late_winter_post_{year}"
            if self._is_alert_sent(alert_key):
                return None

            msg = (
                f"WEED ALERT: Late Winter Scout & Spray\n\n"
                f"Warm Streak: {max_consecutive} days above {conds['warm_day_threshold']:.0f}F\n"
                f"Cumulative GDD32: {cum_gdd32:.0f}\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}\n\n"
                f"Spray within {trigger['spray_window_days']} days while weeds are small."
            )
            return (alert_key, msg)

        return None

    def check_spring_pre(self):
        """
        SPRING PRE-EMERGENT trigger:
        GDD50 approaching crabgrass germination threshold.
        Heads-up at 125, Apply-By at 150, Too-Late at 200.
        Active Mar-May.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["spring_pre"]["season_months"]:
            return None

        latest = self._get_latest_data()
        if not latest:
            return None

        cum_gdd50 = latest[7]  # cum_gdd50
        if cum_gdd50 is None:
            return None

        trigger = TRIGGERS["spring_pre"]
        conds = trigger["conditions"]
        year = datetime.now().year

        # Three-tier alert
        if cum_gdd50 >= conds["gdd50_germination"]:
            alert_key = f"spring_pre_toolate_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: Crabgrass Germination Started!\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f} (threshold: {conds['gdd50_germination']})\n\n"
                f"PRE-emergent window has passed.\n"
                f"Switch to POST-emergent on small seedlings.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}"
            )
            return (alert_key, msg)

        elif cum_gdd50 >= conds["gdd50_apply_by"]:
            alert_key = f"spring_pre_applyby_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: APPLY PRE-EMERGENT NOW\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n"
                f"Apply-By Threshold: {conds['gdd50_apply_by']}\n"
                f"Germination at: {conds['gdd50_germination']}\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}"
            )
            return (alert_key, msg)

        elif cum_gdd50 >= conds["gdd50_headsup"]:
            alert_key = f"spring_pre_headsup_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: Spring PRE Heads-Up\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n"
                f"Apply-By: {conds['gdd50_apply_by']} GDD50\n"
                f"Germination: {conds['gdd50_germination']} GDD50\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Start planning PRE-emergent application."
            )
            return (alert_key, msg)

        return None

    def check_spring_broadleaf(self):
        """
        SPRING BROADLEAF FLUSH trigger:
        GDD50 reaching warm-season broadleaf emergence thresholds.
        Active Apr-Jun.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["spring_broadleaf"]["season_months"]:
            return None

        latest = self._get_latest_data()
        if not latest:
            return None

        cum_gdd50 = latest[7]
        if cum_gdd50 is None:
            return None

        trigger = TRIGGERS["spring_broadleaf"]
        conds = trigger["conditions"]
        year = datetime.now().year

        if cum_gdd50 >= conds["gdd50_emergence"]:
            alert_key = f"spring_broadleaf_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: Spring Broadleaf Flush\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n"
                f"Emergence Threshold: {conds['gdd50_emergence']}\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}\n\n"
                f"Spray POST before GDD50 reaches {conds['gdd50_spray_by']}."
            )
            return (alert_key, msg)

        return None

    def check_perennial_fall(self):
        """
        PERENNIAL FALL ROSETTE trigger:
        Temps cooling, perennials forming rosettes for winter.
        Active Sep-Nov.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["perennial_fall"]["season_months"]:
            return None

        recent = self._get_recent_data(10)
        if len(recent) < 7:
            return None

        trigger = TRIGGERS["perennial_fall"]
        conds = trigger["conditions"]

        # Check 7-day avg temp
        last7_temps = [r[3] for r in recent[-7:] if r[3] is not None]
        if not last7_temps:
            return None

        avg7 = sum(last7_temps) / len(last7_temps)

        if avg7 <= conds["avg_temp_below"]:
            year = datetime.now().year
            alert_key = f"perennial_fall_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: Perennial Fall Rosette Window\n\n"
                f"7-Day Avg Temp: {avg7:.0f}F\n"
                f"Perennials forming rosettes for winter.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}\n\n"
                f"Best uptake while actively growing. Spray within {trigger['spray_window_days']} days."
            )
            return (alert_key, msg)

        return None

    def check_perennial_spring(self):
        """
        PERENNIAL SPRING ROSETTE trigger:
        Warming temps, perennials resuming growth.
        Active Mar-May.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["perennial_spring"]["season_months"]:
            return None

        recent = self._get_recent_data(14)
        if len(recent) < 7:
            return None

        trigger = TRIGGERS["perennial_spring"]
        conds = trigger["conditions"]

        # Check consecutive warm days above 50F
        consecutive = 0
        max_consecutive = 0
        for row in recent:
            tmean = row[3]
            if tmean is not None and tmean >= conds["warm_day_threshold"]:
                consecutive += 1
                max_consecutive = max(max_consecutive, consecutive)
            else:
                consecutive = 0

        latest = recent[-1]
        cum_gdd50 = latest[7]

        if (max_consecutive >= conds["consecutive_warm_days"]
                and cum_gdd50 is not None and cum_gdd50 >= conds["gdd50_min"]):

            year = datetime.now().year
            alert_key = f"perennial_spring_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"WEED ALERT: Perennial Spring Rosette Window\n\n"
                f"Warm Streak: {max_consecutive} days above {conds['warm_day_threshold']:.0f}F\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}\n\n"
                f"Spray within {trigger['spray_window_days']} days before bolting."
            )
            return (alert_key, msg)

        return None

    # -------------------------------------------------------------------------
    # SMS Sending
    # -------------------------------------------------------------------------

    def send_alert(self, message):
        """Send SMS via Zapier webhook."""
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
            self.logger.info("Alert sent: %s", message[:80])
            return True
        except requests.RequestException as e:
            self.logger.error("Failed to send alert: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------

    def backfill(self):
        """Backfill GDD data from Jan 1 of current year to yesterday."""
        year = datetime.now().year
        start = f"{year}-01-01"
        # Use archive API for dates before ~5 days ago, forecast API for recent
        five_days_ago = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        self.logger.info("Backfilling from %s to %s...", start, five_days_ago)

        data = self.fetch_historical(start, five_days_ago)
        if data:
            rows = self.calculate_and_store(data)
            self.logger.info("Backfilled %d days from archive.", rows)
        else:
            self.logger.error("Failed to fetch historical data for backfill.")

        # Also fetch recent + forecast
        data = self.fetch_recent_and_forecast()
        if data:
            rows = self.calculate_and_store(data)
            self.logger.info("Added %d days from recent/forecast.", rows)

    def run_daily_check(self):
        """Daily check: fetch weather, calculate GDD, check triggers, send alerts."""
        self.logger.info("Starting daily GDD weed alert check...")

        # Fetch recent weather + forecast
        data = self.fetch_recent_and_forecast()
        if data is None:
            self.logger.error("Could not fetch weather data.")
            return []

        self.calculate_and_store(data)

        # Check if we have enough data (need backfill if this is first run)
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM daily_weather")
        count = c.fetchone()[0]
        if count < 30:
            self.logger.info("Sparse data (%d days). Running backfill first...", count)
            self.backfill()

        # Run all trigger checks
        all_checks = [
            self.check_fall_pre,
            self.check_late_winter_post,
            self.check_spring_pre,
            self.check_spring_broadleaf,
            self.check_perennial_fall,
            self.check_perennial_spring,
        ]

        sent_messages = []
        for check_fn in all_checks:
            result = check_fn()
            if result:
                alert_key, message = result
                self.logger.info("Trigger fired: %s", alert_key)
                success = self.send_alert(message)
                if success:
                    self._record_alert(alert_key, message)
                    sent_messages.append(message)

        if not sent_messages:
            self.logger.info("No weed alert triggers fired today.")

        self.logger.info("Daily check complete. %d alert(s) sent.", len(sent_messages))
        return sent_messages

    def send_test_alerts(self):
        """Send sample test alerts for all trigger types."""
        import time

        latest = self._get_latest_data()
        cum_gdd50 = latest[7] if latest else 0
        cum_gdd32 = latest[8] if latest else 0

        test_messages = [
            (
                f"WEED ALERT: Fall Pre-Emergent Window Open\n\n"
                f"5-Day Avg Temp: 65F (below 70F)\n"
                f"2-Day Rain: 0.48 in\n\n"
                f"Target Weeds:\n"
                f"chickweed, henbit, mustards, Poa annua, deadnettle\n\n"
                f"Action: Apply PRE-emergent on clean soil. Rainfall will activate.\n\n"
                f"Spray within 14 days for best results."
            ),
            (
                f"WEED ALERT: Late Winter Scout & Spray\n\n"
                f"Warm Streak: 6 days above 45F\n"
                f"Cumulative GDD32: {cum_gdd32:.0f}\n\n"
                f"Target Weeds:\n"
                f"winter annual rosettes, chickweed, henbit, shepherd's purse\n\n"
                f"Action: Scout fields. Spot-spray POST while weeds < 6 inches, before bolting.\n\n"
                f"Spray within 21 days while weeds are small."
            ),
            (
                f"WEED ALERT: APPLY PRE-EMERGENT NOW\n\n"
                f"Cumulative GDD50: 155\n"
                f"Apply-By Threshold: 150\n"
                f"Germination at: 200\n\n"
                f"Target Weeds:\n"
                f"crabgrass, foxtail, other warm-season annual grasses\n\n"
                f"Action: Apply PRE-emergent before GDD50 hits 200."
            ),
            (
                f"WEED ALERT: Spring Broadleaf Flush\n\n"
                f"Cumulative GDD50: 320\n"
                f"Emergence Threshold: 300\n\n"
                f"Target Weeds:\n"
                f"lambsquarters, pigweed, ragweed, spotted spurge, groundsel\n\n"
                f"Action: POST-emergent spray while seedlings small (2-6 leaf stage).\n"
                f"7-21 days after emergence.\n\n"
                f"Spray POST before GDD50 reaches 500."
            ),
            (
                f"WEED ALERT: Perennial Fall Rosette Window\n\n"
                f"7-Day Avg Temp: 58F\n"
                f"Perennials forming rosettes for winter.\n\n"
                f"Target Weeds:\n"
                f"dandelion, dock, thistle, plantain, buttercup, blackberry regrowth\n\n"
                f"Action: Target rosettes and active regrowth.\n"
                f"Best uptake while actively growing. Spray within 21 days."
            ),
            (
                f"WEED ALERT: Perennial Spring Rosette Window\n\n"
                f"Warm Streak: 8 days above 50F\n"
                f"Cumulative GDD50: 125\n\n"
                f"Target Weeds:\n"
                f"dandelion, dock, thistle, plantain, buttercup, burdock, bindweed\n\n"
                f"Action: Spray rosettes/regrowth before bolting.\n"
                f"Repeat apps may be needed for perennials.\n\n"
                f"Spray within 21 days before bolting."
            ),
        ]

        for i, msg in enumerate(test_messages):
            print(f"\n--- Test {i+1}/{len(test_messages)} ---")
            print(msg[:60] + "...")
            self.send_alert(msg)
            time.sleep(3)

        print(f"\nAll {len(test_messages)} test alerts sent!")

    def show_status(self):
        """Display current GDD accumulation and trigger status."""
        print(f"\n{'='*60}")
        print(f"  Degree Day Spray for Weeds - GDD-SIS Status")
        print(f"  Location: {LOCATION['name']}")
        print(f"  Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM daily_weather")
        total_days = c.fetchone()[0]
        print(f"  Database: {total_days} days of data\n")

        if total_days == 0:
            print("  No data yet. Run with --backfill first.\n")
            return

        # Current GDD status
        latest = self._get_latest_data()
        if latest:
            print(f"  Latest Data: {latest[0]}")
            print(f"  Temp: {latest[1]:.0f}F low / {latest[2]:.0f}F high / {latest[3]:.0f}F mean")
            print(f"  Precip: {latest[4]:.2f} in")
            print(f"  Daily GDD50: {latest[5]:.1f}  |  Cumulative GDD50: {latest[7]:.0f}")
            print(f"  Daily GDD32: {latest[6]:.1f}  |  Cumulative GDD32: {latest[8]:.0f}")
            print(f"  5-Day Avg Temp: {latest[9]:.1f}F" if latest[9] else "")
            print(f"  2-Day Rain Sum: {latest[10]:.2f} in" if latest[10] else "")
            print()

        # Show recent 10 days
        recent = self._get_recent_data(10)
        print(f"  {'Date':<12} {'Lo':>5} {'Hi':>5} {'Mean':>5} {'Rain':>6} {'GDD50':>6} {'CumGDD50':>9} {'5dAvg':>6}")
        print(f"  {'-'*12} {'-'*5} {'-'*5} {'-'*5} {'-'*6} {'-'*6} {'-'*9} {'-'*6}")
        for row in recent:
            date_str = row[0]
            lo = f"{row[1]:.0f}F" if row[1] else "N/A"
            hi = f"{row[2]:.0f}F" if row[2] else "N/A"
            mean = f"{row[3]:.0f}F" if row[3] else "N/A"
            rain = f"{row[4]:.2f}" if row[4] else "0.00"
            gdd50 = f"{row[5]:.1f}" if row[5] else "0.0"
            cum50 = f"{row[7]:.0f}" if row[7] else "0"
            avg5 = f"{row[9]:.0f}F" if row[9] else "N/A"
            print(f"  {date_str:<12} {lo:>5} {hi:>5} {mean:>5} {rain:>6} {gdd50:>6} {cum50:>9} {avg5:>6}")

        # Show trigger status
        print(f"\n  --- Trigger Thresholds ---")
        cum_gdd50 = latest[7] if latest else 0
        cum_gdd32 = latest[8] if latest else 0

        print(f"\n  Spring PRE (crabgrass/foxtail):")
        print(f"    Current GDD50: {cum_gdd50:.0f}")
        print(f"    Heads-Up:      125 GDD50  {'<-- PASSED' if cum_gdd50 >= 125 else ''}")
        print(f"    Apply-By:      150 GDD50  {'<-- PASSED' if cum_gdd50 >= 150 else ''}")
        print(f"    Germination:   200 GDD50  {'<-- PASSED' if cum_gdd50 >= 200 else ''}")

        print(f"\n  Spring Broadleaf Flush:")
        print(f"    Current GDD50: {cum_gdd50:.0f}")
        print(f"    Emergence:     300 GDD50  {'<-- PASSED' if cum_gdd50 >= 300 else ''}")
        print(f"    Spray-By:      500 GDD50  {'<-- PASSED' if cum_gdd50 >= 500 else ''}")

        print(f"\n  Late Winter Scout (GDD32 > 200 + warm streak):")
        print(f"    Current GDD32: {cum_gdd32:.0f}  {'<-- THRESHOLD MET' if cum_gdd32 >= 200 else ''}")

        if latest and latest[9]:
            avg5 = latest[9]
            print(f"\n  Fall PRE (5-day avg < 70F + rain):")
            print(f"    5-Day Avg Temp: {avg5:.0f}F  {'<-- BELOW 70F' if avg5 <= 70 else ''}")
            rain2 = latest[10] or 0
            print(f"    2-Day Rain:     {rain2:.2f} in {'<-- RAIN TRIGGER MET' if rain2 >= 0.25 else ''}")

        # Show sent alerts
        c.execute("SELECT alert_key, sent_at FROM alerts_sent ORDER BY sent_at DESC")
        sent = c.fetchall()
        if sent:
            print(f"\n  --- Alerts Sent ---")
            for key, ts in sent:
                print(f"    {ts}  {key}")

        print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Degree Day Spray for Weeds Alert - GDD-based weed emergence tracking"
    )
    parser.add_argument("--status", action="store_true", help="Show GDD status and triggers")
    parser.add_argument("--test", action="store_true", help="Send test alerts for all types")
    parser.add_argument("--backfill", action="store_true", help="Backfill GDD from Jan 1")
    args = parser.parse_args()

    agent = GDDWeedAlert()

    if args.backfill:
        agent.backfill()
        agent.show_status()
    elif args.test:
        agent.backfill()  # Ensure we have data first
        agent.send_test_alerts()
    elif args.status:
        # Auto-backfill if empty
        c = agent.conn.cursor()
        c.execute("SELECT COUNT(*) FROM daily_weather")
        if c.fetchone()[0] == 0:
            agent.backfill()
        agent.show_status()
    else:
        agent.run_daily_check()


if __name__ == "__main__":
    main()
