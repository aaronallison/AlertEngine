"""
Degree Day Spray for Weeds Alert
==================================
Monitors Growing Degree Days (GDD) and weather conditions to send
TWO-PHASE alerts for weed management:

  Phase 1 - SPROUTING ALERT: Conditions are right, weed seeds are germinating.
            Includes estimated spray date (7-21 days out based on temps).

  Phase 2 - SPRAY WINDOW ALERT: Time to spray! Fires when the spray window
            arrives (7-21 days after sprouting, faster in warm weeks).

Location: 45.662917, -122.815922 (Portland, OR 97231 / Sauvie Island)
Data Source: Open-Meteo (free, no API key)

How GDD is Calculated:
  daily_mean = (T_max + T_min) / 2
  GDD = max(0, daily_mean - base_temp)
  Cumulative GDD = sum of daily GDD from season start date

Trigger Seasons:
  FALL (Oct-Dec):
    - Winter annual weeds (chickweed, henbit, mustards, Poa annua)
    - Sprouting trigger: avg daily temp drops below 70F + rain arriving
    - Spray: PRE-emergent within 7-14 days on clean soil

  LATE WINTER (Feb-Mar):
    - Winter annuals + rosettes actively growing
    - Sprouting trigger: consecutive warm days > 45F + GDD32 threshold
    - Spray: POST-emergent 7-21 days later while weeds < 6 inches

  SPRING (Mar-May):
    - Warm-season annual grasses (crabgrass, foxtail)
    - Sprouting trigger: GDD50 reaching ~125/150/200
    - Spray: PRE-emergent before 200 GDD50

  LATE SPRING (May-Jun):
    - Warm-season broadleaves (lambsquarters, pigweed, ragweed)
    - Sprouting trigger: GDD50 reaching ~300
    - Spray: POST-emergent 7-21 days after on small seedlings

Database: SQLite (gdd_sis.db) tracks daily GDD accumulation + spray schedule

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

    # Spray schedule: tracks when sprouting was detected so we can
    # send the follow-up "time to spray" alert at the right time
    c.execute("""
        CREATE TABLE IF NOT EXISTS spray_schedule (
            trigger_key TEXT PRIMARY KEY,
            sprouting_date TEXT,
            spray_date_early TEXT,
            spray_date_late TEXT,
            spray_alert_sent INTEGER DEFAULT 0,
            trigger_name TEXT,
            weeds TEXT,
            action TEXT
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

    def _format_date(self, date_str):
        """Format date as 'Monday, Feb 23'."""
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%A, %b %d").replace(" 0", " ")

    def _estimate_spray_date(self, sprouting_date_str, spray_window_days):
        """
        Estimate when to spray based on upcoming temps.
        Warmer weather = faster weed growth = spray sooner (closer to 7 days).
        Cooler weather = slower growth = spray later (closer to 21 days).
        Returns (early_date_str, late_date_str) for spray window.
        """
        sprouting_date = datetime.strptime(sprouting_date_str, "%Y-%m-%d")

        # Look at forecast temps to estimate growth speed
        recent = self._get_recent_data(30)
        if not recent:
            # Default to middle of window
            mid_days = spray_window_days // 2 + 7
            early = sprouting_date + timedelta(days=7)
            late = sprouting_date + timedelta(days=spray_window_days)
            return early.strftime("%Y-%m-%d"), late.strftime("%Y-%m-%d")

        # Get upcoming average temps (use forecast data)
        upcoming_temps = []
        for row in recent:
            row_date = datetime.strptime(row[0], "%Y-%m-%d")
            if row_date >= sprouting_date and row[3] is not None:
                upcoming_temps.append(row[3])

        if not upcoming_temps:
            upcoming_temps = [row[3] for row in recent[-7:] if row[3] is not None]

        avg_temp = sum(upcoming_temps) / len(upcoming_temps) if upcoming_temps else 45.0

        # Warm weeks (avg > 55F): spray in 7-10 days (fast growth)
        # Moderate (45-55F): spray in 10-14 days
        # Cold (< 45F): spray in 14-21 days (slow growth)
        if avg_temp >= 55:
            early_days = 7
            late_days = 12
        elif avg_temp >= 45:
            early_days = 10
            late_days = 16
        else:
            early_days = 14
            late_days = min(21, spray_window_days)

        early = sprouting_date + timedelta(days=early_days)
        late = sprouting_date + timedelta(days=late_days)
        return early.strftime("%Y-%m-%d"), late.strftime("%Y-%m-%d")

    def _schedule_spray(self, trigger_key, sprouting_date, spray_window_days,
                        trigger_name, weeds, action):
        """Record a sprouting event and schedule the follow-up spray alert."""
        early, late = self._estimate_spray_date(sprouting_date, spray_window_days)

        c = self.conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO spray_schedule
            (trigger_key, sprouting_date, spray_date_early, spray_date_late,
             spray_alert_sent, trigger_name, weeds, action)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
        """, (trigger_key, sprouting_date, early, late, trigger_name, weeds, action))
        self.conn.commit()

        return early, late

    def check_spray_windows(self):
        """
        Phase 2: Check if any scheduled spray windows have arrived.
        Returns list of (alert_key, message) tuples.
        """
        alerts = []
        today = datetime.now().strftime("%Y-%m-%d")

        c = self.conn.cursor()
        c.execute("""
            SELECT trigger_key, sprouting_date, spray_date_early, spray_date_late,
                   trigger_name, weeds, action
            FROM spray_schedule
            WHERE spray_alert_sent = 0 AND spray_date_early <= ?
        """, (today,))

        for row in c.fetchall():
            trigger_key, sprout_date, early, late, name, weeds, action = row
            alert_key = f"spray_{trigger_key}"

            if self._is_alert_sent(alert_key):
                continue

            days_since = (datetime.now() - datetime.strptime(sprout_date, "%Y-%m-%d")).days
            late_date = datetime.strptime(late, "%Y-%m-%d")
            days_left = (late_date - datetime.now()).days

            if days_left < 0:
                urgency = "OVERDUE"
            elif days_left <= 3:
                urgency = "URGENT"
            else:
                urgency = "READY"

            msg = (
                f"SPRAY WINDOW: {name}\n\n"
                f"Status: {urgency} - {days_since} days since sprouting\n"
                f"Spray by: {self._format_date(late)}\n\n"
                f"Target Weeds:\n"
                f"{weeds}\n\n"
                f"Action: {action}\n\n"
                f"Treat while weeds are small (2-6 leaf, under 6 inches)."
            )
            alerts.append((alert_key, msg))

            # Mark spray alert as sent
            c.execute("UPDATE spray_schedule SET spray_alert_sent = 1 WHERE trigger_key = ?",
                      (trigger_key,))
            self.conn.commit()

        return alerts

    def check_fall_pre(self):
        """
        FALL PRE-EMERGENT SPROUTING trigger:
        5-day avg temp drops below 70F AND 2-day rain sum >= 0.25 inches.
        Active Sep-Dec. Sends sprouting alert + schedules spray follow-up.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["fall_pre"]["season_months"]:
            return None

        recent = self._get_recent_data(7)
        if len(recent) < 5:
            return None

        latest = recent[-1]
        date_str = latest[0]
        avg_temp_5day = latest[9]
        rain_2day = latest[10]

        trigger = TRIGGERS["fall_pre"]
        conds = trigger["conditions"]

        if (avg_temp_5day is not None and avg_temp_5day <= conds["avg_temp_below"]
                and rain_2day is not None and rain_2day >= conds["rain_2day_min"]):

            year = datetime.now().year
            alert_key = f"fall_pre_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            # Schedule spray follow-up
            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"fall_pre_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"], trigger["action"]
            )

            msg = (
                f"SPROUTING ALERT: Fall Weed Seeds Germinating\n\n"
                f"5-Day Avg Temp: {avg_temp_5day:.0f}F (below {conds['avg_temp_below']:.0f}F)\n"
                f"2-Day Rain: {rain_2day:.2f} in\n\n"
                f"Winter annual weed seeds are germinating NOW.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent when it's time to treat."
            )
            return (alert_key, msg)

        return None

    def check_late_winter_post(self):
        """
        LATE WINTER SPROUTING trigger:
        5+ consecutive days with avg temp > 45F AND cumulative GDD32 >= 200.
        Active Feb-Mar. Winter annuals resuming growth.
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
            alert_key = f"late_winter_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            # Schedule spray follow-up
            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"late_winter_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"], trigger["action"]
            )

            msg = (
                f"SPROUTING ALERT: Winter Weeds Resuming Growth\n\n"
                f"Warm Streak: {max_consecutive} days above {conds['warm_day_threshold']:.0f}F\n"
                f"Cumulative GDD32: {cum_gdd32:.0f}\n\n"
                f"Winter annual rosettes are actively growing.\n"
                f"Weeds emerging and building leaf area.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent when it's time to treat."
            )
            return (alert_key, msg)

        return None

    def check_spring_pre(self):
        """
        SPRING PRE-EMERGENT trigger (3-tier):
        GDD50 approaching crabgrass germination threshold.
        125 = Heads-up, 150 = Apply PRE now, 200 = Germination started (sprouting alert).
        Active Mar-May.
        """
        current_month = datetime.now().month
        if current_month not in TRIGGERS["spring_pre"]["season_months"]:
            return None

        latest = self._get_latest_data()
        if not latest:
            return None

        cum_gdd50 = latest[7]
        if cum_gdd50 is None:
            return None

        trigger = TRIGGERS["spring_pre"]
        conds = trigger["conditions"]
        year = datetime.now().year

        # Tier 3: Germination started - this IS the sprouting alert
        if cum_gdd50 >= conds["gdd50_germination"]:
            alert_key = f"spring_pre_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"spring_pre_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"],
                "POST-emergent spray on small seedlings (2-6 leaf stage)."
            )

            msg = (
                f"SPROUTING ALERT: Crabgrass & Foxtail Germinating!\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f} (threshold: {conds['gdd50_germination']})\n\n"
                f"Warm-season grass seeds are sprouting NOW.\n"
                f"PRE-emergent window has passed.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date (POST-emergent):\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent."
            )
            return (alert_key, msg)

        # Tier 2: Apply PRE now (before germination)
        elif cum_gdd50 >= conds["gdd50_apply_by"]:
            alert_key = f"spring_pre_applyby_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"SPRAY WINDOW: APPLY PRE-EMERGENT NOW\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n"
                f"Apply-By Threshold: {conds['gdd50_apply_by']}\n"
                f"Germination at: {conds['gdd50_germination']}\n\n"
                f"Weed seeds have NOT sprouted yet.\n"
                f"Apply PRE-emergent NOW to prevent germination.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Action: {trigger['action']}"
            )
            return (alert_key, msg)

        # Tier 1: Heads-up (plan your application)
        elif cum_gdd50 >= conds["gdd50_headsup"]:
            alert_key = f"spring_pre_headsup_{year}"
            if self._is_alert_sent(alert_key):
                return None
            msg = (
                f"HEADS UP: Spring PRE-Emergent Coming Soon\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n"
                f"Apply-By: {conds['gdd50_apply_by']} GDD50\n"
                f"Germination: {conds['gdd50_germination']} GDD50\n\n"
                f"Soil warming. Weed seeds NOT sprouting yet.\n"
                f"Start planning PRE-emergent application.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}"
            )
            return (alert_key, msg)

        return None

    def check_spring_broadleaf(self):
        """
        SPRING BROADLEAF FLUSH SPROUTING trigger:
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
            alert_key = f"spring_broadleaf_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"spring_broadleaf_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"], trigger["action"]
            )

            msg = (
                f"SPROUTING ALERT: Spring Broadleaf Weeds Emerging\n\n"
                f"Cumulative GDD50: {cum_gdd50:.0f} (threshold: {conds['gdd50_emergence']})\n\n"
                f"Warm-season broadleaf seeds are sprouting NOW.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent.\n"
                f"Spray POST before GDD50 reaches {conds['gdd50_spray_by']}."
            )
            return (alert_key, msg)

        return None

    def check_perennial_fall(self):
        """
        PERENNIAL FALL ROSETTE SPROUTING trigger:
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

        last7_temps = [r[3] for r in recent[-7:] if r[3] is not None]
        if not last7_temps:
            return None

        avg7 = sum(last7_temps) / len(last7_temps)

        if avg7 <= conds["avg_temp_below"]:
            year = datetime.now().year
            alert_key = f"perennial_fall_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"perennial_fall_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"], trigger["action"]
            )

            msg = (
                f"SPROUTING ALERT: Perennial Rosettes Forming (Fall)\n\n"
                f"7-Day Avg Temp: {avg7:.0f}F\n"
                f"Perennials are forming rosettes for winter.\n"
                f"Active growth window before dormancy.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent."
            )
            return (alert_key, msg)

        return None

    def check_perennial_spring(self):
        """
        PERENNIAL SPRING ROSETTE SPROUTING trigger:
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
            alert_key = f"perennial_spring_sprout_{year}"
            if self._is_alert_sent(alert_key):
                return None

            today = datetime.now().strftime("%Y-%m-%d")
            early, late = self._schedule_spray(
                f"perennial_spring_{year}", today, trigger["spray_window_days"],
                trigger["name"], trigger["weeds"], trigger["action"]
            )

            msg = (
                f"SPROUTING ALERT: Perennial Weeds Resuming Growth (Spring)\n\n"
                f"Warm Streak: {max_consecutive} days above {conds['warm_day_threshold']:.0f}F\n"
                f"Cumulative GDD50: {cum_gdd50:.0f}\n\n"
                f"Perennial weeds breaking dormancy and actively growing.\n"
                f"Rosettes forming - spray before bolting.\n\n"
                f"Target Weeds:\n"
                f"{trigger['weeds']}\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(early)} - {self._format_date(late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent."
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

        # Phase 1: Check all SPROUTING triggers
        sprouting_checks = [
            self.check_fall_pre,
            self.check_late_winter_post,
            self.check_spring_pre,
            self.check_spring_broadleaf,
            self.check_perennial_fall,
            self.check_perennial_spring,
        ]

        sent_messages = []
        for check_fn in sprouting_checks:
            result = check_fn()
            if result:
                alert_key, message = result
                self.logger.info("Sprouting trigger fired: %s", alert_key)
                success = self.send_alert(message)
                if success:
                    self._record_alert(alert_key, message)
                    sent_messages.append(message)

        # Phase 2: Check if any scheduled SPRAY WINDOWS have arrived
        spray_alerts = self.check_spray_windows()
        for alert_key, message in spray_alerts:
            self.logger.info("Spray window trigger fired: %s", alert_key)
            success = self.send_alert(message)
            if success:
                self._record_alert(alert_key, message)
                sent_messages.append(message)

        if not sent_messages:
            self.logger.info("No weed alert triggers fired today.")

        self.logger.info("Daily check complete. %d alert(s) sent.", len(sent_messages))
        return sent_messages

    def send_test_alerts(self):
        """Send sample test alerts showing both SPROUTING and SPRAY WINDOW phases."""
        import time

        today = datetime.now()
        spray_early = (today + timedelta(days=10)).strftime("%Y-%m-%d")
        spray_late = (today + timedelta(days=16)).strftime("%Y-%m-%d")

        latest = self._get_latest_data()
        cum_gdd32 = latest[8] if latest else 450

        test_messages = [
            # --- SPROUTING ALERTS (Phase 1) ---
            (
                f"SPROUTING ALERT: Fall Weed Seeds Germinating\n\n"
                f"5-Day Avg Temp: 65F (below 70F)\n"
                f"2-Day Rain: 0.48 in\n\n"
                f"Winter annual weed seeds are germinating NOW.\n\n"
                f"Target Weeds:\n"
                f"chickweed, henbit, mustards, Poa annua, deadnettle\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(spray_early)} - {self._format_date(spray_late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent when it's time to treat."
            ),
            (
                f"SPROUTING ALERT: Winter Weeds Resuming Growth\n\n"
                f"Warm Streak: 6 days above 45F\n"
                f"Cumulative GDD32: {cum_gdd32:.0f}\n\n"
                f"Winter annual rosettes are actively growing.\n"
                f"Weeds emerging and building leaf area.\n\n"
                f"Target Weeds:\n"
                f"winter annual rosettes, chickweed, henbit, shepherd's purse\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(spray_early)} - {self._format_date(spray_late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent when it's time to treat."
            ),
            (
                f"SPROUTING ALERT: Crabgrass & Foxtail Germinating!\n\n"
                f"Cumulative GDD50: 205 (threshold: 200)\n\n"
                f"Warm-season grass seeds are sprouting NOW.\n"
                f"PRE-emergent window has passed.\n\n"
                f"Target Weeds:\n"
                f"crabgrass, foxtail, other warm-season annual grasses\n\n"
                f"Estimated Spray Date (POST-emergent):\n"
                f"{self._format_date(spray_early)} - {self._format_date(spray_late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent."
            ),
            (
                f"SPROUTING ALERT: Spring Broadleaf Weeds Emerging\n\n"
                f"Cumulative GDD50: 320 (threshold: 300)\n\n"
                f"Warm-season broadleaf seeds are sprouting NOW.\n\n"
                f"Target Weeds:\n"
                f"lambsquarters, pigweed, ragweed, spotted spurge, groundsel\n\n"
                f"Estimated Spray Date:\n"
                f"{self._format_date(spray_early)} - {self._format_date(spray_late)}\n\n"
                f"A follow-up SPRAY WINDOW alert will be sent."
            ),

            # --- SPRAY WINDOW ALERTS (Phase 2) ---
            (
                f"SPRAY WINDOW: FALL PRE-EMERGENT\n\n"
                f"Status: READY - 12 days since sprouting\n"
                f"Spray by: {self._format_date(spray_late)}\n\n"
                f"Target Weeds:\n"
                f"chickweed, henbit, mustards, Poa annua, deadnettle\n\n"
                f"Action: Apply PRE-emergent on clean soil. Rainfall will activate.\n\n"
                f"Treat while weeds are small (2-6 leaf, under 6 inches)."
            ),
            (
                f"SPRAY WINDOW: LATE WINTER SCOUT & SPRAY\n\n"
                f"Status: READY - 14 days since sprouting\n"
                f"Spray by: {self._format_date(spray_late)}\n\n"
                f"Target Weeds:\n"
                f"winter annual rosettes, chickweed, henbit, shepherd's purse\n\n"
                f"Action: Scout fields. Spot-spray POST while weeds < 6 inches, before bolting.\n\n"
                f"Treat while weeds are small (2-6 leaf, under 6 inches)."
            ),
        ]

        for i, msg in enumerate(test_messages):
            label = "SPROUTING" if i < 4 else "SPRAY WINDOW"
            print(f"\n--- Test {i+1}/{len(test_messages)} ({label}) ---")
            print(msg[:70] + "...")
            self.send_alert(msg)
            time.sleep(3)

        print(f"\nAll {len(test_messages)} test alerts sent!")
        print("4 SPROUTING alerts + 2 SPRAY WINDOW alerts")

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
