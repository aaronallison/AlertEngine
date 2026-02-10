"""
Seasonal Schedule Alert Agent
==============================
Sends a text message on the 20th of the month before each season starts,
listing the tasks for the upcoming 3 months from the Seasonal Schedule.

Schedule:
  - Aug 20  -> Fall alert   (September, October, November)
  - Nov 20  -> Winter alert (December, January, February)
  - Feb 20  -> Spring alert (March, April, May)
  - May 20  -> Summer alert (June, July, August)

Usage:
  python seasonal_schedule_alert.py            # Check if today is an alert day, send if so
  python seasonal_schedule_alert.py --test     # Send the next upcoming season's alert as a test
  python seasonal_schedule_alert.py --all      # Send all 4 seasonal alerts (for testing)
  python seasonal_schedule_alert.py --status   # Show all schedules without sending
"""

import sys
import os
import json
import logging
from datetime import datetime

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

ZAPIER_CONFIG = {
    "webhook_url": os.environ.get("ZAPIER_WEBHOOK_URL", ""),
    "phone_number": os.environ.get("ALERT_PHONE_NUMBER", ""),
}

ALERT_DAY = 20  # Send alerts on the 20th of the trigger month

# =============================================================================
# SEASONAL SCHEDULE DATA (from Seasonal Schedule SIS 2.7.2026.xlsx)
# =============================================================================

SEASONAL_SCHEDULE = {
    "Fall": {
        "trigger_month": 8,   # August 20th
        "months": [
            {
                "name": "September",
                "rainfall": "2 In",
                "tasks": [
                    "Spread Manure",
                    "Mow Hay Fields",
                    "Spray All Fields",
                    "Spray all roads and sides of roads",
                    "Restock the Propane tanks",
                    "Fertilize & lime the fields",
                ],
            },
            {
                "name": "October",
                "rainfall": "3.6 in",
                "tasks": [
                    "Hay Tarps - Hay in Barn Covered",
                    "Leaves clean up",
                    "Spray All Fields",
                    "Spray all roads and sides of roads",
                    "Freeze Prep - Heat Tape all Water",
                    "Move Mobile Structures to Dry lot locations",
                ],
            },
            {
                "name": "November",
                "rainfall": "8 In +",
                "tasks": [
                    "Hay Tarps - Hay in Barn Covered",
                    "Leaves clean up",
                    "Blowing the roads with big blower",
                    "Clean all catch basins",
                    "Test run the generator",
                    "Clean Gutters Barn 1, Tool Shop & Apartment",
                ],
            },
        ],
    },
    "Winter": {
        "trigger_month": 11,  # November 20th
        "months": [
            {
                "name": "December",
                "rainfall": "8 In +",
                "tasks": [
                    "Spread Manure, If Ground Freezes",
                    "Mow Fields if Dry enough",
                    "Blowing the roads with big blower",
                    "Vehicle Maint: Oils, Grease, Antifreeze, Fixes",
                ],
            },
            {
                "name": "January",
                "rainfall": "8 In +",
                "tasks": [
                    "Spread Manure, If Ground Freezes",
                    "Mow Fields if Dry enough",
                    "Vehicle Maint: Oils, Grease, Antifreeze, Fixes",
                ],
            },
            {
                "name": "February",
                "rainfall": "4.6 In",
                "tasks": [
                    "Spread Manure, If Ground Freezes",
                    "Mow Fields if Dry enough",
                    "Vehicle Maint: Oils, Grease, Antifreeze, Fixes",
                ],
            },
        ],
    },
    "Spring": {
        "trigger_month": 2,   # February 20th
        "months": [
            {
                "name": "March",
                "rainfall": "4.2 in",
                "tasks": [
                    "Manure Drops: David Fazzio / Oak Island",
                ],
            },
            {
                "name": "April",
                "rainfall": "3.9 in",
                "tasks": [
                    "Call Steve - hay fields coming up in May",
                    "Fertilize Hay Fields",
                    "Spray all roads and sides of roads",
                    "Mowing - 4-5 times depending on sun",
                    "Prep barn for new hay bales coming in",
                ],
            },
            {
                "name": "May",
                "rainfall": "3.2 in",
                "tasks": [
                    "Hay field cut and bailed",
                    "Spray All Fields",
                    "Mowing - 4-5 times depending on sun",
                    "Plow and Seed new field areas when dry",
                ],
            },
        ],
    },
    "Summer": {
        "trigger_month": 5,   # May 20th
        "months": [
            {
                "name": "June",
                "rainfall": "2.6 In",
                "tasks": [
                    "Spread Manure - 2 Full Days / Winter Pile Up",
                    "Spray All Fields",
                ],
            },
            {
                "name": "July",
                "rainfall": "0.3 In",
                "tasks": [
                    "Spread Manure - 2 Full Days",
                ],
            },
            {
                "name": "August",
                "rainfall": "0.6 In",
                "tasks": [
                    "Spread Manure - 2 Full Days",
                ],
            },
        ],
    },
}


# =============================================================================
# SEASONAL SCHEDULE ALERT AGENT
# =============================================================================

class SeasonalScheduleAlert:
    """Sends seasonal schedule text alerts via Zapier webhook."""

    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = self._setup_logging()

        if not REQUESTS_AVAILABLE:
            self.logger.error("Cannot run without 'requests' library.")
            sys.exit(1)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SeasonalScheduleAlert/1.0',
            'Accept': 'application/json',
        })

        self.state = self._load_state()

    def _setup_logging(self):
        logger = logging.getLogger("SeasonalScheduleAlert")
        logger.setLevel(logging.DEBUG)
        if logger.handlers:
            return logger

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

        log_path = os.path.join(self.script_dir, "seasonal_schedule_alert.log")
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
    # State management (deduplication)
    # -------------------------------------------------------------------------

    def _state_path(self):
        return os.path.join(self.script_dir, "seasonal_schedule_state.json")

    def _load_state(self):
        path = self._state_path()
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"sent_alerts": {}}

    def _save_state(self):
        try:
            with open(self._state_path(), 'w') as f:
                json.dump(self.state, f, indent=2)
        except IOError as e:
            self.logger.error("Could not save state: %s", e)

    def _is_already_sent(self, alert_key):
        return alert_key in self.state.get("sent_alerts", {})

    def _record_sent(self, alert_key):
        self.state.setdefault("sent_alerts", {})[alert_key] = datetime.now().isoformat()
        self._save_state()

    # -------------------------------------------------------------------------
    # Message formatting
    # -------------------------------------------------------------------------

    def build_message(self, season_name):
        """Build the text message body for a season."""
        season = SEASONAL_SCHEDULE[season_name]
        lines = [f"Seasonal Schedule Alert - {season_name} Schedule", ""]

        for month_data in season["months"]:
            lines.append(f"{month_data['name']} - {month_data['rainfall']}")
            lines.append("")
            for task in month_data["tasks"]:
                lines.append(task)
            lines.append("")
            lines.append("")

        return "\n".join(lines).rstrip()

    # -------------------------------------------------------------------------
    # Sending
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
            self.logger.info("Alert sent: %s", message[:60])
            return True
        except requests.RequestException as e:
            self.logger.error("Failed to send: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Check & send logic
    # -------------------------------------------------------------------------

    def get_season_for_today(self):
        """If today is the 20th of a trigger month, return the season name."""
        today = datetime.now()
        if today.day != ALERT_DAY:
            return None

        for season_name, season_data in SEASONAL_SCHEDULE.items():
            if today.month == season_data["trigger_month"]:
                return season_name

        return None

    def get_next_season(self):
        """Return the next upcoming season based on current date."""
        today = datetime.now()
        current_month = today.month

        # Order of trigger months through the year
        season_order = [
            ("Spring", 2),
            ("Summer", 5),
            ("Fall", 8),
            ("Winter", 11),
        ]

        # Find the next trigger month
        for season_name, trigger_month in season_order:
            if current_month <= trigger_month:
                return season_name

        # If past November, next is Spring (February)
        return "Spring"

    def run_check(self):
        """Check if today is an alert day and send if not already sent."""
        season = self.get_season_for_today()

        if season is None:
            today = datetime.now()
            self.logger.info(
                "Today is %s - not a seasonal alert day (alerts go out on the 20th of Feb/May/Aug/Nov).",
                today.strftime("%B %d")
            )
            return None

        # Build dedup key for this season + year
        year = datetime.now().year
        alert_key = f"seasonal_{season.lower()}_{year}"

        if self._is_already_sent(alert_key):
            self.logger.info("Seasonal alert '%s' already sent this year. Skipping.", alert_key)
            return None

        message = self.build_message(season)
        self.logger.info("Sending %s seasonal schedule alert...", season)
        success = self.send_alert(message)

        if success:
            self._record_sent(alert_key)
            self.logger.info("Successfully sent %s schedule alert!", season)
            return message
        else:
            self.logger.error("Failed to send %s schedule alert.", season)
            return None

    # -------------------------------------------------------------------------
    # CLI modes
    # -------------------------------------------------------------------------

    def send_test(self):
        """Send the next upcoming season's alert as a test."""
        season = self.get_next_season()
        message = self.build_message(season)
        print(f"\n--- Sending TEST: {season} Schedule ---\n")
        print(message)
        print("\n--- Sending via Zapier... ---\n")
        success = self.send_alert(message)
        if success:
            print("Test sent successfully!")
        else:
            print("Failed to send test.")
        return success

    def send_all_tests(self):
        """Send all 4 seasonal alerts for testing."""
        import time
        for season in ["Fall", "Winter", "Spring", "Summer"]:
            message = self.build_message(season)
            print(f"\n--- Sending: {season} Schedule ---")
            self.send_alert(message)
            time.sleep(3)  # Space out the texts
        print("\nAll 4 seasonal alerts sent!")

    def show_status(self):
        """Display all seasonal schedules and alert state."""
        print(f"\n{'='*60}")
        print(f"  Seasonal Schedule Alert - Status")
        print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        print(f"  Alert Schedule:")
        print(f"    Feb 20 -> Spring (March, April, May)")
        print(f"    May 20 -> Summer (June, July, August)")
        print(f"    Aug 20 -> Fall   (September, October, November)")
        print(f"    Nov 20 -> Winter (December, January, February)")
        print()

        # Show what would trigger today
        season_today = self.get_season_for_today()
        if season_today:
            print(f"  TODAY IS ALERT DAY: {season_today} schedule would be sent!")
        else:
            next_season = self.get_next_season()
            print(f"  Next alert: {next_season} schedule")
        print()

        # Show each season's message
        for season in ["Spring", "Summer", "Fall", "Winter"]:
            print(f"  {'-'*50}")
            msg = self.build_message(season)
            for line in msg.split("\n"):
                print(f"  {line}")
            print()

        # Show sent history
        sent = self.state.get("sent_alerts", {})
        if sent:
            print(f"  {'='*50}")
            print(f"  Recently Sent Alerts:")
            for key, ts in sorted(sent.items(), key=lambda x: x[1], reverse=True):
                print(f"    {ts}  {key}")
        print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Seasonal Schedule Alert - Text reminders for upcoming seasonal tasks"
    )
    parser.add_argument("--test", action="store_true", help="Send next season's alert as test")
    parser.add_argument("--all", action="store_true", help="Send all 4 seasonal alerts (testing)")
    parser.add_argument("--status", action="store_true", help="Show schedules and state")
    args = parser.parse_args()

    agent = SeasonalScheduleAlert()

    if args.test:
        agent.send_test()
    elif args.all:
        agent.send_all_tests()
    elif args.status:
        agent.show_status()
    else:
        agent.run_check()


if __name__ == "__main__":
    main()
