# ============================================================
# India Data Platform
# File    : spark_jobs/silver_to_gold.py
# Purpose : Read clean silver data, generate business insights
#           for Gold layer — flood risk, water scarcity risk,
#           heat alerts, city rankings, state summaries.
#           This data feeds directly into Power BI dashboard.
# Author  : Kevin Josh
# ============================================================

import csv
import json
import logging
import os
from datetime import datetime, date

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

SILVER_FOLDER = "silver_data"
GOLD_FOLDER   = "gold_data"

# ── Business Rules ──────────────────────────────────────────
# These thresholds are based on IMD (India Meteorological
# Department) guidelines. In production these would come
# from a config file or database — not hardcoded.

FLOOD_RAIN_MM        = 5.0    # rainfall above this = flood risk
FLOOD_HUMIDITY_PCT   = 85.0   # humidity above this = flood risk

SCARCITY_TEMP_C      = 35.0   # temp above this = scarcity risk
SCARCITY_RAIN_MM     = 0.0    # zero rainfall = scarcity risk
SCARCITY_HUMIDITY    = 40.0   # humidity below this = scarcity risk

HEAT_ALERT_TEMP_C    = 38.0   # temp above this = heat alert


# ============================================================
# RISK CALCULATION FUNCTIONS
# ============================================================

def calculate_flood_risk(rainfall: float, humidity: float) -> str:
    """
    Flood risk based on rainfall and humidity.
    Returns risk level as a string.

    In interviews: explain why we use TWO conditions —
    high rainfall alone does not mean flood if ground absorbs it.
    High humidity means ground is already saturated.
    Both together = real flood risk.
    """
    if rainfall > FLOOD_RAIN_MM and humidity > FLOOD_HUMIDITY_PCT:
        if rainfall > 50:
            return "CRITICAL"
        elif rainfall > 20:
            return "HIGH"
        else:
            return "MEDIUM"
    elif humidity > FLOOD_HUMIDITY_PCT:
        return "LOW"
    else:
        return "NONE"


def calculate_water_scarcity_risk(
    temp: float,
    rainfall: float,
    humidity: float
) -> str:
    """
    Water scarcity risk based on temperature, rainfall, humidity.
    High heat + no rain + dry air = water stress.
    """
    if (temp > SCARCITY_TEMP_C and
            rainfall <= SCARCITY_RAIN_MM and
            humidity < SCARCITY_HUMIDITY):
        if temp > 45:
            return "CRITICAL"
        elif temp > 40:
            return "HIGH"
        else:
            return "MEDIUM"
    elif temp > SCARCITY_TEMP_C and rainfall <= SCARCITY_RAIN_MM:
        return "LOW"
    else:
        return "NONE"


def calculate_heat_alert(temp: float) -> str:
    """
    Heat alert level based on temperature alone.
    """
    if temp >= 45:
        return "EXTREME"
    elif temp >= 40:
        return "SEVERE"
    elif temp >= HEAT_ALERT_TEMP_C:
        return "MODERATE"
    else:
        return "NONE"


def calculate_cyclone_readiness(
    wind_speed: float,
    rainfall: float,
    humidity: float
) -> str:
    """
    Cyclone READINESS indicator — not a confirmed cyclone.
    We only have city-level data, not pressure systems.
    This flags cities that should be monitored closely.
    High wind + high rain + high humidity = watch zone.
    """
    score = 0
    if wind_speed > 60:
        score += 3
    elif wind_speed > 40:
        score += 2
    elif wind_speed > 20:
        score += 1

    if rainfall > 20:
        score += 2
    elif rainfall > 10:
        score += 1

    if humidity > 90:
        score += 1

    if score >= 5:
        return "WATCH"
    elif score >= 3:
        return "MONITOR"
    else:
        return "NORMAL"


# ============================================================
# DATA READING
# ============================================================

def read_silver_file(process_date: str) -> list:
    """
    Reads the clean silver CSV for a given date.
    """
    file_path = os.path.join(
        SILVER_FOLDER,
        "weather_cleaned",
        f"fetch_date={process_date}",
        "data.csv"
    )

    if not os.path.exists(file_path):
        logger.error(f"Silver file not found: {file_path}")
        logger.error("Run bronze_to_silver.py first")
        return []

    logger.info(f"Reading silver file: {file_path}")

    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric strings back to numbers
            row["temperature_c"]  = float(row["temperature_c"])
            row["humidity_pct"]   = float(row["humidity_pct"])
            row["rainfall_mm"]    = float(row["rainfall_mm"])
            row["wind_speed_kmh"] = float(row["wind_speed_kmh"])
            records.append(row)

    logger.info(f"Loaded {len(records)} silver records")
    return records


# ============================================================
# GOLD LAYER GENERATORS
# Each function produces one Gold table
# ============================================================

def generate_city_alerts(records: list) -> list:
    """
    Gold Table 1 — City level alerts.
    One row per city with all risk indicators.
    This is the main table for the Power BI dashboard map.
    """
    alerts = []

    for rec in records:
        city      = rec["city"]
        state     = rec["state"]
        temp      = rec["temperature_c"]
        humidity  = rec["humidity_pct"]
        rainfall  = rec["rainfall_mm"]
        wind      = rec["wind_speed_kmh"]

        flood_risk    = calculate_flood_risk(rainfall, humidity)
        scarcity_risk = calculate_water_scarcity_risk(temp, rainfall, humidity)
        heat_alert    = calculate_heat_alert(temp)
        cyclone_watch = calculate_cyclone_readiness(wind, rainfall, humidity)

        # Overall risk — highest of all risks
        risk_levels = {
            "CRITICAL": 5,
            "EXTREME":  4,
            "SEVERE":   4,
            "HIGH":     3,
            "WATCH":    3,
            "MEDIUM":   2,
            "MONITOR":  2,
            "LOW":      1,
            "MODERATE": 1,
            "NONE":     0,
        }

        max_risk = max(
            risk_levels.get(flood_risk, 0),
            risk_levels.get(scarcity_risk, 0),
            risk_levels.get(heat_alert, 0),
            risk_levels.get(cyclone_watch, 0),
        )

        overall_risk = (
            "CRITICAL" if max_risk >= 5 else
            "HIGH"     if max_risk >= 3 else
            "MEDIUM"   if max_risk == 2 else
            "LOW"      if max_risk == 1 else
            "NORMAL"
        )

        alert = {
            "city":              city,
            "state":             state,
            "temperature_c":     temp,
            "humidity_pct":      humidity,
            "rainfall_mm":       rainfall,
            "wind_speed_kmh":    wind,
            "flood_risk":        flood_risk,
            "water_scarcity":    scarcity_risk,
            "heat_alert":        heat_alert,
            "cyclone_watch":     cyclone_watch,
            "overall_risk":      overall_risk,
            "fetch_date":        rec["fetch_date"],
            "processed_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        alerts.append(alert)

        # Log any city that is not NORMAL/NONE
        if overall_risk != "NORMAL":
            logger.warning(
                f"  ⚠️  {city}: overall={overall_risk} | "
                f"flood={flood_risk} | scarcity={scarcity_risk} | "
                f"heat={heat_alert} | cyclone={cyclone_watch}"
            )
        else:
            logger.info(
                f"  ✅ {city}: NORMAL | "
                f"temp={temp}°C | rain={rainfall}mm"
            )

    return alerts


def generate_state_summary(records: list, process_date: str) -> list:
    """
    Gold Table 2 — State level aggregations.
    Groups cities by state and calculates averages.
    In real Spark: df.groupBy('state').agg(avg('temperature'))
    """
    # Group records by state
    state_groups = {}
    for rec in records:
        state = rec["state"]
        if state not in state_groups:
            state_groups[state] = []
        state_groups[state].append(rec)

    summaries = []
    for state, cities in state_groups.items():
        avg_temp     = round(sum(r["temperature_c"]  for r in cities) / len(cities), 2)
        avg_humidity = round(sum(r["humidity_pct"]   for r in cities) / len(cities), 2)
        total_rain   = round(sum(r["rainfall_mm"]    for r in cities), 2)
        max_temp     = round(max(r["temperature_c"]  for r in cities), 2)
        min_temp     = round(min(r["temperature_c"]  for r in cities), 2)
        city_count   = len(cities)

        summaries.append({
            "state":           state,
            "city_count":      city_count,
            "avg_temp_c":      avg_temp,
            "max_temp_c":      max_temp,
            "min_temp_c":      min_temp,
            "avg_humidity_pct":avg_humidity,
            "total_rain_mm":   total_rain,
            "fetch_date":      process_date,
            "processed_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    return summaries


def generate_daily_rankings(records: list, process_date: str) -> list:
    """
    Gold Table 3 — Daily city rankings.
    Ranks cities by temperature, humidity, rainfall.
    This powers the leaderboard in Power BI dashboard.
    """
    sorted_by_temp     = sorted(records, key=lambda x: x["temperature_c"], reverse=True)
    sorted_by_humidity = sorted(records, key=lambda x: x["humidity_pct"],  reverse=True)
    sorted_by_rain     = sorted(records, key=lambda x: x["rainfall_mm"],   reverse=True)

    rankings = []
    for rank, rec in enumerate(sorted_by_temp, start=1):
        city = rec["city"]
        rankings.append({
            "rank":          rank,
            "city":          city,
            "state":         rec["state"],
            "temperature_c": rec["temperature_c"],
            "humidity_pct":  rec["humidity_pct"],
            "rainfall_mm":   rec["rainfall_mm"],
            "temp_rank":     sorted_by_temp.index(rec) + 1,
            "humidity_rank": sorted_by_humidity.index(rec) + 1,
            "rain_rank":     sorted_by_rain.index(rec) + 1,
            "fetch_date":    process_date,
        })

    return rankings


# ============================================================
# SAVING GOLD DATA
# ============================================================

def save_gold_csv(records: list, table_name: str, process_date: str) -> str:
    """
    Saves a gold table as CSV.
    Each table gets its own folder — mirrors Redshift table structure.
    """
    folder_path = os.path.join(
        GOLD_FOLDER,
        table_name,
        f"fetch_date={process_date}"
    )
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, "data.csv")
    columns   = list(records[0].keys())

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    return file_path


# ============================================================
# MAIN FUNCTION
# ============================================================

def run_gold_layer(process_date: str = None) -> None:
    """
    Reads silver data and generates all 3 gold tables.
    """
    if process_date is None:
        process_date = str(date.today())

    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — SILVER TO GOLD START")
    logger.info(f"Processing date : {process_date}")
    logger.info("=" * 55)

    # Read silver
    records = read_silver_file(process_date)
    if not records:
        logger.error("No silver records found — stopping")
        return

    # Generate Gold Table 1 — City Alerts
    logger.info("Generating city alerts...")
    city_alerts = generate_city_alerts(records)
    path1 = save_gold_csv(city_alerts, "city_alerts", process_date)
    logger.info(f"Saved city alerts to: {path1}")

    # Generate Gold Table 2 — State Summary
    logger.info("Generating state summary...")
    state_summary = generate_state_summary(records, process_date)
    path2 = save_gold_csv(state_summary, "state_summary", process_date)
    logger.info(f"Saved state summary to: {path2}")

    # Generate Gold Table 3 — Daily Rankings
    logger.info("Generating daily rankings...")
    daily_rankings = generate_daily_rankings(records, process_date)
    path3 = save_gold_csv(daily_rankings, "daily_rankings", process_date)
    logger.info(f"Saved daily rankings to: {path3}")

    # Final summary
    logger.info("=" * 55)
    logger.info("GOLD LAYER SUMMARY")
    logger.info(f"  Date            : {process_date}")
    logger.info(f"  Cities processed: {len(records)}")
    logger.info(f"  Gold tables     : 3")
    logger.info(f"  Table 1         : city_alerts ({len(city_alerts)} rows)")
    logger.info(f"  Table 2         : state_summary ({len(state_summary)} rows)")
    logger.info(f"  Table 3         : daily_rankings ({len(daily_rankings)} rows)")
    logger.info("=" * 55)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_gold_layer()