# ============================================================
# India Data Platform
# File    : spark_jobs/bronze_to_silver.py
# Purpose : Read raw bronze weather JSON, apply cleaning and
#           validation, save as structured silver layer CSV.
#           Fixes: null values, invalid ranges, date types,
#           inconsistent city name capitalization.
# Author  : Kevin Josh
# ============================================================

import json
import csv
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

# Input — bronze layer (raw JSON)
BRONZE_FOLDER = "bronze_data"

# Output — silver layer (clean CSV)
SILVER_FOLDER = "silver_data"

# Valid temperature range for India in Celsius
TEMP_MIN = -5.0
TEMP_MAX = 55.0

# Valid humidity range
HUMIDITY_MIN = 0.0
HUMIDITY_MAX = 100.0


# ============================================================
# VALIDATION FUNCTIONS
# Each function checks one specific rule
# Returns True if valid, False if invalid
# ============================================================

def is_valid_temperature(temp) -> bool:
    """
    Checks if temperature is realistic for India.
    Anything below -5 or above 55 is clearly wrong data.
    """
    if temp is None:
        return False
    try:
        temp = float(temp)
        return TEMP_MIN <= temp <= TEMP_MAX
    except (ValueError, TypeError):
        return False


def is_valid_humidity(humidity) -> bool:
    """
    Humidity must be between 0 and 100 percent.
    """
    if humidity is None:
        return False
    try:
        humidity = float(humidity)
        return HUMIDITY_MIN <= humidity <= HUMIDITY_MAX
    except (ValueError, TypeError):
        return False


# ============================================================
# TRANSFORMATION FUNCTIONS
# Each function fixes one specific problem
# ============================================================

def clean_city_name(city: str) -> str:
    """
    Problem 4 fix — standardize city names to uppercase.
    'mumbai', 'Mumbai', 'MUMBAI' all become 'MUMBAI'
    """
    if city is None:
        return "UNKNOWN"
    return str(city).strip().upper()


def clean_state_name(state: str) -> str:
    """
    Standardize state names to uppercase.
    """
    if state is None:
        return "UNKNOWN"
    return str(state).strip().upper()


def clean_rainfall(rainfall) -> float:
    """
    Problem 2 fix — replace null rainfall with 0.0
    Null rainfall means no rain was recorded — 0 is correct.
    """
    if rainfall is None:
        return 0.0
    try:
        value = float(rainfall)
        # Rainfall cannot be negative
        return max(0.0, value)
    except (ValueError, TypeError):
        return 0.0


def clean_wind_speed(wind_speed) -> float:
    """
    Replace null wind speed with 0.0
    """
    if wind_speed is None:
        return 0.0
    try:
        value = float(wind_speed)
        return max(0.0, value)
    except (ValueError, TypeError):
        return 0.0


def parse_fetch_date(fetched_at: str) -> str:
    """
    Problem 3 fix — convert plain text date to proper format.
    Input  : "2026-03-20 01:01:16"  (plain text)
    Output : "2026-03-20"           (clean date string)
    In real Spark this becomes a proper DateType column.
    """
    if fetched_at is None:
        return str(date.today())
    try:
        dt = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return str(date.today())


def get_temperature_category(temp: float) -> str:
    """
    Derives a human readable category from temperature.
    This is a new column we ADD during transformation.
    Bronze had raw numbers — Silver adds meaning.
    """
    if temp >= 40:
        return "EXTREME_HEAT"
    elif temp >= 35:
        return "VERY_HOT"
    elif temp >= 25:
        return "WARM"
    elif temp >= 15:
        return "MODERATE"
    else:
        return "COOL"


# ============================================================
# MAIN TRANSFORMATION LOGIC
# ============================================================

def transform_record(raw: dict) -> dict | None:
    """
    Takes one raw bronze record and returns one clean silver record.
    Returns None if the record fails critical validation.

    This is the heart of the Silver layer —
    every single record passes through this function.
    """
    city = clean_city_name(raw.get("city"))
    state = clean_state_name(raw.get("state"))

    # --- Critical validations ---
    # If temperature is invalid we REJECT the record entirely
    # We cannot guess what the real temperature was
    temp_raw = raw.get("temperature_c")
    if not is_valid_temperature(temp_raw):
        logger.warning(
            f"REJECTED {city}: invalid temperature '{temp_raw}' "
            f"— must be between {TEMP_MIN} and {TEMP_MAX}"
        )
        return None

    humidity_raw = raw.get("humidity_pct")
    if not is_valid_humidity(humidity_raw):
        logger.warning(
            f"REJECTED {city}: invalid humidity '{humidity_raw}' "
            f"— must be between {HUMIDITY_MIN} and {HUMIDITY_MAX}"
        )
        return None

    # --- Safe to transform now ---
    temp = round(float(temp_raw), 2)
    humidity = round(float(humidity_raw), 2)
    rainfall = clean_rainfall(raw.get("rainfall_mm"))
    wind_speed = clean_wind_speed(raw.get("wind_speed_kmh"))
    fetch_date = parse_fetch_date(raw.get("fetched_at"))
    temp_category = get_temperature_category(temp)

    # Build clean silver record
    return {
        "city":              city,
        "state":             state,
        "temperature_c":     temp,
        "humidity_pct":      humidity,
        "rainfall_mm":       rainfall,
        "wind_speed_kmh":    wind_speed,
        "temperature_cat":   temp_category,
        "fetch_date":        fetch_date,
        "processed_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version":  "1.0.0",
        "source":            "open-meteo.com",
    }


def read_bronze_file(file_path: str) -> list:
    """
    Reads the raw JSON file from bronze layer.
    """
    logger.info(f"Reading bronze file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} raw records from bronze")
    return data


def save_silver_file(records: list, fetch_date: str) -> str:
    """
    Saves clean records to silver layer as CSV.
    CSV is easier to query with SQL tools like Athena.
    Folder structure mirrors S3 silver layer partitioning.
    """
    folder_path = os.path.join(
        SILVER_FOLDER,
        "weather_cleaned",
        f"fetch_date={fetch_date}"
    )
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, "data.csv")

    # Get column names from first record
    columns = list(records[0].keys())

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    return file_path


def run_transformation(process_date: str = None) -> None:
    """
    Main function — reads bronze, transforms, saves silver.
    process_date format: YYYY-MM-DD
    If not provided, uses today's date.
    """
    if process_date is None:
        process_date = str(date.today())

    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — BRONZE TO SILVER START")
    logger.info(f"Processing date : {process_date}")
    logger.info("=" * 55)

    # Parse date to build folder path
    dt = datetime.strptime(process_date, "%Y-%m-%d")

    # Build bronze input path
    bronze_path = os.path.join(
        BRONZE_FOLDER,
        "weather",
        f"year={dt.year}",
        f"month={dt.month:02d}",
        f"day={dt.day:02d}",
        "data.json"
    )

    # Check bronze file exists
    if not os.path.exists(bronze_path):
        logger.error(f"Bronze file not found: {bronze_path}")
        logger.error("Run api_ingest.py first to fetch today's data")
        return

    # Read bronze
    raw_records = read_bronze_file(bronze_path)

    # Transform each record
    silver_records = []
    rejected_count = 0

    for raw in raw_records:
        clean = transform_record(raw)
        if clean is not None:
            silver_records.append(clean)
            logger.info(
                f"  ✅ {clean['city']}: "
                f"{clean['temperature_c']}°C | "
                f"{clean['temperature_cat']} | "
                f"Humidity: {clean['humidity_pct']}% | "
                f"Rain: {clean['rainfall_mm']}mm"
            )
        else:
            rejected_count += 1

    # Save silver
    if silver_records:
        file_path = save_silver_file(silver_records, process_date)
        logger.info(f"Saved {len(silver_records)} clean records to: {file_path}")
    else:
        logger.error("No valid records after transformation — nothing saved")

    # Summary
    logger.info("=" * 55)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info(f"  Date              : {process_date}")
    logger.info(f"  Raw records       : {len(raw_records)}")
    logger.info(f"  Passed validation : {len(silver_records)}")
    logger.info(f"  Rejected          : {rejected_count}")
    logger.info(
        f"  Success rate      : "
        f"{round(len(silver_records)/len(raw_records)*100, 1)}%"
    )
    logger.info("=" * 55)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_transformation()