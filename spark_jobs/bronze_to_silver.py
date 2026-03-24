# ============================================================
# India Data Platform
# File    : spark_jobs/bronze_to_silver.py
# Purpose : Read raw JSON from S3 bronze layer, clean and
#           validate, write clean CSV to S3 silver layer.
#           Fully cloud native — no local files needed.
# Author  : Kevin Josh
# ============================================================

import json
import csv
import io
import logging
import boto3
from datetime import datetime, date
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

S3_BUCKET    = "india-data-platform-111315405619"
REGION       = "ap-south-1"
TEMP_MIN     = -5.0
TEMP_MAX     = 55.0
HUMIDITY_MIN = 0.0
HUMIDITY_MAX = 100.0

s3_client = boto3.client("s3", region_name=REGION)


# ============================================================
# S3 READ / WRITE HELPERS
# ============================================================

def read_from_s3(s3_key: str) -> dict:
    """
    Reads a JSON file directly from S3.
    No local download needed.
    """
    try:
        logger.info(f"Reading from S3: s3://{S3_BUCKET}/{s3_key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content  = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey":
            logger.error(f"File not found in S3: {s3_key}")
            logger.error("Run api_ingest.py first to fetch today's data")
        else:
            logger.error(f"S3 error reading {s3_key}: {e}")
        raise


def write_csv_to_s3(records: list, s3_key: str) -> None:
    """
    Writes a list of dictionaries as CSV directly to S3.
    Uses in-memory buffer — no local file created.
    """
    buffer  = io.StringIO()
    columns = list(records[0].keys())
    writer  = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerows(records)

    s3_client.put_object(
        Bucket      = S3_BUCKET,
        Key         = s3_key,
        Body        = buffer.getvalue().encode("utf-8"),
        ContentType = "text/csv",
        Metadata    = {
            "pipeline"        : "india-data-platform",
            "processed_at"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "record_count"    : str(len(records)),
            "pipeline_version": "1.0.0",
        }
    )
    logger.info(f"Written to S3: s3://{S3_BUCKET}/{s3_key}")


# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def is_valid_temperature(temp) -> bool:
    if temp is None:
        return False
    try:
        return TEMP_MIN <= float(temp) <= TEMP_MAX
    except (ValueError, TypeError):
        return False


def is_valid_humidity(humidity) -> bool:
    if humidity is None:
        return False
    try:
        return HUMIDITY_MIN <= float(humidity) <= HUMIDITY_MAX
    except (ValueError, TypeError):
        return False


# ============================================================
# TRANSFORMATION FUNCTIONS
# ============================================================

def clean_city_name(city: str) -> str:
    if city is None:
        return "UNKNOWN"
    return str(city).strip().upper()


def clean_state_name(state: str) -> str:
    if state is None:
        return "UNKNOWN"
    return str(state).strip().upper()


def clean_rainfall(rainfall) -> float:
    if rainfall is None:
        return 0.0
    try:
        return max(0.0, float(rainfall))
    except (ValueError, TypeError):
        return 0.0


def clean_wind_speed(wind_speed) -> float:
    if wind_speed is None:
        return 0.0
    try:
        return max(0.0, float(wind_speed))
    except (ValueError, TypeError):
        return 0.0


def parse_fetch_date(fetched_at: str) -> str:
    if fetched_at is None:
        return str(date.today())
    try:
        dt = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return str(date.today())


def get_temperature_category(temp: float) -> str:
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


def transform_record(raw: dict) -> dict | None:
    city  = clean_city_name(raw.get("city"))
    state = clean_state_name(raw.get("state"))

    temp_raw     = raw.get("temperature_c")
    humidity_raw = raw.get("humidity_pct")

    if not is_valid_temperature(temp_raw):
        logger.warning(f"REJECTED {city}: invalid temperature '{temp_raw}'")
        return None

    if not is_valid_humidity(humidity_raw):
        logger.warning(f"REJECTED {city}: invalid humidity '{humidity_raw}'")
        return None

    temp      = round(float(temp_raw), 2)
    humidity  = round(float(humidity_raw), 2)
    rainfall  = clean_rainfall(raw.get("rainfall_mm"))
    wind      = clean_wind_speed(raw.get("wind_speed_kmh"))
    fetch_date= parse_fetch_date(raw.get("fetched_at"))

    return {
        "city"            : city,
        "state"           : state,
        "temperature_c"   : temp,
        "humidity_pct"    : humidity,
        "rainfall_mm"     : rainfall,
        "wind_speed_kmh"  : wind,
        "temperature_cat" : get_temperature_category(temp),
        "fetch_date"      : fetch_date,
        "processed_at"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version": "1.0.0",
        "source"          : "open-meteo.com",
    }


# ============================================================
# MAIN FUNCTION
# ============================================================

def run_transformation(process_date: str = None) -> None:
    if process_date is None:
        process_date = str(date.today())

    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — BRONZE TO SILVER START")
    logger.info(f"Processing date : {process_date}")
    logger.info("=" * 55)

    dt = datetime.strptime(process_date, "%Y-%m-%d")

    # S3 paths
    bronze_key = (
        f"bronze/weather/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"data.json"
    )
    silver_key = (
        f"silver/weather_cleaned/"
        f"fetch_date={process_date}/"
        f"data.csv"
    )

    # Read from S3 bronze
    raw_records = read_from_s3(bronze_key)
    logger.info(f"Loaded {len(raw_records)} raw records from bronze")

    # Transform
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
                f"Humidity: {clean['humidity_pct']}%"
            )
        else:
            rejected_count += 1

    # Write to S3 silver
    if silver_records:
        write_csv_to_s3(silver_records, silver_key)

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


if __name__ == "__main__":
    run_transformation()