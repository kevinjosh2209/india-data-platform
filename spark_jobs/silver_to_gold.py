# ============================================================
# India Data Platform
# File    : spark_jobs/silver_to_gold.py
# Purpose : Read clean silver CSV from S3, generate business
#           insights, write gold tables back to S3.
#           Fully cloud native — no local files needed.
# Author  : Kevin Josh
# ============================================================

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

S3_BUCKET = "india-data-platform-111315405619"
REGION    = "ap-south-1"

s3_client = boto3.client("s3", region_name=REGION)

FLOOD_RAIN_MM      = 5.0
FLOOD_HUMIDITY_PCT = 85.0
SCARCITY_TEMP_C    = 35.0
SCARCITY_RAIN_MM   = 0.0
SCARCITY_HUMIDITY  = 40.0
HEAT_ALERT_TEMP_C  = 38.0


# ============================================================
# S3 HELPERS
# ============================================================

def read_csv_from_s3(s3_key: str) -> list:
    """
    Reads a CSV file directly from S3 into a list of dicts.
    """
    try:
        logger.info(f"Reading from S3: s3://{S3_BUCKET}/{s3_key}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content  = response["Body"].read().decode("utf-8")
        reader   = csv.DictReader(io.StringIO(content))
        records  = []
        for row in reader:
            row["temperature_c"]  = float(row["temperature_c"])
            row["humidity_pct"]   = float(row["humidity_pct"])
            row["rainfall_mm"]    = float(row["rainfall_mm"])
            row["wind_speed_kmh"] = float(row["wind_speed_kmh"])
            records.append(row)
        return records
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"Silver file not found: {s3_key}")
            logger.error("Run bronze_to_silver.py first")
        raise


def write_csv_to_s3(records: list, s3_key: str) -> None:
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
    )
    logger.info(f"Written to S3: s3://{S3_BUCKET}/{s3_key}")


# ============================================================
# RISK FUNCTIONS
# ============================================================

def calculate_flood_risk(rainfall: float, humidity: float) -> str:
    if rainfall > FLOOD_RAIN_MM and humidity > FLOOD_HUMIDITY_PCT:
        if rainfall > 50:
            return "CRITICAL"
        elif rainfall > 20:
            return "HIGH"
        else:
            return "MEDIUM"
    elif humidity > FLOOD_HUMIDITY_PCT:
        return "LOW"
    return "NONE"


def calculate_water_scarcity_risk(temp, rainfall, humidity) -> str:
    if temp > SCARCITY_TEMP_C and rainfall <= SCARCITY_RAIN_MM and humidity < SCARCITY_HUMIDITY:
        if temp > 45:
            return "CRITICAL"
        elif temp > 40:
            return "HIGH"
        return "MEDIUM"
    elif temp > SCARCITY_TEMP_C and rainfall <= SCARCITY_RAIN_MM:
        return "LOW"
    return "NONE"


def calculate_heat_alert(temp: float) -> str:
    if temp >= 45:
        return "EXTREME"
    elif temp >= 40:
        return "SEVERE"
    elif temp >= HEAT_ALERT_TEMP_C:
        return "MODERATE"
    return "NONE"


def calculate_cyclone_readiness(wind, rainfall, humidity) -> str:
    score = 0
    if wind > 60:
        score += 3
    elif wind > 40:
        score += 2
    elif wind > 20:
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
    return "NORMAL"


# ============================================================
# GOLD TABLE GENERATORS
# ============================================================

def generate_city_alerts(records: list) -> list:
    risk_levels = {
        "CRITICAL": 5, "EXTREME": 4, "SEVERE": 4,
        "HIGH": 3, "WATCH": 3, "MEDIUM": 2,
        "MONITOR": 2, "LOW": 1, "MODERATE": 1, "NONE": 0,
    }
    alerts = []
    for rec in records:
        temp     = rec["temperature_c"]
        humidity = rec["humidity_pct"]
        rainfall = rec["rainfall_mm"]
        wind     = rec["wind_speed_kmh"]

        flood    = calculate_flood_risk(rainfall, humidity)
        scarcity = calculate_water_scarcity_risk(temp, rainfall, humidity)
        heat     = calculate_heat_alert(temp)
        cyclone  = calculate_cyclone_readiness(wind, rainfall, humidity)

        max_risk = max(
            risk_levels.get(flood, 0),
            risk_levels.get(scarcity, 0),
            risk_levels.get(heat, 0),
            risk_levels.get(cyclone, 0),
        )
        overall = (
            "CRITICAL" if max_risk >= 5 else
            "HIGH"     if max_risk >= 3 else
            "MEDIUM"   if max_risk == 2 else
            "LOW"      if max_risk == 1 else
            "NORMAL"
        )

        if overall != "NORMAL":
            logger.warning(
                f"  ⚠️  {rec['city']}: overall={overall} | "
                f"flood={flood} | scarcity={scarcity} | "
                f"heat={heat} | cyclone={cyclone}"
            )
        else:
            logger.info(f"  ✅ {rec['city']}: NORMAL | temp={temp}°C")

        alerts.append({
            "city"           : rec["city"],
            "state"          : rec["state"],
            "temperature_c"  : temp,
            "humidity_pct"   : humidity,
            "rainfall_mm"    : rainfall,
            "wind_speed_kmh" : wind,
            "flood_risk"     : flood,
            "water_scarcity" : scarcity,
            "heat_alert"     : heat,
            "cyclone_watch"  : cyclone,
            "overall_risk"   : overall,
            "processed_at"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return alerts


def generate_state_summary(records: list, process_date: str) -> list:
    state_groups = {}
    for rec in records:
        state = rec["state"]
        if state not in state_groups:
            state_groups[state] = []
        state_groups[state].append(rec)

    summaries = []
    for state, cities in state_groups.items():
        summaries.append({
            "state"           : state,
            "city_count"      : len(cities),
            "avg_temp_c"      : round(sum(r["temperature_c"] for r in cities) / len(cities), 2),
            "max_temp_c"      : round(max(r["temperature_c"] for r in cities), 2),
            "min_temp_c"      : round(min(r["temperature_c"] for r in cities), 2),
            "avg_humidity_pct": round(sum(r["humidity_pct"]  for r in cities) / len(cities), 2),
            "total_rain_mm"   : round(sum(r["rainfall_mm"]   for r in cities), 2),
            "processed_at"    : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return summaries


def generate_daily_rankings(records: list, process_date: str) -> list:
    sorted_temp     = sorted(records, key=lambda x: x["temperature_c"], reverse=True)
    sorted_humidity = sorted(records, key=lambda x: x["humidity_pct"],  reverse=True)
    sorted_rain     = sorted(records, key=lambda x: x["rainfall_mm"],   reverse=True)

    return [{
        "rank"        : i + 1,
        "city"        : rec["city"],
        "state"       : rec["state"],
        "temperature_c": rec["temperature_c"],
        "humidity_pct": rec["humidity_pct"],
        "rainfall_mm" : rec["rainfall_mm"],
        "temp_rank"   : sorted_temp.index(rec) + 1,
        "humidity_rank": sorted_humidity.index(rec) + 1,
        "rain_rank"   : sorted_rain.index(rec) + 1,
    } for i, rec in enumerate(sorted_temp)]


# ============================================================
# MAIN FUNCTION
# ============================================================

def run_gold_layer(process_date: str = None) -> None:
    if process_date is None:
        process_date = str(date.today())

    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — SILVER TO GOLD START")
    logger.info(f"Processing date : {process_date}")
    logger.info("=" * 55)

    silver_key = (
        f"silver/weather_cleaned/"
        f"fetch_date={process_date}/"
        f"data.csv"
    )

    records = read_csv_from_s3(silver_key)
    logger.info(f"Loaded {len(records)} silver records")

    # City alerts
    logger.info("Generating city alerts...")
    city_alerts = generate_city_alerts(records)
    write_csv_to_s3(
        city_alerts,
        f"gold/city_alerts/fetch_date={process_date}/data.csv"
    )

    # State summary
    logger.info("Generating state summary...")
    state_summary = generate_state_summary(records, process_date)
    write_csv_to_s3(
        state_summary,
        f"gold/state_summary/fetch_date={process_date}/data.csv"
    )

    # Daily rankings
    logger.info("Generating daily rankings...")
    daily_rankings = generate_daily_rankings(records, process_date)
    write_csv_to_s3(
        daily_rankings,
        f"gold/daily_rankings/fetch_date={process_date}/data.csv"
    )

    logger.info("=" * 55)
    logger.info("GOLD LAYER SUMMARY")
    logger.info(f"  Date            : {process_date}")
    logger.info(f"  Cities processed: {len(records)}")
    logger.info(f"  Gold tables     : 3")
    logger.info(f"  Table 1         : city_alerts ({len(city_alerts)} rows)")
    logger.info(f"  Table 2         : state_summary ({len(state_summary)} rows)")
    logger.info(f"  Table 3         : daily_rankings ({len(daily_rankings)} rows)")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_gold_layer()