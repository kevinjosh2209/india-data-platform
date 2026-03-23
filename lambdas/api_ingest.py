# ============================================================
# India Data Platform
# File    : lambdas/api_ingest.py
# Purpose : Fetch live weather data from Open-Meteo API
#           for major Indian cities and save to local bronze
#           folder. This will later run as AWS Lambda function.
# Author  : Kevin Josh
# ============================================================

import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, date
import os
import boto3

# ============================================================
# LOGGING SETUP
# In production we always use logging — never plain print()
# logging gives us timestamp, severity level and source
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# All config stays at top — easy to find and change
# ============================================================

# Major Indian cities with their coordinates
# Latitude and longitude taken from Google Maps
INDIAN_CITIES = [
    {"name": "Mumbai",     "state": "Maharashtra",  "lat": 19.0760, "lon": 72.8777},
    {"name": "Delhi",      "state": "Delhi",        "lat": 28.6139, "lon": 77.2090},
    {"name": "Chennai",    "state": "Tamil Nadu",   "lat": 13.0827, "lon": 80.2707},
    {"name": "Bangalore",  "state": "Karnataka",    "lat": 12.9716, "lon": 77.5946},
    {"name": "Coimbatore", "state": "Tamil Nadu",   "lat": 11.0168, "lon": 76.9558},
    {"name": "Kolkata",    "state": "West Bengal",  "lat": 22.5726, "lon": 88.3639},
    {"name": "Hyderabad",  "state": "Telangana",    "lat": 17.3850, "lon": 78.4867},
    {"name": "Ahmedabad",  "state": "Gujarat",      "lat": 23.0225, "lon": 72.5714},
]

# Output folder — this mimics S3 bronze layer structure locally
BRONZE_FOLDER = "bronze_data"


# ============================================================
# FUNCTIONS
# ============================================================

def build_api_url(lat: float, lon: float) -> str:
    """
    Builds the Open-Meteo API URL for a given location.
    Open-Meteo is free — no API key needed. Perfect for learning.
    """
    return (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"
        f"&timezone=Asia%2FKolkata"
    )


def fetch_city_weather(city: dict) -> dict | None:
    """
    Calls the weather API for one city.
    Returns a clean dictionary or None if something went wrong.

    This is the pattern used in every real Lambda function:
    - try to get the data
    - handle specific errors separately
    - never crash the whole pipeline for one city failure
    """
    url = build_api_url(city["lat"], city["lon"])

    try:
        logger.info(f"Fetching weather for {city['name']}...")

        request = urllib.request.Request(
            url,
            headers={"User-Agent": "india-data-platform/1.0"}
        )

        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8")
            data = json.loads(raw)

        # Extract only the fields we need from the API response
        current = data["current"]

        result = {
            "city":           city["name"],
            "state":          city["state"],
            "latitude":       city["lat"],
            "longitude":      city["lon"],
            "temperature_c":  current["temperature_2m"],
            "humidity_pct":   current["relative_humidity_2m"],
            "rainfall_mm":    current["precipitation"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "fetched_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fetch_date":     str(date.today()),
            "source":         "open-meteo.com",
            "pipeline":       "india-data-platform",
        }

        logger.info(
            f"  ✅ {city['name']}: "
            f"{result['temperature_c']}°C | "
            f"Humidity: {result['humidity_pct']}% | "
            f"Rain: {result['rainfall_mm']}mm"
        )
        return result

    except urllib.error.URLError as e:
        # Network error — no internet or API is down
        logger.error(f"  ❌ Network error for {city['name']}: {e}")
        return None

    except KeyError as e:
        # API returned unexpected structure
        logger.error(f"  ❌ Unexpected API response for {city['name']}: missing key {e}")
        return None

    except Exception as e:
        # Catch anything else — log it but don't crash
        logger.error(f"  ❌ Unexpected error for {city['name']}: {e}")
        return None


def save_to_bronze(records: list, fetch_date: date) -> str:
    """
    Saves fetched records directly to S3 bronze layer.
    No local file — goes straight to cloud.
    This is the production pattern.
    """
    # S3 key follows the same date partition pattern
    s3_key = (
        f"bronze/weather/"
        f"year={fetch_date.year}/"
        f"month={fetch_date.month:02d}/"
        f"day={fetch_date.day:02d}/"
        f"data.json"
    )

    # Convert records to JSON string
    body = json.dumps(records, indent=2, ensure_ascii=False)

    # Upload directly to S3
    s3_client = boto3.client("s3", region_name="ap-south-1")
    s3_client.put_object(
        Bucket   = "india-data-platform-111315405619",
        Key      = s3_key,
        Body     = body.encode("utf-8"),
        ContentType = "application/json",
        Metadata = {
            "pipeline"    : "india-data-platform",
            "ingested_at" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "record_count": str(len(records)),
        }
    )

    logger.info(f"Uploaded to S3: s3://india-data-platform-111315405619/{s3_key}")
    return s3_key


def run_ingestion() -> None:
    """
    Main function — orchestrates the full ingestion run.
    In AWS this becomes the Lambda handler function.
    """
    logger.info("=" * 55)
    logger.info("INDIA DATA PLATFORM — WEATHER INGESTION START")
    logger.info("=" * 55)

    today = date.today()
    successful_records = []
    failed_cities = []

    # Loop through every city and fetch weather
    for city in INDIAN_CITIES:
        result = fetch_city_weather(city)

        if result is not None:
            successful_records.append(result)
        else:
            failed_cities.append(city["name"])

    # Save whatever we got to bronze layer
    if successful_records:
        file_path = save_to_bronze(successful_records, today)
        logger.info(f"Saved {len(successful_records)} records to: {file_path}")
    else:
        logger.error("No records fetched — nothing saved")

    # Final summary — this is what you check in CloudWatch logs
    logger.info("=" * 55)
    logger.info("INGESTION SUMMARY")
    logger.info(f"  Date          : {today}")
    logger.info(f"  Total cities  : {len(INDIAN_CITIES)}")
    logger.info(f"  Successful    : {len(successful_records)}")
    logger.info(f"  Failed        : {len(failed_cities)}")
    if failed_cities:
        logger.warning(f"  Failed cities : {', '.join(failed_cities)}")
    logger.info("=" * 55)


# ============================================================
# ENTRY POINT
# This block only runs when you execute this file directly.
# When AWS Lambda imports this file, it won't run automatically.
# ============================================================

if __name__ == "__main__":
    run_ingestion()