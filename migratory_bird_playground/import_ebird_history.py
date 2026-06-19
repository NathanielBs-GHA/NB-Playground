import os
import time
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

EBIRD_API_KEY = os.getenv("EBIRD_API_KEY")
NEON_DATABASE_URL = os.getenv("NEON_CONNECTION_STRING")

if not EBIRD_API_KEY or not NEON_DATABASE_URL:
    raise ValueError("Missing environment variables! Check EBIRD_API_KEY and NEON_CONNECTION_STRING.")

REGIONS = [
    "US-IL", "US-IN", "US-IA", "US-MI", "US-MN", "US-MO", "US-OH", "US-WI",
    "CA-AB", "CA-SK", "CA-MB", "CA-ON", "CA-NT", "CA-NU", "CA-YT"
]
WATERFOWL_FAMILIES = ["anatid1"] 

def get_db_connection():
    return psycopg2.connect(NEON_DATABASE_URL)

def get_waterfowl_species_codes():
    url = "https://ebird.org"
    headers = {"X-eBirdApiToken": EBIRD_API_KEY}
    
    print("Sending taxonomy request to eBird API...")
    response = requests.get(url, headers=headers)
    
    # Check if we didn't get a successful 200 OK status
    if response.status_code != 200:
        print(f"!!! CRITICAL API ERROR !!!")
        print(f"Status Code: {response.status_code}")
        print(f"Raw Response: {response.text}")
        raise ValueError(f"eBird API rejected request with status code {response.status_code}")
        
    try:
        data = response.json()
    except Exception as e:
        print("!!! FAILED TO PARSE JSON !!!")
        print(f"Raw response preview: {response.text[:500]}")
        raise e

    waterfowl_codes = {}
    for record in data:
        if record.get("familyCode") in WATERFOWL_FAMILIES:
            waterfowl_codes[record["speciesCode"]] = record["comName"]
    return waterfowl_codes

def fetch_and_load_migration():
    waterfowl_dict = get_waterfowl_species_codes()
    print(f"Loaded {len(waterfowl_dict)} valid waterfowl species definitions.")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    
    headers = {"X-eBirdApiToken": EBIRD_API_KEY}
    current_date = start_date
    
    while current_date <= end_date:
        year, month, day = current_date.year, current_date.month, current_date.day
        print(f"Processing date: {year}-{month:02d}-{day:02d}")
        
        for region in REGIONS:
            url = f"https://ebird.org{region}/historic/{year}/{month}/{day}"
            try:
                response = requests.get(url, headers=headers, params={"detail": "simple"})
                if response.status_code == 429:
                    print("Rate limit reached. Sleeping 60 seconds...")
                    time.sleep(60)
                    continue
                response.raise_for_status()
                records = response.json()
            except Exception as e:
                print(f"Failed pulling data for {region} on {year}-{month:02d}-{day:02d}: {e}")
                continue

            batch_data = []
            for obs in records:
                spec_code = obs.get("speciesCode")
                if spec_code in waterfowl_dict:
                    batch_data.append((
                        spec_code,
                        waterfowl_dict[spec_code],
                        region,
                        f"{year}-{month:02d}-{day:02d}",
                        obs.get("howMany", 1),
                        obs.get("lat"),
                        obs.get("lng"),
                        obs.get("obsValid", True)
                    ))
            
            if batch_data:
                insert_query = """
                    INSERT INTO waterfowl_migration 
                    (species_code, common_name, region_code, observation_date, how_many, latitude, longitude, valid)
                    VALUES %s
                    ON CONFLICT (species_code, region_code, observation_date, latitude, longitude) 
                    DO UPDATE SET how_many = EXCLUDED.how_many;
                """
                try:
                    execute_values(cursor, insert_query, batch_data)
                    conn.commit()
                except Exception as e:
                    print(f"Database insertion error: {e}")
                    conn.rollback()
            time.sleep(0.1)
        current_date += timedelta(days=1)
        
    cursor.close()
    conn.close()
    print("Migration data ingestion pipeline complete!")

if __name__ == "__main__":
    fetch_and_load_migration()
