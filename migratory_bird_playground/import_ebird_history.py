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

if not EBIRD_API_KEY or EBIRD_API_KEY.strip() == "" or "YOUR_" in EBIRD_API_KEY:
    raise ValueError("CRITICAL: EBIRD_API_KEY is blank or unconfigured.")

if not NEON_DATABASE_URL:
    raise ValueError("CRITICAL: NEON_CONNECTION_STRING environment variable is missing.")

REGIONS = [
    "US-IL", "US-IN", "US-IA", "US-MI", "US-MN", "US-MO", "US-OH", "US-WI",
    "CA-AB", "CA-SK", "CA-MB", "CA-ON", "CA-NT", "CA-NU", "CA-YT"
]
WATERFOWL_FAMILIES = ["anatid1"] 

def get_db_connection():
    return psycopg2.connect(NEON_DATABASE_URL)

def get_waterfowl_species_codes():
    # URL 1: Corrected taxonomy endpoint
    url = "https://ebird.org"
    headers = {
        "x-ebirdapitoken": EBIRD_API_KEY.strip(),
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    query_params = {
        "fmt": "json",
        "cat": "species" 
    }
    print(f"Sending taxonomy request using token ending in: ...{EBIRD_API_KEY[-4:] if EBIRD_API_KEY else 'NONE'}")
    response = requests.get(url, headers=headers, params=query_params)
    content_type = response.headers.get('Content-Type', '')
    if "json" not in content_type.lower():
        print("!!! eBird API returned HTML instead of data !!!")
        raise ValueError("API redirected to an HTML login page.")
    return response.json()

def fetch_and_load_migration():
    waterfowl_dict = get_waterfowl_species_codes()
    print(f"Loaded {len(waterfowl_dict)} valid waterfowl species definitions.")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    
    headers = {
        "x-ebirdapitoken": EBIRD_API_KEY.strip(),
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    current_date = start_date
    while current_date <= end_date:
        year, month, day = current_date.year, current_date.month, current_date.day
        print(f"Processing date: {year}-{month:02d}-{day:02d}")
        
        for region in REGIONS:
            # URL 2: Corrected historical endpoint
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
