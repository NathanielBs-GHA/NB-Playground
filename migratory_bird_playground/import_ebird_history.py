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

# --- REINFORCED ENVIRONMENT CHECK ---
if not EBIRD_API_KEY or EBIRD_API_KEY.strip() == "" or "YOUR_" in EBIRD_API_KEY:
    raise ValueError("CRITICAL: EBIRD_API_KEY is blank or unconfigured in the runner context.")

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
    url = "https://api.ebird.org/v2/ref/taxonomy/ebird"
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
        raise ValueError("API redirected to an HTML login page. Your API Token is invalid or blocked.")
    
    raw_list = response.json()
    
    # Transform the list into a fast lookup dictionary filtered to waterfowl
    waterfowl_lookup = {}
    for item in raw_list:
        # eBird uses 'familyCode' in its taxonomy response
        if item.get("familyCode") in WATERFOWL_FAMILIES:
            species_code = item.get("speciesCode")
            common_name = item.get("comName")
            waterfowl_lookup[species_code] = common_name
            
    return waterfowl_lookup

def fetch_and_load_migration():
    waterfowl_dict = get_waterfowl_species_codes()
    print(f"Loaded {len(waterfowl_dict)} valid waterfowl species definitions.")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5 * 365)
    
    # FIXED: Standardizing loop headers to match working taxonomy call specifications
    headers = {
        "x-ebirdapitoken": EBIRD_API_KEY.strip(),
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    current_date = start_date
    while current_date <= end_date:
        year, month, day = current_date.year, current_date.month, current_date.day
        print(f"Processing date: {year}-{month:02d}-{day:02d}")
        
        # Move the batch tracker HERE so it collects data from all regions on this date
        batch_data = []
        
        for region in REGIONS:
            base_url = f"https://api.ebird.org/v2/data/obs/{region}/historic/{year}/{month}/{day}"
            try:
                response = requests.get(base_url, headers=headers, params={"detail": "simple"})
                if response.status_code == 429:
                    print("Rate limit reached. Sleeping 60 seconds...")
                    time.sleep(60)
                    continue
                response.raise_for_status()
                records = response.json()
            except Exception as e:
                print(f"Failed pulling data for {region} on {year}-{month:02d}-{day:02d}: {e}")
                continue

            for obs in records:
                spec_code = obs.get("speciesCode")
                if spec_code in waterfowl_dict:
                    # Map null or missing quantities to 1
                    how_many = obs.get("howMany")
                    how_many = int(how_many) if how_many is not None else 1
                    
                    batch_data.append((
                        spec_code,
                        waterfowl_dict[spec_code],
                        region,
                        f"{year}-{month:02d}-{day:02d}",
                        how_many,
                        obs.get("lat"),
                        obs.get("lng"),
                        obs.get("obsValid", True)
                    ))
            
            # Short sleep between regions to prevent aggressive throttling
            time.sleep(0.2)

        # Insert all gathered data for this specific date at once
        if batch_data:
            insert_query = """
            INSERT INTO waterfowl_migration (species_code, common_name, region_code, observation_date, how_many, latitude, longitude, valid)
            VALUES %s
            ON CONFLICT (species_code, region_code, observation_date, latitude, longitude)
            DO UPDATE SET how_many = EXCLUDED.how_many;
            """
            try:
                execute_values(cursor, insert_query, batch_data)
                conn.commit()
                print(f" Successfully inserted {len(batch_data)} rows for {year}-{month:02d}-{day:02d}")
            except Exception as e:
                print(f"Database insertion error: {e}")
                conn.rollback()

        current_date += timedelta(days=1)
       
    cursor.close()
    conn.close()
    print("Migration data ingestion pipeline complete!")

if __name__ == "__main__":
    fetch_and_load_migration()
