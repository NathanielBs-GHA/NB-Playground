import os
import time
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
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

# Initialize a Threaded Database Connection Pool
# Allows up to 5 concurrent connections to Neon safely across threads
db_pool = ThreadedConnectionPool(1, 5, dsn=NEON_DATABASE_URL)

def get_waterfowl_species_codes():
    url = "https://api.ebird.org/v2/ref/taxonomy/ebird"
    headers = {
        "x-ebirdapitoken": EBIRD_API_KEY.strip(),
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    query_params = {"fmt": "json", "cat": "species"}
    
    print(f"Fetching taxonomy mapping using token ending in: ...{EBIRD_API_KEY[-4:] if EBIRD_API_KEY else 'NONE'}")
    response = requests.get(url, headers=headers, params=query_params)
    response.raise_for_status()
    
    content_type = response.headers.get('Content-Type', '')
    if "json" not in content_type.lower():
        raise ValueError("API redirected to HTML login page. Your API Token is invalid or blocked.")
    
    raw_list = response.json()
    waterfowl_lookup = {}
    for item in raw_list:
        if item.get("familyCode") in WATERFOWL_FAMILIES:
            species_code = item.get("speciesCode")
            waterfowl_lookup[species_code] = item.get("comName")
            
    return waterfowl_lookup

def process_single_date(target_date, waterfowl_dict):
    """Worker function executed inside thread pool to parse a single day across regions"""
    year, month, day = target_date.year, target_date.month, target_date.day
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    headers = {
        "x-ebirdapitoken": EBIRD_API_KEY.strip(),
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    batch_data = []
    
    for region in REGIONS:
        base_url = f"https://api.ebird.org/v2/data/obs/{region}/historic/{year}/{month}/{day}"
        try:
            response = requests.get(base_url, headers=headers, params={"detail": "simple"}, timeout=15)
            
            if response.status_code == 429:
                print(f"[{date_str}] Rate limit reached. Backing off 30s...")
                time.sleep(30)
                continue
                
            response.raise_for_status()
            records = response.json()
        except Exception as e:
            print(f"[{date_str}] Failed fetching data for region {region}: {e}")
            continue

        for obs in records:
            spec_code = obs.get("speciesCode")
            if spec_code in waterfowl_dict:
                how_many = obs.get("howMany")
                how_many = int(how_many) if how_many is not None else 1
                
                batch_data.append((
                    spec_code,
                    waterfowl_dict[spec_code],
                    region,
                    date_str,
                    how_many,
                    obs.get("lat"),
                    obs.get("lng"),
                    obs.get("obsValid", True)
                ))
        
        # Tiny delay between regional API queries inside the thread
        time.sleep(0.1)

    # Database Write Phase (If matching target data exists)
    if batch_data:
        # Request a dedicated connection from our connection pool
        conn = db_pool.getconn()
        try:
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO waterfowl_migration (species_code, common_name, region_code, observation_date, how_many, latitude, longitude, valid)
            VALUES %s
            ON CONFLICT (species_code, region_code, observation_date, latitude, longitude)
            DO UPDATE SET how_many = EXCLUDED.how_many;
            """
            execute_values(cursor, insert_query, batch_data)
            conn.commit()
            cursor.close()
            print(f"✅ [{date_str}] Successfully inserted {len(batch_data)} rows into Neon.")
        except Exception as e:
            print(f"❌ [{date_str}] Database insertion error: {e}")
            conn.rollback()
        finally:
            # Always return connection back to pool for next thread to use
            db_pool.putconn(conn)
    else:
        print(f"ℹ️ [{date_str}] Complete. No matching waterfowl observed on this day.")

def run_pipeline():
    try:
        # Load fast dictionary layout map
        waterfowl_dict = get_waterfowl_species_codes()
        print(f"Loaded {len(waterfowl_dict)} valid waterfowl species taxonomy definitions.\n")
        
        # Build list of chronological target dates (5 years)
        end_date = datetime.today()
        start_date = end_date - timedelta(days=5 * 365)
        
        date_list = []
        current = start_date
        while current <= end_date:
            date_list.append(current)
            current += timedelta(days=1)
            
        print(f"Initializing Multi-threaded Ingestion Engine across {len(date_list)} targets...")
        
        # Execute concurrent tasks with max_workers balancing performance & rate limits
        # Use python -u flag in GitHub runner settings for instantaneous terminal updates
        with ThreadPoolExecutor(max_workers=4) as executor:
            for d in date_list:
                executor.submit(process_single_date, d, waterfowl_dict)
                
    finally:
        # Clean up database resource pool pools when pipeline completes
        db_pool.closeall()
        print("\nMigration parallel data ingestion pipeline lifecycle complete!")

if __name__ == "__main__":
    run_pipeline()
