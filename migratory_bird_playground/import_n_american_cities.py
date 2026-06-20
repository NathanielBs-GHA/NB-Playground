import os
import time
import requests
import psycopg2

# Load credentials securely from environment variables
API_KEY = os.getenv("X_RAPIDAPI_KEY")
NEON_CONN_STRING = os.getenv("NEON_CONNECTION_STRING")

API_URL = "https://rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": str(API_KEY).strip() if API_KEY else "",
    "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def import_cities(country_code):
    # API parameter key MUST be 'countryIds' (exact case-sensitivity match)
    params = {
        "countryIds": country_code, 
        "minPopulation": 50000, 
        "limit": 100
    }
    
    print(f"--- Processing {country_code} ---")
    try:
        response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        print(f"HTTP Status Received: {response.status_code}")
        
        if response.status_code == 200:
            payload = response.json()
            cities_data = payload.get('data', [])
            
            if not cities_data:
                print(f"Warning: No city data records returned for {country_code}.")
                return
            
            # Establish database pipeline connection
            conn = psycopg2.connect(NEON_CONN_STRING)
            cursor = conn.cursor()
            
            inserted_count = 0
            for city in cities_data:
                # FIX: GeoDB maps state/province to 'regionCode' or 'region', fallback handles both cleanly
                state_prov = city.get('regionCode') or city.get('region', 'N/A')
                
                cursor.execute(
                    """
                    INSERT INTO na_cities (city_name, state_province, country, latitude, longitude, population)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (city['city'], state_prov, city['country'], city['latitude'], city['longitude'], city['population'])
                )
                inserted_count += 1
                
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Success: Verified and queued {inserted_count} cities for {country_code}.")
        else:
            print(f"API Gateway rejected request. Status code: {response.status_code}")
            print(f"Payload trace: {response.text[:300]}")
            
    except Exception as e:
        print(f"Network error parsing context pipeline for {country_code}: {str(e)}")

if __name__ == "__main__":
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: Target environment variable string secrets are missing or blank.")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        time.sleep(2) # Prevent free-tier gateway firewalls from flagging the GitHub runner IP
