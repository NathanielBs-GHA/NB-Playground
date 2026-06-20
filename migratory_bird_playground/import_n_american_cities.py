import os
import time
import requests
import psycopg2

# 1. Map exactly to the environment variables exposed in your workflow log
API_KEY = os.getenv("X_RAPIDAPI_KEY")
NEON_CONN_STRING = os.getenv("NEON_CONNECTION_STRING")

API_URL = "https://wft-geo-db.p.rapidapi.com/v1/geo/places/Q60/distance?toPlaceId=Q60m"

HEADERS = {
    "X-RapidAPI-Key": str(API_KEY).strip() if API_KEY else "",
    "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def import_cities(country_code):
    # Adjusted parameters for reliable free-tier responses
    params = {
        "countryIds": country_code,
        "limit": 50
    }
    
    print(f"\n--- Processing {country_code} ---")
    response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
    print(f"HTTP Status Received: {response.status_code}")
    
    if response.status_code == 200:
        payload = response.json()
        cities_data = payload.get('data', [])
        
        if not cities_data:
            print(f"Warning: No city data records found for {country_code}.")
            print(f"Raw Response Body: {response.text[:200]}")
            return
        
        # Connect to Neon
        conn = psycopg2.connect(NEON_CONN_STRING)
        cursor = conn.cursor()
        inserted_count = 0
        
        for city in cities_data:
            # Fallback schema handling for regions/states
            state_prov = city.get('regionCode') or city.get('region') or 'N/A'
            population = city.get('population') or 0
            
            cursor.execute(
                """
                INSERT INTO na_cities (city_name, state_province, country, latitude, longitude, population)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (city['city'], state_prov, city['country'], city['latitude'], city['longitude'], population)
            )
            inserted_count += 1
            
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Success: Imported {inserted_count} cities for {country_code}.")
    else:
        print(f"Failed. Status code: {response.status_code}")
        print(f"Details: {response.text[:200]}")

if __name__ == "__main__":
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: Target environment variable string secrets are missing or blank.")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        time.sleep(3) # Safe pacing delay for the free-tier gateway
