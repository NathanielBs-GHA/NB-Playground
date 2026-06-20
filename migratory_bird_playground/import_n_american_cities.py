import os
import time
import requests
import psycopg2

# 1. Map exactly to the environment variables exposed in your workflow log
API_KEY = os.getenv("X_RAPIDAPI_KEY")
NEON_CONN_STRING = os.getenv("NEON_CONNECTION_STRING")

# FIX: Changed endpoint to /v1/geo/cities to correctly search for cities
API_URL = "https://rapidapi.com"

HEADERS = {
    "X-RapidAPI-Key": str(API_KEY).strip() if API_KEY else "",
    "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com",
    "Accept": "application/json"
}

def import_cities(country_code):
    # Parameters for the /v1/geo/cities endpoint
    params = {
        "countryIds": country_code,
        "limit": 50
    }
    
    print(f"\n--- Processing {country_code} ---")
    
    try:
        response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        print(f"HTTP Status Received: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        return
    
    if response.status_code == 200:
        # Stop early if the content type is HTML instead of JSON
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            print(f"Error: Server returned an HTML web page instead of JSON.")
            print(f"Ensure your X_RAPIDAPI_KEY secret is valid and not empty.")
            return

        if not response.text.strip():
            print(f"Error: Server returned an empty response body.")
            return
            
        try:
            payload = response.json()
        except ValueError:
            print(f"Error: Failed to decode JSON. Preview: {response.text[:200]}")
            return

        # Connect to Neon
        try:
            conn = psycopg2.connect(NEON_CONN_STRING)
            cursor = conn.cursor()
            inserted_count = 0
            
            for city in cities_data:
                # Fallback schema handling for regions/states
                state_prov = city.get('regionCode') or city.get('region') or 'N/A'
                population = city.get('population') or 0
                
                # Using city.get() to safely avoid KeyError if fields are missing
                cursor.execute(
                    """
                    INSERT INTO na_cities (city_name, state_province, country, latitude, longitude, population)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (city.get('city', 'Unknown'), state_prov, city.get('country'), city.get('latitude'), city.get('longitude'), population)
                )
                inserted_count += 1
                
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Success: Imported {inserted_count} cities for {country_code}.")
            
        except psycopg2.Error as db_err:
            print(f"Database error occurred: {db_err}")
            
    else:
        print(f"Failed. Status code: {response.status_code}")
        print(f"Details: {response.text[:200]}")

if __name__ == "__main__":
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: Target environment variable string secrets are missing or blank.")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        time.sleep(3)  # Safe pacing delay for the free-tier gateway
