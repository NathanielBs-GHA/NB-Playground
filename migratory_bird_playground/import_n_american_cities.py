import os
import time
import requests
import psycopg2

# 1. Grab environment variables exposed by your GitHub Actions workflow
API_KEY = os.getenv("X_RAPIDAPI_KEY")
NEON_CONN_STRING = os.getenv("NEON_CONNECTION_STRING")

# 2. Define the API endpoint and headers for the GeoDB Cities API
API_URL = "http://geodb-cities-api.wirefreethought.com/v1/geo/cities"

HEADERS = {
    "Accept": "application/json"
}


def import_cities(country_code):
    # API lookup parameters targeting 50 records per country code
    params = {
        "countryIds": country_code,
        "limit": 50
    }
    
    print(f"\n--- Processing {country_code} ---")
    
    try:
        response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        print(f"HTTP Status Received: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Network error trying to contact GeoDB API: {e}")
        return

    # Check for successful response
    if response.status_code == 200:
        # Detect if RapidAPI returned an HTML gateway page instead of raw data
        # Enhanced HTML Debugging Block
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            print("Error: Server returned an HTML web page instead of JSON.")
            # This extracts the HTML title tag or first 500 characters so you can read the error
            import re
            title = re.search(r'<title>(.*?)</title>', response.text, re.IGNORECASE)
            page_title = title.group(1) if title else "No Title Found"
            print(f"HTML Page Title: {page_title}")
            print(f"Raw Snippet: {response.text[:500]}")
            return

        if not response.text.strip():
            print("Error: Server returned a 200 OK but the response text is empty.")
            return
            
        try:
            payload = response.json()
        except ValueError:
            print(f"Error: Failed to safely parse JSON. Raw body preview: {response.text[:200]}")
            return

        cities_data = payload.get('data', [])
        if not cities_data:
            print(f"Warning: No city data records found inside payload for {country_code}.")
            print(f"Server response payload: {payload}")
            return

        # Connect to your Neon Postgres Instance
        try:
            conn = psycopg2.connect(NEON_CONN_STRING)
            cursor = conn.cursor()
            inserted_count = 0
            
            for city in cities_data:
                # Safe checks for nested region structures
                state_prov = city.get('regionCode') or city.get('region') or 'N/A'
                population = city.get('population') or 0
                
                cursor.execute(
                    """
                    INSERT INTO na_cities (city_name, state_province, country, latitude, longitude, population)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (
                        city.get('city', 'Unknown'), 
                        state_prov, 
                        city.get('country', country_code), 
                        city.get('latitude'), 
                        city.get('longitude'), 
                        population
                    )
                )
                inserted_count += 1
                
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Success: Safely imported {inserted_count} cities for {country_code} into Neon.")
            
        except psycopg2.Error as db_err:
            print(f"Database error occurred while interacting with Neon: {db_err}")
            
    elif response.status_code == 401 or response.status_code == 403:
        print(f"Authentication Failed ({response.status_code}): Your RapidAPI Key was rejected.")
        print(f"Gateway Response: {response.text[:150]}")
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print(f"Details: {response.text[:200]}")

if __name__ == "__main__":
    # Immediate pre-flight check to verify environmental settings
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: One or more target environment string variables are missing or completely blank.")
        print(f"X_RAPIDAPI_KEY Loaded: {'YES (Hidden)' if API_KEY else 'NO'}")
        print(f"NEON_CONNECTION_STRING Loaded: {'YES (Hidden)' if NEON_CONN_STRING else 'NO'}")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        time.sleep(3)  # Pacing pause to ensure free-tier compliance
