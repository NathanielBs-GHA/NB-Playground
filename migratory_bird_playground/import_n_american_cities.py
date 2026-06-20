import os
import time
import requests
import psycopg2
from requests.exceptions import JSONDecodeError

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
            try:
                payload = response.json()
                cities_data = payload.get('data', [])
                
                if not cities_data:
                    print(f"Warning: No city data returned for {country_code}.")
                    return
                
                # Execute Database Insertion
                conn = psycopg2.connect(NEON_CONN_STRING)
                cursor = conn.cursor()
                for city in cities_data:
                    cursor.execute(
                        """
                        INSERT INTO na_cities (city_name, state_province, country, latitude, longitude, population)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (city['city'], city['region'], city['country'], city['latitude'], city['longitude'], city['population'])
                    )
                conn.commit()
                cursor.close()
                conn.close()
                print(f"Success: Imported {len(cities_data)} cities for {country_code}.")
                
            except JSONDecodeError:
                print(f"Parsing Failure: Response was not valid JSON.")
                print(f"Snippet of unexpected raw data: {response.text[:300]}")
        else:
            print(f"API Rejected Request. Error Code: {response.status_code}")
            print(f"Message payload: {response.text[:300]}")
            
    except Exception as e:
        print(f"Network error trying to process {country_code}: {str(e)}")

if __name__ == "__main__":
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: One or more environment secrets are missing or blank.")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        # Pace requests out by 2 seconds to safely pass RapidAPI's basic free-tier firewall
        time.sleep(2)
