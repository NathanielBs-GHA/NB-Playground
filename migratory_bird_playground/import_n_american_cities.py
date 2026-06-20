import os
import time
import requests
import psycopg2
from requests.exceptions import JSONDecodeError

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
    # Modified parameters to minimize variables
    params = {
        "countryIds": country_code, 
        "minPopulation": 50000, 
        "limit": 10
    }
    
    print(f"--- Processing {country_code} ---")
    response = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
    print(f"HTTP Status Received: {response.status_code}")
    
    if response.status_code == 200:
        try:
            payload = response.json()
            cities_data = payload.get('data', [])
            
            if not cities_data:
                print(f"Warning: No city data records returned for {country_code}.")
                return
            
            conn = psycopg2.connect(NEON_CONN_STRING)
            cursor = conn.cursor()
            inserted_count = 0
            for city in cities_data:
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
            
        except JSONDecodeError:
            print("CRITICAL JSON ERROR: Server returned 200 OK but the response text is NOT valid JSON.")
            print("----------------- SERVER TEXT START -----------------")
            print(response.text[:1000])  # Expose raw text payload directly to the logs
            print("------------------ SERVER TEXT END ------------------")
            
        except Exception as db_err:
            print(f"Database error during insertion logic: {str(db_err)}")
    else:
        print(f"API Gateway rejected request with status code: {response.status_code}")
        print(f"Payload trace: {response.text[:300]}")

if __name__ == "__main__":
    if not API_KEY or not NEON_CONN_STRING:
        print("CRITICAL: Target environment variables are missing.")
        exit(1)
        
    for country in ['US', 'CA', 'MX']:
        import_cities(country)
        time.sleep(3)
