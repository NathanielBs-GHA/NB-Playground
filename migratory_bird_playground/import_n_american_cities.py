import requests
import psycopg2
import os

# 1. Point to the collection endpoint, NOT the distance calculator
API_URL = "https://rapidapi.com"

# Derived directly from the working headers layout in your screenshot
HEADERS = {
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# 2. Neon Database Connection
# Copy your exact connection string from the Neon Console dashboard
NEON_CONN_STRING = os.getenv("NEON_CONNECTION_STRING")

def import_cities(country_code):
    # Fetch top populated cities for the given country code (e.g., 'US', 'CA', 'MX')
    params = {"countryIds": country_code, "minPopulation": 50000, "limit": 100}
    response = requests.get(API_URL, headers=HEADERS, params=params)
    
    if response.status_code == 200:
        cities_data = response.json().get('data', [])
        
        # Connect to Neon
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
        print(f"Successfully imported cities for {country_code}")
    else:
        print(f"Failed to fetch data for {country_code}: {response.text}")

# Run import for major North American countries
for country in ['US', 'CA', 'MX']:
    import_cities(country)
