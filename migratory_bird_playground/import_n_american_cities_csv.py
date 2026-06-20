import os
import zipfile
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Grab this from your Neon Console
#PLEASE NOTE: Never share your actual DATABASE_URL in public code or forums. 
# Use environment variables or secure vaults to manage sensitive information or execute this locally.
DATABASE_URL = "YOUR_NEON_DATABASE_URL_HERE"

# Define North American Country ISO codes
NORTH_AMERICA_ISO = {
    'US', 'CA', 'MX', 'GL', 'BM', 'PM',
    'CR', 'SV', 'GT', 'HN', 'NI', 'PA', 'BZ',
    'AG', 'BS', 'BB', 'CU', 'DM', 'DO', 'GD', 'HT', 'JM', 'KN', 'LC', 'VC', 'TT',
    'AI', 'AW', 'BQ', 'KY', 'CW', 'GP', 'MQ', 'MS', 'PR', 'BL', 'MF', 'SX', 'TC', 'VG', 'VI'
}

def load_local_zip_data():
    # Target the unzipped folder path shown in your terminal output
    csv_path = "YOUR_UNZIPPED_FOLDER_PATH/worldcities.csv"  # Update this to your actual unzipped path
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Could not find worldcities.csv at the explicit path:\n{csv_path}"
        )
            
    print(f"Reading local CSV file: {csv_path}")
    
    # Read the CSV file directly from your unzipped directory
    df = pd.read_csv(csv_path)
            
    print("Filtering rows for North American cities...")
    df_filtered = df[df['iso2'].isin(NORTH_AMERICA_ISO)].copy()
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    
    print(f"Successfully processed {len(df_filtered)} North American records.")
    return df_filtered

def insert_to_neon(df):
    print("Connecting to Neon PostgreSQL database...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Lean Query targeting exclusively your 7 requested columns
    insert_query = """
        INSERT INTO world_cities (
            id, city, city_ascii, lat, lng, country, same_name
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """
    
    # Safely pull the targeted values row-by-row
    data_tuples = [
        (
            row.get('id'), 
            row.get('city'), 
            row.get('city_ascii'), 
            row.get('lat'), 
            row.get('lng'),
            row.get('country'), 
            row.get('same_name')
        )
        for _, row in df.iterrows()
    ]
    
    try:
        print("Executing fast batch array insert into Neon...")
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        print(f"Data imported successfully! {len(data_tuples)} records processed.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during database processing: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    cities_df = load_local_zip_data()
    insert_to_neon(cities_df)
