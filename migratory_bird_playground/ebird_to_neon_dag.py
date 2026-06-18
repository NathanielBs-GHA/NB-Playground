from datetime import datetime, timedelta
import logging
import time
import requests
import psycopg
import os
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Configuration
EBIRD_API_KEY = os.getenv("EBIRD_API_KEY")
REGIONS = [
    "US-IL", "US-IN", "US-IA", "US-MI", "US-MN", "US-MO", "US-OH", "US-WI",
    "CA-AB", "CA-SK", "CA-MB", "CA-ON", "CA-NT", "CA-NU", "CA-YT"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_schedule_cadence(**context):
    """
    Evaluates the execution date to enforce the conditional schedule:
    - Feb to Aug: Runs ONLY on the 1st day of the month.
    - Sept to Jan: Runs EVERY day.
    """
    exec_date = context['logical_date']
    current_month = exec_date.month
    current_day = exec_date.day

    logging.info(f"Evaluating cadence for Date: {exec_date.strftime('%Y-%m-%d')} (Month: {current_month}, Day: {current_day})")

    # Feb (2) through August (8)
    if 2 <= current_month <= 8:
        if current_day == 1:
            logging.info("February-August cadence: First day of the month. Proceeding with task execution.")
            return True
        else:
            logging.info("February-August cadence: Not the first day of the month. Skipping downstream tasks.")
            return False
            
    # September (9) through January (1)
    else:
        logging.info("September-January cadence: Daily execution window active. Proceeding with task execution.")
        return True


def fetch_and_load_ebird_data():
    """
    Fetches bird data from eBird API across the regions array and loads it into Neon.
    """
    # 1. Fetch data from eBird
    headers = {"X-eBirdApiToken": EBIRD_API_KEY}
    compiled_records = []

    for region in REGIONS:
        logging.info(f"Fetching recent observations for region: {region}")
        url = f"https://ebird.org{region}/recent"
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                for obs in data:
                    # Append flat dictionary tuple matching database columns
                    compiled_records.append((
                        obs.get('subId'),
                        obs.get('speciesCode'),
                        obs.get('comName'),
                        obs.get('sciName'),
                        obs.get('locId'),
                        obs.get('locName'),
                        obs.get('obsDt'),
                        obs.get('howMany', 0),
                        region
                    ))
            else:
                logging.error(f"Failed to fetch data for {region}. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Error connecting to eBird API for {region}: {e}")
            
        # Respectful 1-second API sleep cooldown 
        time.sleep(1.0)

    if not compiled_records:
        logging.info("No records pulled from eBird API. Skipping database upsert.")
        return

    # 2. Extract database connection URI from Airflow Connection
    # It safely converts an Airflow connection setup directly into a PostgreSQL URI string
    conn_obj = Connection.get_connection_from_secrets('neon_db')
    pg_uri = f"postgresql://{conn_obj.login}:{conn_obj.password}@{conn_obj.host}:{conn_obj.port or 5432}/{conn_obj.schema}"

    # 3. Connect to Neon and load data
    # 'with' handles safe commits and connections opening/closing automatically
    with psycopg.connect(pg_uri) as conn:
        with conn.cursor() as cur:
            # Create landing table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ebird_observations (
                    submission_id VARCHAR(50),
                    species_code VARCHAR(10),
                    common_name VARCHAR(100),
                    scientific_name VARCHAR(100),
                    location_id VARCHAR(50),
                    location_name TEXT,
                    observation_date TIMESTAMP,
                    count INTEGER,
                    region_code VARCHAR(15),
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (submission_id, species_code)
                );
            """)
            
            # Perform a batch upsert to ensure clean daily data loading without duplicates
            upsert_query = """
                INSERT INTO ebird_observations 
                (submission_id, species_code, common_name, scientific_name, location_id, location_name, observation_date, count, region_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (submission_id, species_code) 
                DO UPDATE SET 
                    count = EXCLUDED.count,
                    observation_date = EXCLUDED.observation_date,
                    ingested_at = CURRENT_TIMESTAMP;
            """
            
            logging.info(f"Loading {len(compiled_records)} records into Neon database.")
            cur.executemany(upsert_query, compiled_records)
            conn.commit() # Save changes to cloud DB permanently
            logging.info("Database transaction committed successfully.")


# DAG Definition
# Run on a daily cron loop so the schedule gate operator evaluates every day at midnight
with DAG(
    'ebird_waterfowl_to_neon',
    default_args=default_args,
    description='Fetches eBird waterfowl tracking data and upserts into Neon Postgres',
    schedule_interval='0 0 * * *', 
    catchup=False, # Set to True if you want to automatically backfill skipped historic periods
) as dag:

    # Gating Task: Dictates if downstream processes proceed based on calendar date conditions
    gate_schedule = ShortCircuitOperator(
        task_id='gate_schedule_cadence',
        python_callable=check_schedule_cadence,
        provide_context=True,
    )

    # Execution Task: Extraction & Data Warehousing
    etl_job = PythonOperator(
        task_id='fetch_and_load_data',
        python_callable=fetch_and_load_data,
    )

    # Task dependency pipeline
    gate_schedule >> etl_job
