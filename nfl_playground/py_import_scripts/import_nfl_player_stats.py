import nflreadpy as nfl
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
import urllib.parse

# 1. Get the full connection string from the environment variable
conn_str = os.getenv('NEON_CONNECTION_STRING')

if not conn_str:
    # Fallback for local testing if you still use PASSWORD locally
    PASSWORD = os.getenv('DB_PASSWORD')
    if PASSWORD:
        encoded_password = urllib.parse.quote_plus(str(PASSWORD))
        conn_str = f'postgresql://github_app_user:{encoded_password}@ep-blue-scene-ap3o5e85-pooler.c-7.us-east-1.aws.neon.tech/neondb?sslmode=require'
    else:
        raise ValueError("Neither NEON_CONNECTION_STRING nor DB_PASSWORD found!")

# 2. Connect using the string
try:
    conn = psycopg2.connect(conn_str)
    print("Successfully connected to Neon!")
    # ... rest of your code ...
except Exception as e:
    print(f"Connection failed: {e}")

# 2. Load the NFL player stats
# Specify the seasons you want to load (e.g., 2023 and 2024)
print("Downloading NFL player stats...")
# load_player_stats returns a Polars DataFrame by default
player_stats_polars = nfl.load_player_stats(seasons=[2022, 2023, 2024, 2025])

# 3. Convert to Pandas for easy database writing
player_stats_df = player_stats_polars.to_pandas()

# 4. Connect and load into Neon
try:
    print("Connecting to Neon and loading data...")
    engine = create_engine(conn_str)
    
    # Write the data to a table named 'nfl_player_stats'
    # 'if_exists="replace"' will overwrite the table; use "append" to add data
    player_stats_df.to_sql(
        name='nfl_player_stats', 
        con=engine, 
        if_exists='replace', 
        index=False
    )
    print("Success! Data loaded into 'nfl_player_stats' table.")
    
except Exception as e:
    print(f"An error occurred: {e}")
