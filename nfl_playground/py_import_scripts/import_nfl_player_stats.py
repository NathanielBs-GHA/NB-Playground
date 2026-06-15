import nflreadpy as nfl
import pandas as pd
from sqlalchemy import create_engine
import os
import urllib.parse

# 1. Database connection settings
# Get these from your Neon project dashboard (Connect button)
USERNAME = os.getenv("DB_USERNAME")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB_NAME")
encoded_password = urllib.parse.quote_plus(PASSWORD)

# Construct the Neon connection string
# Neon requires 'sslmode=require' for secure connections

# Safely encode the password

conn_str = f'postgresql://github_app_user:{encoded_password}@ep-blue-scene-ap3o5e85-pooler.c-7.us-east-1.aws.neon.tech/neondb?sslmode=require'

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
