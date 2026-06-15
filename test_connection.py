import os
import psycopg2
from urllib.parse import quote_plus

# This works for both local .env and GitHub Actions environment variables
connection_string = os.getenv('NEON_CONNECTION_STRING')

if not connection_string:
    raise ValueError("DB_CONNECTION_STRING is not set!")

try:
    conn = psycopg2.connect(connection_string)
    print("Successfully connected to Neon!")
    # Your database logic here...
finally:
    if 'conn' in locals():
        conn.close()
