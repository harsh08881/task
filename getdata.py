import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read PostgreSQL credentials from .env
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# PostgreSQL URI
uri = f"postgres://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
    conn = psycopg2.connect(uri)
    cursor = conn.cursor()

    # Fetch all rows ordered by order_date
    cursor.execute("SELECT * FROM customer_orders ORDER BY order_date DESC;")
    rows = cursor.fetchall()

    # Get and print column names
    colnames = [desc[0] for desc in cursor.description]
    print(" | ".join(colnames))
    print("-" * 80)

    # Print each row
    for row in rows:
        print(" | ".join(str(item) for item in row))

    cursor.close()
    conn.close()
except Exception as e:
    print("❌ Error:", e)
