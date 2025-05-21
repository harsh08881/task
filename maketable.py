import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()

# Read DB credentials from .env
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

def create_table():
    try:
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        # SQL to create table
        create_query = """
        CREATE TABLE IF NOT EXISTS customer_orders (
            id SERIAL PRIMARY KEY,
            customer_name TEXT,
            product_name TEXT,
            quantity INTEGER,
            price_per_unit FLOAT,
            order_date TIMESTAMP
        );
        """

        cur.execute(create_query)
        conn.commit()
        print("✅ Table 'customer_orders' created successfully.")

    except Exception as e:
        print("❌ Error:", e)

    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    create_table()
