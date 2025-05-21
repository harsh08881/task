import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Read DB credentials from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Initialize Faker and constants
fake = Faker()
NUM_ROWS = 500
PRODUCTS = [
    ("Laptop", 700, 1500),
    ("Smartphone", 300, 1000),
    ("Headphones", 30, 200),
    ("Monitor", 100, 400),
    ("Keyboard", 20, 100),
    ("Mouse", 10, 60)
]

def generate_random_date():
    """Return a random datetime within the last 90 days."""
    return datetime.now() - timedelta(days=random.randint(0, 89))

def generate_data():
    """Generate a list of tuples with fake customer purchase data."""
    data = []
    for _ in range(NUM_ROWS):
        customer = fake.name()
        product, min_price, max_price = random.choice(PRODUCTS)
        price = round(random.uniform(min_price, max_price), 2)
        quantity = random.randint(1, 5)
        date = generate_random_date()
        data.append((customer, product, quantity, price, date))
    return data

def insert_data(data):
    """Insert generated data into PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()

       
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_orders (
                id SERIAL PRIMARY KEY,
                customer_name TEXT,
                product_name TEXT,
                quantity INTEGER,
                price_per_unit FLOAT,
                order_date TIMESTAMP
            )
        """)

      
        insert_query = """
            INSERT INTO customer_orders (customer_name, product_name, quantity, price_per_unit, order_date)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.executemany(insert_query, data)
        conn.commit()

        print(f"Inserted {len(data)} rows successfully into 'customer_orders'.")

    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    data = generate_data()
    insert_data(data)
