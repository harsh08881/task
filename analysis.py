import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

# Build the PostgreSQL connection URI from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

uri = f"postgres://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
    # Connect to the database
    conn = psycopg2.connect(uri)

    # Fetch data into a DataFrame
    df = pd.read_sql("SELECT * FROM customer_orders", conn)

    # Ensure order_date is datetime
    df['order_date'] = pd.to_datetime(df['order_date'])

    print("✅ Data loaded successfully.\n")

    # 1. Top 5 customers by total spend
    df['total_spend'] = df['quantity'] * df['price_per_unit']
    top_customers = df.groupby('customer_name')['total_spend'].sum().sort_values(ascending=False).head(5)
    print("🏆 Top 5 Customers by Total Spend:")
    print(top_customers, "\n")

    # 2. Most popular product (by quantity sold)
    popular_product = df.groupby('product_name')['quantity'].sum().sort_values(ascending=False).head(1)
    print("📦 Most Popular Product (by Quantity Sold):")
    print(popular_product, "\n")

    # 3. Average order value
    avg_order_value = df['total_spend'].mean()
    print("💰 Average Order Value: ₹{:.2f}".format(avg_order_value), "\n")

    # 4. Number of orders each week for the past 8 weeks
    df['week'] = df['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
    last_8_weeks = df[df['order_date'] >= (pd.Timestamp.today() - pd.Timedelta(weeks=8))]
    weekly_orders = last_8_weeks.groupby('week').size()
    print("📈 Number of Orders Each Week (Last 8 Weeks):")
    print(weekly_orders)

    conn.close()

except Exception as e:
    print("❌ Error:", e)
