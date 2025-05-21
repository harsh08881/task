import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

uri = f"postgres://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
  
    conn = psycopg2.connect(uri)

  
    df = pd.read_sql("SELECT * FROM customer_orders", conn)

   
    df['order_date'] = pd.to_datetime(df['order_date'])

    print("✅ Data loaded successfully.\n")

    
    df['total_spend'] = df['quantity'] * df['price_per_unit']
    top_customers = df.groupby('customer_name')['total_spend'].sum().sort_values(ascending=False).head(5)
    print("🏆 Top 5 Customers by Total Spend:")
    print(top_customers, "\n")

   
    popular_product = df.groupby('product_name')['quantity'].sum().sort_values(ascending=False).head(1)
    print("📦 Most Popular Product (by Quantity Sold):")
    print(popular_product, "\n")

   
    avg_order_value = df['total_spend'].mean()
    print("💰 Average Order Value: ₹{:.2f}".format(avg_order_value), "\n")

    
    df['week'] = df['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
    last_8_weeks = df[df['order_date'] >= (pd.Timestamp.today() - pd.Timedelta(weeks=8))]
    weekly_orders = last_8_weeks.groupby('week').size()
    print("📈 Number of Orders Each Week (Last 8 Weeks):")
    print(weekly_orders)

    conn.close()

except Exception as e:
    print("❌ Error:", e)
