import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import google.generativeai as genai


load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


genai.configure(api_key=GEMINI_API_KEY)


uri = f"postgres://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

try:
    conn = psycopg2.connect(uri)
    df = pd.read_sql("SELECT * FROM customer_orders", conn)
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Compute total spend per order
    df['total_spend'] = df['price_per_unit'] * df['quantity']

    # 1. Top 5 customers by total spend
    top_customers = df.groupby('customer_name')['total_spend'].sum().sort_values(ascending=False).head(5)

    # 2. Most popular product (by quantity sold)
    popular_product = df.groupby('product_name')['quantity'].sum().sort_values(ascending=False)

    # 3. Average order value
    avg_order_value = df['total_spend'].mean()

    # 4. Weekly orders over past 8 weeks
    df['week'] = df['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
    cutoff_date = pd.Timestamp.today() - pd.Timedelta(weeks=8)
    last_8_weeks = df[df['order_date'] >= cutoff_date]
    weekly_orders = last_8_weeks.groupby('week').size().sort_index()

    # --- Build Data Summary for Gemini ---
    summary_lines = []

    summary_lines.append("Top 5 Customers by Total Spend:")
    for customer, spend in top_customers.items():
        summary_lines.append(f"- {customer}: ₹{spend:.2f}")

    most_popular = popular_product.index[0]
    summary_lines.append(f"\nMost Popular Product: {most_popular} with {popular_product.iloc[0]} units sold.")

    summary_lines.append(f"\nAverage Order Value: ₹{avg_order_value:.2f}")

    summary_lines.append("\nNumber of Orders Each Week (Last 8 Weeks):")
    for week, count in weekly_orders.items():
        summary_lines.append(f"- Week starting {week.strftime('%Y-%m-%d')}: {count} orders")

    data_summary = "\n".join(summary_lines)

    # Gemini prompt
    prompt = f"""
    Based on the following e-commerce data summary:

    {data_summary}

    1. Summarize key findings in 3-5 bullet points.
    2. Provide one marketing insight or action suggestion.
    """

    # Send to Gemini
    model = genai.GenerativeModel("models/gemini-1.5-flash-002")
    response = model.generate_content(prompt)

    print("\n📊 Data Summary Sent to Gemini:\n")
    print(data_summary)

    print("\n🧠 Gemini Insights:\n")
    print(response.text)

    conn.close()

except Exception as e:
    print("❌ Error:", e)
