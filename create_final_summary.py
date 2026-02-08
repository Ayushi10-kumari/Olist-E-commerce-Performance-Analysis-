import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from datetime import datetime
from ingestion_db import ingest_db  # Make sure this module exists

# Setup logging
logging.basicConfig(
    filename="logs/olist_etl.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Function to connect to the database
def create_db_connection(user, password, host, port, db_name):
    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")
        logging.info("Database connection established.")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

# Function to create summary
def create_summary(engine):
    query = """
     SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp::timestamp) AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(r.review_score)::numeric, 2) AS avg_review_score,
    ROUND(AVG(p.payment_value)::numeric, 2) AS avg_payment_value,
    ROUND(
        AVG(EXTRACT(DAY FROM o.order_delivered_customer_date::timestamp - o.order_purchase_timestamp::timestamp))::numeric,
        2
    ) AS avg_shipping_days,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(DISTINCT i.seller_id) AS unique_sellers
FROM olist_orders_dataset o
LEFT JOIN olist_order_reviews_dataset r ON o.order_id = r.order_id
LEFT JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
LEFT JOIN olist_order_items_dataset i ON o.order_id = i.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY month
ORDER BY month;


    """
    try:
        df = pd.read_sql(query, engine)
        df.to_sql("olist_final_analysis", engine, index=False, if_exists="replace")
        df.to_csv("olist_final_analysis.csv", index=False)
        logging.info("Final summary table created successfully.")
        return df
    except Exception as e:
        logging.error(f"Error creating summary table: {e}")
        raise

# Function to clean the summary dataframe
def clean_data(df):
    logging.info(f"Original DataFrame shape: {df.shape}")

    df.columns = [col.strip().lower() for col in df.columns]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    df.drop_duplicates(inplace=True)

    datetime_columns = ['month']
    for col in datetime_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logging.info(f"Converted column to datetime: {col}")
            except Exception as e:
                logging.warning(f"Could not convert {col} to datetime: {e}")

    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.info(f"Null values found:\n{null_counts[null_counts > 0]}")

    logging.info(f"Cleaned DataFrame shape: {df.shape}")
    return df

# Optional: List available tables
def list_tables(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Available tables in DB:")
    for table in tables:
        print(table)

# Main execution
if __name__ == "__main__":
    engine = create_db_connection(
        user="postgres",
        password="ayushi625",
        host="localhost",
        port=5432,
        db_name="inventory_db"
    )

    list_tables(engine)  # Optional debug step

    logging.info("Creating final summary table...")
    summary_df = create_summary(engine)

    logging.info("Cleaning data...")
    clean_df = clean_data(summary_df)

    logging.info("Ingesting cleaned data into DB...")
    ingest_db(clean_df, "olist_final_analysis", engine)

    logging.info("ETL process completed successfully.")
