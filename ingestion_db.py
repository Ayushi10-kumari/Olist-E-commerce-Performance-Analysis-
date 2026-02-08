import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import time
import os# --- Configure logging ---
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
# --- PostgreSQL credentials ---
PG_USER = 'postgres'
PG_PASSWORD = 'ayushi625'
PG_HOST = 'localhost'
PG_PORT = 5432  
DB_NAME = 'inventory_db'

def create_database_if_not_exists():
    """Creates the inventory_db database if it doesn't exist."""
    try:
        # Connect to the default 'postgres' database to check/create DB
        conn = psycopg2.connect(
            dbname='postgres',
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT  
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if the database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            logging.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logging.info(f"Database '{DB_NAME}' already exists.")

        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Failed to create database: {e}")
        raise
def get_engine():
    """Returns SQLAlchemy engine connected to the inventory_db database."""
    return create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}')

def ingest_db(df, table_name, engine):
    """Ingests the DataFrame into the specified PostgreSQL table."""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' ingested successfully.")
def load_raw_data():
    """Loads all CSV files from the data/ folder and ingests them into the DB."""
    start = time.time()
    logging.info("Starting ingestion process...")

    # Ensure data folder exists
    if not os.path.exists('data'):
        logging.error("Directory 'data' does not exist.")
        return

    engine = get_engine()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('data', file))
                table_name = file[:-4]  # Remove .csv extension
                logging.info(f"Ingesting {file} into table {table_name}")
                ingest_db(df, table_name, engine)
            except Exception as e:
                logging.error(f"Failed to ingest {file}: {e}")
    end = time.time()
    total = (end - start) / 60
    logging.info("Ingestion Complete")
    logging.info(f"Total Time Taken: {total:.2f} minutes")

if __name__ == '__main__':
    create_database_if_not_exists()
    load_raw_data()