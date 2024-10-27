import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")  # Username for connecting to PostgreSQL
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  # Password for the user
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")  # Database host
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")  # Database port
DB_NAME = os.getenv("DB_NAME")  # The name of the database you want to create

# Connect to the 'postgres' database to check and create the target database
ADMIN_CONNECTION_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"
admin_engine = create_engine(ADMIN_CONNECTION_URL, isolation_level="AUTOCOMMIT")

def database_exists(db_name):
    """Check if a PostgreSQL database exists."""
    with admin_engine.connect() as connection:
        result = connection.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"))
        return result.scalar() is not None

def create_database_if_not_exists(db_name):
    """Create the PostgreSQL database if it doesn't already exist."""
    if not database_exists(db_name):
        print(f"Database '{db_name}' does not exist. Creating it now.")
        with admin_engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {db_name}"))
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists. Skipping creation.")

def load_to_postgresql(data: pd.DataFrame, table_name: str):
    """Load data into PostgreSQL table."""
    # After ensuring the database exists, connect directly to it
    DB_CONNECTION_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DB_NAME}"
    db_engine = create_engine(DB_CONNECTION_URL)

    try:
        # Load data into the specified table
        data.to_sql(table_name, db_engine, if_exists="replace", index=False)
        print(f"Data loaded to PostgreSQL table '{table_name}'")
    except OperationalError as e:
        print("An error occurred while connecting to the database:", e)