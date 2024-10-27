import os
import pandas as pd
from prefect import task, flow
from src.utils.google_sheets_to_csv import download_all_sheets_as_csv
from src.utils.database import load_to_postgresql

CSV_DIR = os.getenv("CSV_DIR")

@task
def extract_csv_files(input_dir: str) -> list:
    csv_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.csv')]
    return csv_files

@task
def transform_data(file_path: str) -> pd.DataFrame:
    data = pd.read_csv(file_path)
    # Add any transformation logic here (e.g., clean up, data manipulation)
    data["processed"] = True  # Example transformation
    return data

@task
def load_data_to_db(data: pd.DataFrame, table_name: str):
    load_to_postgresql(data, table_name)

@flow
def etl_flow():
    # Extract phase
    download_all_sheets_as_csv()
    csv_files = extract_csv_files(CSV_DIR)
    
    # Transform and load phase
    for file_path in csv_files:
        sheet_name = os.path.splitext(os.path.basename(file_path))[0]  # use filename as table name
        data = transform_data(file_path)
        load_data_to_db(data, sheet_name)

if __name__ == "__main__":
    etl_flow()  # Run the flow locally
