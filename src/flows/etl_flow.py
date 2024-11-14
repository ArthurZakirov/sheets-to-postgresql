import os
import requests
from io import BytesIO
from prefect import task, flow
from googleapiclient.discovery import build
from dotenv import load_dotenv
from src.utils.auth import authenticate

load_dotenv()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
STAGING_DIR = os.getenv("STAGING_DIR")

@task
def extract(spreadsheet_id):
    """Extracts all sheets from the Google Sheets document and returns them as in-memory CSV files."""
    creds = authenticate()
    service = build("sheets", "v4", credentials=creds)
    spreadsheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet_metadata.get("sheets", [])

    extracted_sheets = []

    for sheet in sheets:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_title = sheet["properties"]["title"]
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={sheet_id}"
        headers = {"Authorization": f"Bearer {creds.token}"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            csv_data = BytesIO(response.content)
            extracted_sheets.append((sheet_title, csv_data))
            print(f"Sheet '{sheet_title}' extracted successfully")
        else:
            print(f"Error downloading sheet '{sheet_title}':", response.status_code, response.text)

    return extracted_sheets


@task
def load(extracted_sheets, STAGING_DIR):
    """Loads the in-memory CSV files to the specified directory."""
    
    # Create the staging directory if it doesn't exist
    if not os.path.exists(STAGING_DIR):
        os.makedirs(STAGING_DIR)
    
    for sheet_title, csv_data in extracted_sheets:
        output_filename = os.path.join(STAGING_DIR, f"{sheet_title}.csv")
        with open(output_filename, 'wb') as f:
            f.write(csv_data.getvalue())
        print(f"Sheet '{sheet_title}' saved to {output_filename}")

@task
def transform():
    pass

@flow
def etl_flow():
    extracted_sheets = extract(SPREADSHEET_ID)
    load(extracted_sheets, STAGING_DIR)
    transform()

if __name__ == "__main__":
    etl_flow()  
