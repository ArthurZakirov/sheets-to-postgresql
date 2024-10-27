import os
import requests
from googleapiclient.discovery import build
from src.utils.auth import authenticate
from dotenv import load_dotenv

load_dotenv()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
CSV_DIR = os.getenv("CSV_DIR")
os.makedirs(CSV_DIR, exist_ok=True)

def download_all_sheets_as_csv():
    creds = authenticate()
    service = build("sheets", "v4", credentials=creds)
    spreadsheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = spreadsheet_metadata.get("sheets", [])

    for sheet in sheets:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_title = sheet["properties"]["title"]
        url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={sheet_id}"
        headers = {"Authorization": f"Bearer {creds.token}"}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            output_filename = os.path.join(CSV_DIR, f"{sheet_title}.csv")
            with open(output_filename, 'wb') as f:
                f.write(response.content)
            print(f"Sheet '{sheet_title}' exported to {output_filename}")
        else:
            print(f"Error downloading sheet '{sheet_title}':", response.status_code, response.text)