import os
import argparse
import requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Load environment variables
load_dotenv()
TOKEN_PATH = os.getenv("TOKEN_PATH")
CRED_PATH = os.getenv("CRED_PATH")
SCOPES = os.getenv("SCOPES").split(",")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
OUTPUT_DIR = "data/raw"

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def authenticate(token_path, credentials_path, scopes):
    """Authenticate and return Google API credentials."""
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as token_file:
            token_file.write(creds.to_json())
    return creds

def download_sheet_as_csv(credentials, spreadsheet_id, sheet_id, sheet_title):
    """Download a single sheet as a CSV file."""
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={sheet_id}"
    headers = {"Authorization": f"Bearer {credentials.token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        output_filename = os.path.join(OUTPUT_DIR, f"{sheet_title}.csv")
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        print(f"Sheet '{sheet_title}' exported to {output_filename}")
    else:
        print(f"Error downloading sheet '{sheet_title}':", response.status_code, response.text)

def main():
    creds = authenticate(TOKEN_PATH, CRED_PATH, SCOPES)
    service = build("sheets", "v4", credentials=creds)

    # Retrieve spreadsheet metadata to get sheet names and IDs
    spreadsheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = spreadsheet_metadata.get("sheets", [])

    # Loop through each sheet and download it as a CSV
    for sheet in sheets:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_title = sheet["properties"]["title"]
        download_sheet_as_csv(creds, SPREADSHEET_ID, sheet_id, sheet_title)

if __name__ == "__main__":
    main()