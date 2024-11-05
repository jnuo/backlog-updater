import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from config.settings import SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET, SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_MAINSHEET, CREDENTIALS_FILE

def authorize_google_sheets():
    """
    Authorize and return a Google Sheets client.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(credentials)
    return client

def read_google_sheet(spreadsheet_id, sheet_name):
    """
    Read a specific Google Sheets document and return it as a DataFrame.
    
    Parameters:
        spreadsheet_id (str): The ID of the Google Sheets document.
        sheet_name (str): The name of the sheet within the document to read.
    
    Returns:
        pd.DataFrame: The data from the specified sheet as a DataFrame.
    """
    client = authorize_google_sheets()
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    return df

def print_summary(df, description="Data"):
    """
    Print a summary of the Google Sheets data.
    
    Parameters:
        df (pd.DataFrame): The DataFrame containing the sheet data.
        description (str): A description of the data being summarized.
    """
    print(f"Summary of {description}:")
    print("Number of tasks:", len(df))
    print("Columns:", df.columns.tolist())
    print("Sample data:")
    print(df.head())

# Example usage
if __name__ == "__main__":
    # Load data from the main database sheet
    database_sheet_name = "Backend/Frontend"  # Replace with the actual sheet name in the database document
    database_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)
    print_summary(database_df, description="BU VIDEO PRODUCT BACKLOG DATABASE")

    # Load data from the summary sheet
    summary_sheet_name = "BackendFrontend"  # Replace with the actual sheet name in the summary document
    summary_df = read_google_sheet(SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_MAINSHEET)
    print_summary(summary_df, description="BU VIDEO Key Issues 2024")
