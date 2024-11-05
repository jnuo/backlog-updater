# src/data_processing.py

import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from config.settings import SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET
from src.google_sheets import read_google_sheet, authorize_google_sheets
from src.fetch_jira_csv import read_jira_csv

def transform_priority(priority):
    """
    Convert Jira priority to custom format for database.
    """
    priority_mapping = {
        "Blocker": "5-Blocker",
        "Critical": "4-Critical",
        "Major": "3-Major",
        "Minor": "2-Minor",
        "Trivial": "1-Trivial"
    }
    return priority_mapping.get(priority, priority)

def format_date(date_str):
    """
    Format date to "DD-MMM-YYYY" format, ensuring no time component is included.
    """
    if pd.isna(date_str) or date_str == "":
        return ""
    
    try:
        # Check if the input has time included and parse accordingly
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") if " " in date_str else datetime.strptime(date_str, "%Y-%m-%d")
        return parsed_date.strftime("%d-%b-%Y")
    except ValueError:
        # Try parsing common alternate formats
        try:
            parsed_date = datetime.strptime(date_str, "%d/%b/%y %I:%M %p")  # Format as shown in the issue
            return parsed_date.strftime("%d-%b-%Y")
        except ValueError:
            return date_str  # Return the original if parsing fails


def prepare_new_tasks(jira_df, database_df):
    """
    Find and prepare new tasks from Jira to be added to the database.
    """
    # Identify new tasks
    new_tasks_df = find_new_tasks(jira_df, database_df, jira_key_column="Issue key", database_key_column="TICKET")

    # Map and transform columns for new tasks
    new_tasks_df = new_tasks_df.rename(columns={
        "Issue key": "TICKET",
        "Status": "Status",
        "Issue Type": "Type",
        "Priority": "Priority",
        "Summary": "Summary",
        "Created": "Creation Date",
        "Resolved": "Resolved Date"
    })

    # Apply transformations
    new_tasks_df["Priority"] = new_tasks_df["Priority"].apply(transform_priority)
    new_tasks_df["Creation Date"] = new_tasks_df["Creation Date"].apply(format_date)
    new_tasks_df["Resolved Date"] = new_tasks_df["Resolved Date"].apply(format_date)

    # Add missing columns with default values to match Google Sheets structure
    expected_columns = [
        "TICKET", "CLIENT", "Type", "Priority", "Status", "Summary",
        "Creation Date", "SLA Limit", "SLA Deadline", "SLA Overdue Days",
        "Resolved Date", "Days to Complete", "Category (Optional)", "Comments", 
        "Duplicate ID", "Support Sheet?", "ticket-id", "url_concat", "url_text"
    ]
    for column in expected_columns:
        if column not in new_tasks_df.columns:
            new_tasks_df[column] = ""  # Fill missing columns with empty strings

    # Reorder columns to match the expected structure in Google Sheets
    new_tasks_df = new_tasks_df[expected_columns]

    return new_tasks_df

def append_new_tasks_to_database():
    """
    Append new tasks from Jira to the Google Sheets database and print them to the console.
    """
    # Load data from Jira CSV and Google Sheets database
    jira_data = read_jira_csv()
    database_data = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Prepare new tasks
    new_tasks_df = prepare_new_tasks(jira_data, database_data)
    
    if new_tasks_df.empty:
        print("No new tasks to append.")
        return

    # Select only the first new task to append
    first_new_task = new_tasks_df.head(1).fillna("").astype(str)
    # Print the first new task to confirm
    print("First new task to be added to the database:")
    print(first_new_task)

    # Append new tasks to Google Sheets
    client = authorize_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)

    # Convert DataFrame to list of lists and append to Google Sheets
    first_new_task_list = first_new_task.values.tolist()
    sheet.append_rows(first_new_task_list, value_input_option="USER_ENTERED")
    print("New tasks successfully appended to the Google Sheets database.")

def find_new_tasks(jira_df, database_df, jira_key_column="Issue key", database_key_column="TICKET"):
    """
    Find tasks in the Jira data that are not present in the Google Sheets database.
    
    Parameters:
        jira_df (pd.DataFrame): DataFrame containing Jira tasks.
        database_df (pd.DataFrame): DataFrame containing tasks from Google Sheets.
        jira_key_column (str): The column name for the unique identifier in Jira data (e.g., "Issue key").
        database_key_column (str): The column name for the unique identifier in the database (e.g., "TICKET").
    
    Returns:
        pd.DataFrame: DataFrame of tasks present in Jira but not in the database.
    """
    # Ensure both DataFrames have their respective key columns
    if jira_key_column not in jira_df.columns or database_key_column not in database_df.columns:
        print(f"Error: '{jira_key_column}' or '{database_key_column}' column not found in the data sources.")
        return pd.DataFrame()

    # Find tasks in Jira that are not in the database
    new_tasks_df = jira_df[~jira_df[jira_key_column].isin(database_df[database_key_column])]
    print(f"Found {len(new_tasks_df)} new tasks in Jira not present in the database.")
    return new_tasks_df

# Example usage
if __name__ == "__main__":
    append_new_tasks_to_database()


