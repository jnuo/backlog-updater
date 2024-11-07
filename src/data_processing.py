# src/data_processing.py

import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np
from config.settings import SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET, SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_PLUGINSHEET, SPREADSHEET_DATABASE_PLUGINDONESHEET, SPREADSHEET_KEY_ISSUES_MAINSHEET
from src.google_sheets import read_google_sheet, authorize_google_sheets
from src.fetch_jira_csv import read_jira_csv

from datetime import datetime, timedelta

CLIENT_LABELS = {
    "DT": "Deutsche Telekom",
    "dtgroup": "Deutsche Telekom",
    "globo": "Globo",
    "rtlgermany": "RTL",
    "vodafone": "Vodafone",
    "go3": "GO3",
    "avatel": "Avatel",
    "blutv": "BluTV",
    "multitv": "Multi TV",
    "orange": "Orange",
    "shahid": "Shahid",
    "pluto": "Pluto TV",
    "plutotv": "Pluto TV",
    "tv4": "TV4",
    "tv2no": "TV2 Norway",
    "mediacorp": "Mediacorp",
    "skybrazil": "Sky Brazil",
    "foxsportsaustralia": "Fox Sport AU",
    "movistargo": "Moviestar GO"
}

def map_plugin_task_fields(row):
    """
    Map fields from the Plugins(All) sheet to the PluginDone sheet format.
    """
    return {
        "Ticket": row.get("Ticket"),
        "Client": row.get("Client"),
        "Type": row.get("Type"),
        "Priority": row.get("Priority"),
        "QA/Release Status": row.get("Status"),
        "Platform": row.get("PluginPlatform"),
        "PluginVersion": row.get("PluginVersion"),
        "Summary": row.get("Summary"),
        "Deadline": row.get("Deadline"),
        "ETA": row.get("ETA")
    }

def move_done_tasks_to_archive():
    """
    Remove 'Done' or 'Released' tasks from the Plugins(All) sheet and move them to the PluginDone sheet.
    """
    print("Step 6: Remove Done tasks from Key Issues - move them to Database")

    # Load Plugins(All) sheet data
    plugins_df = read_google_sheet(SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_PLUGINSHEET)
    # Load PluginDone sheet data for appending
    plugin_done_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_PLUGINDONESHEET)

    # Filter for tasks with Status 'Done' or 'Released'
    done_or_released_df = plugins_df[plugins_df["Status"].isin(["Done", "Released"])]

    # Map and append filtered tasks to PluginDone sheet
    if not done_or_released_df.empty:
        # Apply field mapping
        archived_tasks_df = done_or_released_df.apply(map_plugin_task_fields, axis=1)
        archived_tasks_df = pd.DataFrame(archived_tasks_df.tolist())  # Ensure archived_tasks_df is a DataFrame
        # Append archived tasks to the PluginDone DataFrame
        plugin_done_df = pd.concat([plugin_done_df, archived_tasks_df], ignore_index=True)

        # Replace NaN and infinite values with empty strings
        plugin_done_df.replace([np.nan, np.inf, -np.inf], "", inplace=True)

        # Remove Done or Released tasks from Plugins(All) DataFrame
        plugins_df = plugins_df[~plugins_df["Status"].isin(["Done", "Released"])]

        # Write the updated PluginDone DataFrame to Google Sheets
        client = authorize_google_sheets()
        plugin_done_sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_PLUGINDONESHEET)
        plugin_done_sheet.clear()
        plugin_done_sheet.append_rows([plugin_done_df.columns.values.tolist()] + plugin_done_df.values.tolist(), value_input_option="USER_ENTERED")

        # Write the updated Plugins(All) DataFrame back to Google Sheets
        plugins_sheet = client.open_by_key(SPREADSHEET_KEY_ISSUES_ID).worksheet(SPREADSHEET_KEY_ISSUES_PLUGINSHEET)
        plugins_sheet.clear()
        plugins_sheet.append_rows([plugins_df.columns.values.tolist()] + plugins_df.values.tolist(), value_input_option="USER_ENTERED")
        
        # Print summary of the operation
        print(f"\tMoved {len(done_or_released_df)} tasks to PluginDone archive and removed them from Plugins(All).")
    else:
        print("\tNo Done or Released tasks found in Plugins(All).")

def update_resolved_dates():
    """
    Update the resolved dates in the Google Sheets document for tasks that are newly marked as 'Done'.
    Also, count and print how many new resolved dates were added.
    """
    print("Step 3: Adding resolved dates for newly resolved tasks")

    # Load the latest data from Jira CSV and Google Sheets
    jira_df = read_jira_csv()
    google_sheet_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Counter for tracking new resolved dates added
    resolved_dates_added_count = 0

    # Iterate over each row in the Jira DataFrame to find tasks with status 'Done'
    for index, jira_row in jira_df.iterrows():
        ticket_id = jira_row["Issue key"]
        jira_status = jira_row["Status"]
        resolved_date = jira_row.get("Resolved", None)  # Get the resolved date from Jira if it exists

        # Check if the status is 'Done' and if we have a resolved date
        if jira_status == "Done" and resolved_date:
            # Find the corresponding row in Google Sheets based on the unique ticket identifier
            google_sheet_row = google_sheet_df[google_sheet_df["Ticket"] == ticket_id]

            # If the task is found and has an empty Resolved Date in Google Sheets, update it
            if not google_sheet_row.empty and pd.isna(google_sheet_row.iloc[0]["ResolvedDate"]):
                # Update the Resolved Date in the Google Sheets DataFrame
                google_sheet_df.loc[google_sheet_df["Ticket"] == ticket_id, "ResolvedDate"] = resolved_date
                resolved_dates_added_count += 1  # Increment the counter

    # Write the updated DataFrame back to Google Sheets
    client = authorize_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)

    # Clear the sheet before updating to avoid duplication
    sheet.clear()

    # Convert DataFrame to list and append it back to Google Sheets
    sheet.append_rows([google_sheet_df.columns.values.tolist()] + google_sheet_df.values.tolist(), value_input_option="USER_ENTERED")
    
    # Print the results
    print("\tResolved dates updated successfully in Google Sheets.")
    print(f"\tTotal new resolved dates added: {resolved_dates_added_count}")

def should_update_status(old_status, new_status):
    """
    Determine whether to update the status based on specific rules.
    """
    non_updatable_statuses = {
        "Needs Product / Business Decision", 
        "Out of Scope", 
        "New UI", 
        "Duplicate"
    }
    
    # Rule: Update if the new status is "Done"
    if new_status == "Done":
        return True
    
    # Rule: Do not update if old status is in the non-updatable list
    if old_status in non_updatable_statuses:
        return False
    
    # In all other cases, allow update
    return True

def sync_plugin_tasks():
    """
    Sync plugin tasks between the DATABASE document and the Key Issues document based on the DevTeam column.
    """
    print("Step 5: Syncing plugin tasks between DATABASE and Key Issues documents")
    
    # Load tasks from the DATABASE document
    database_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)
    
    # Filter for tasks assigned to the "Plugin" team and exclude tasks marked as "Done" or "Won't Do"
    plugin_tasks_df = database_df[
        (database_df["DevTeam"] == "Plugin") & 
        (~database_df["Status"].isin(["Done", "Won't Do"]))
    ]
    print(f"\tFound {len(plugin_tasks_df)} plugin tasks in the DATABASE document (excluding 'Done' and 'Won't Do' tasks).")

    # Load the Plugins (All) sheet from the Key Issues document
    key_issues_df = read_google_sheet(SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_PLUGINSHEET)
    print("\tKey Issues document loaded successfully.")

    # Define column mappings from DATABASE to Key Issues document
    column_mapper = {
        "Ticket": "Ticket",
        "Client": "Client",
        "Type": "Type",
        "Priority": "Priority",
        "Status": "Status",
        "Summary": "Summary",
        "CreationDate": "CreationDate",
        "SLALimit": "SLALimit",
        "SLADeadline": "SLADeadline",
        "SLAOverdueDays": "SLAOverdueDays",
        "TicketId": "TicketId",
        "url_concat": "url_concat",
        "url_text": "url_text",
        "url_hyperlink": "url_hyperlink"
    }

    # Counters to track actions
    added_count = 0
    updated_count = 0

    # Iterate over each plugin task in the DATABASE document
    for index, plugin_task in plugin_tasks_df.iterrows():
        ticket_id = plugin_task["Ticket"]
        latest_status = plugin_task["Status"]

        # Check if the task already exists in the Key Issues document
        key_issues_task = key_issues_df[key_issues_df["Ticket"].str.contains(ticket_id, na=False)]

        if key_issues_task.empty:
            # Task does not exist in Key Issues document, add it
            new_row = {key_issues_col: plugin_task[db_col] if db_col in plugin_task else "" 
               for db_col, key_issues_col in column_mapper.items()}
            
            # Use TicketId for display text and url_concat for the URL in the hyperlink
            url = new_row["url_concat"]
            # Check if the URL already ends with the TicketId to avoid duplication
            if not url.endswith(new_row["TicketId"]):
                url += new_row["TicketId"]

            # Create the hyperlink using the appropriate URL
            new_row["Ticket"] = f'=HYPERLINK("{url}", "{new_row["TicketId"]}")'

            # Convert new_row to DataFrame and concatenate
            key_issues_df = pd.concat([key_issues_df, pd.DataFrame([new_row])], ignore_index=True)
            added_count += 1
        else:
            # Task exists, update the status (taking the first match if multiple found)
            first_match_index = key_issues_task.index[0]
            key_issues_df.at[first_match_index, "Status"] = latest_status
            updated_count += 1

            # Retrieve 'Platform' and 'V6 / V7' from Key Issues and update in the DATABASE document
            platform = key_issues_task.iloc[0].get("PluginPlatform", "")
            version = key_issues_task.iloc[0].get("PluginVersion", "")
            database_df.at[index, "PluginPlatform"] = platform
            database_df.at[index, "PluginVersion"] = version

    # Before updating, handle any problematic data in key_issues_df
    key_issues_df = key_issues_df.replace([float('inf'), -float('inf')], 0)
    key_issues_df = key_issues_df.fillna("")

    # SAFEGUARD: Keep a local backup of key_issues_df
    key_issues_backup = key_issues_df.copy()
    
    # Update the Key Issues document safely
    client = authorize_google_sheets()
    key_issues_sheet = client.open_by_key(SPREADSHEET_KEY_ISSUES_ID).worksheet(SPREADSHEET_KEY_ISSUES_PLUGINSHEET)
    key_issues_sheet.clear()

    # Append rows from the prepared DataFrame
    try:
        key_issues_sheet.append_rows([key_issues_df.columns.values.tolist()] + key_issues_df.values.tolist(), value_input_option="USER_ENTERED")
        print("\tKey Issues document updated successfully.")
    except Exception as e:
        # Restore from backup if there's an error
        print("Error while updating Key Issues document. Restoring from backup.")
        key_issues_sheet.append_rows([key_issues_backup.columns.values.tolist()] + key_issues_backup.values.tolist(), value_input_option="USER_ENTERED")
        raise e  # Re-raise the exception to signal that something went wrong
    
    # Update the DATABASE document with 'plugin-version' and 'plugin-platform'
    database_sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)
    database_sheet.clear()
    database_sheet.append_rows([database_df.columns.values.tolist()] + database_df.values.tolist(), value_input_option="USER_ENTERED")
    print("\tDATABASE document updated with plugin version and platform information.")

    # Print summary results
    print(f"\tPlugin task sync completed. Total new tasks added: {added_count} & Total tasks updated: {updated_count}")

def categorize_tasks_by_team():
    """
    Categorize tasks by team (Plugin, Backend, Frontend) based on rules and update the 'DevTeam' column in Google Sheets.
    """
    print("Step 4: Categorizing tasks by team (Plugin / Backend / Frontend)")

    # Load the latest Google Sheets data
    google_sheet_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Initialize counters for summarizing results
    plugin_count = 0
    backend_count = 0
    frontend_count = 0
    undecided_count = 0

    # Iterate over each row in the Google Sheets DataFrame to categorize tasks
    for index, row in google_sheet_df.iterrows():
        ticket_id = row["Ticket"]
        status = row["Status"]
        dev_team = None  # Initialize the DevTeam as None

        # Apply categorization rules
        if ticket_id.startswith("PGN-"):
            dev_team = "Plugin"
            plugin_count += 1
        elif ticket_id.startswith("UI-"):
            dev_team = "Frontend"
            frontend_count += 1
        elif ticket_id.startswith("YC-"):
            dev_team = "Backend"
            backend_count += 1
        elif "Backend" in status:
            dev_team = "Backend"
            backend_count += 1
        elif "Frontend" in status:
            dev_team = "Frontend"
            frontend_count += 1
        elif "Plugin" in status:
            dev_team = "Plugin"
            plugin_count += 1

        # If no categorization rule was matched, increment undecided count
        if dev_team is None:
            undecided_count += 1
        else:
            # Update the DevTeam column in the DataFrame
            google_sheet_df.at[index, "DevTeam"] = dev_team

    # Write the updated DataFrame back to Google Sheets
    client = authorize_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)

    # Clear the sheet before updating to avoid duplication
    sheet.clear()

    # Convert DataFrame to list and append it back to Google Sheets
    sheet.append_rows([google_sheet_df.columns.values.tolist()] + google_sheet_df.values.tolist(), value_input_option="USER_ENTERED")

    # Print the summary results
    print("\tTask categorization by team completed successfully.")
    print(f"\tTotal Plugin tasks: {plugin_count}")
    print(f"\tTotal Backend tasks: {backend_count}")
    print(f"\tTotal Frontend tasks: {frontend_count}")
    print(f"\tTotal undecided tasks: {undecided_count}")

def update_task_statuses():
    """
    Update the statuses of tasks in the Google Sheets document based on the latest Jira data.
    Also, track and print the number of statuses changed and skipped due to rules.
    """
    print("Step 2: Updating task statuses based on the latest Jira data")

    # Load the latest data from Jira CSV and Google Sheets
    jira_df = read_jira_csv()
    google_sheet_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Counters for tracking changes and skips
    changed_count = 0
    skipped_count = 0

    # Iterate over each row in the Jira DataFrame to apply the update rules
    for index, jira_row in jira_df.iterrows():
        ticket_id = jira_row["Issue key"]
        new_status = jira_row["Status"]

        # Find the corresponding row in Google Sheets based on the unique ticket identifier
        google_sheet_row = google_sheet_df[google_sheet_df["Ticket"] == ticket_id]
        
        if not google_sheet_row.empty:
            old_status = google_sheet_row.iloc[0]["Status"]

            # Apply the status update rule
            if should_update_status(old_status, new_status):
                # Update the status in Google Sheets DataFrame
                google_sheet_df.loc[google_sheet_df["Ticket"] == ticket_id, "Status"] = new_status
                changed_count += 1  # Increment changed counter
            else:
                skipped_count += 1  # Increment skipped counter

    # Write the updated DataFrame back to Google Sheets
    client = authorize_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)

    # Clear the sheet before updating to avoid duplication
    sheet.clear()

    # Convert DataFrame to list and append it back to Google Sheets
    sheet.append_rows([google_sheet_df.columns.values.tolist()] + google_sheet_df.values.tolist(), value_input_option="USER_ENTERED")
    
    # Print the results
    print(f"\tTask statuses updated successfully in Google Sheets.")
    print(f"\tTotal statuses changed: {changed_count}")
    print(f"\tTotal statuses skipped due to rules: {skipped_count}")

def calculate_sla_limit(priority):
    """
    Determine SLA Limit based on Priority.
    """
    if priority == "5-Blocker" or priority == "4-Critical":
        return 3
    elif priority == "3-Major":
        return 10
    else:
        return 60

def calculate_sla_deadline(row):
    """
    Calculate SLA Deadline as Creation Date + SLA Limit days.
    """
    creation_date = row["CreationDate"]
    sla_limit = row["SLALimit"]

    if creation_date and isinstance(sla_limit, int):
        try:
            creation_date_dt = datetime.strptime(creation_date, "%d-%b-%Y")
            sla_deadline_dt = creation_date_dt + timedelta(days=sla_limit)
            return sla_deadline_dt.strftime("%d-%b-%Y")
        except ValueError:
            return "N/A"  # If date parsing fails
    else:
        return "N/A"  # If either date or SLA Limit is missing

def calculate_sla_overdue_days(sla_deadline):
    """
    Calculate SLA Overdue Days as the difference between today and the SLA Deadline.
    """
    if sla_deadline and sla_deadline != "N/A":
        try:
            sla_deadline_dt = datetime.strptime(sla_deadline, "%d-%b-%Y")
            today = datetime.today()
            overdue_days = (today - sla_deadline_dt).days
            return max(overdue_days, 0)  # If overdue, return days, else 0
        except ValueError:
            return 0  # If date parsing fails, assume not overdue
    else:
        return 0  # If SLA Deadline is missing

def determine_client(row):
    """
    Determine the client based on label columns by checking for specific keywords.
    """
    clients = set()  # Use a set to avoid duplicate client names
    
    # Check each label column for client-specific keywords
    for col in row.index:
        if "Label" in col and isinstance(row[col], str):  # Only process label columns that contain strings
            for keyword, client_name in CLIENT_LABELS.items():
                if keyword.lower() in row[col].lower():  # Case-insensitive matching
                    clients.add(client_name)

    # Join client names with a comma if multiple clients are identified
    return ", ".join(clients) if clients else ""

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

def calculate_days_to_complete(row):
    """
    Calculate the number of days to complete a task, if Resolved Date is present.
    """
    creation_date = row["CreationDate"]
    resolved_date = row["ResolvedDate"]

    if creation_date and resolved_date:  # Ensure both dates are present
        try:
            # Parse dates in "DD-MMM-YYYY" format
            creation_date_dt = datetime.strptime(creation_date, "%d-%b-%Y")
            resolved_date_dt = datetime.strptime(resolved_date, "%d-%b-%Y")
            # Calculate difference in days
            days_to_complete = (resolved_date_dt - creation_date_dt).days
            return days_to_complete
        except ValueError:
            return "N/A"  # If date parsing fails, return "N/A"
    else:
        return "N/A"  # Return "N/A" if either date is missing

def prepare_new_tasks(jira_df, database_df):
    """
    Find and prepare new tasks from Jira to be added to the database.
    """
    # Identify new tasks
    new_tasks_df = find_new_tasks(jira_df, database_df, jira_key_column="Issue key", database_key_column="Ticket")

    # Map and transform columns for new tasks
    new_tasks_df = new_tasks_df.rename(columns={
        "Issue key": "Ticket",
        "Status": "Status",
        "Issue Type": "Type",
        "Priority": "Priority",
        "Summary": "Summary",
        "Created": "CreationDate",
        "Resolved": "ResolvedDate"
    })

    if(len(new_tasks_df)==0):
        return new_tasks_df

    # Apply transformations
    new_tasks_df["Priority"] = new_tasks_df["Priority"].apply(transform_priority)
    new_tasks_df["CreationDate"] = new_tasks_df["CreationDate"].apply(format_date)
    new_tasks_df["ResolvedDate"] = new_tasks_df["ResolvedDate"].apply(format_date)

    # Additional logic
    base_url = "https://niceteam.atlassian.net/browse/"
    new_tasks_df["TicketId"] = new_tasks_df["Ticket"]
    new_tasks_df["url_concat"] = base_url + new_tasks_df["Ticket"]
    new_tasks_df["url_text"] = '=HYPERLINK("' + new_tasks_df["url_concat"] + '", "' + new_tasks_df["TicketId"] + '")'
    new_tasks_df["Ticket"] = new_tasks_df["url_text"]
    
    # Calculate SLA Limit based on Priority
    new_tasks_df["SLALimit"] = new_tasks_df["Priority"].apply(calculate_sla_limit)

    # Calculate SLA Deadline
    new_tasks_df["SLADeadline"] = new_tasks_df.apply(calculate_sla_deadline, axis=1)

    # Calculate SLA Overdue Days
    new_tasks_df["SLAOverdueDays"] = new_tasks_df["SLADeadline"].apply(calculate_sla_overdue_days)

    # Calculate Days to Complete if Resolved Date is present
    new_tasks_df["DaysToComplete"] = new_tasks_df.apply(calculate_days_to_complete, axis=1)

    # Determine client based on labels
    new_tasks_df["Client"] = new_tasks_df.apply(determine_client, axis=1)

    # Add missing columns with default values to match Google Sheets structure
    expected_columns = [
        "Ticket", "Client", "Type", "Priority", "Status", "Summary",
        "CreationDate", "SLALimit", "SLADeadline", "SLAOverdueDays",
        "ResolvedDate", "DaysToComplete", "DevTeam", "Comments", 
        "DuplicateID", "SupportSheet?", "TicketId", "url_concat", "url_text"
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
    print("Step 1: Appending new tasks to the database")

    # Load data from Jira CSV and Google Sheets database
    jira_data = read_jira_csv()
    database_data = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Prepare new tasks
    new_tasks_df = prepare_new_tasks(jira_data, database_data)
    
    if new_tasks_df.empty:
        print("\tNo new tasks to append.")
        return

    # Append new tasks to Google Sheets
    client = authorize_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_DATABASE_ID).worksheet(SPREADSHEET_DATABASE_MAINSHEET)

    # Convert DataFrame to list of lists and append to Google Sheets
    first_new_task_list = new_tasks_df.values.tolist()
    sheet.append_rows(first_new_task_list, value_input_option="USER_ENTERED")
    print("\tNew tasks successfully appended to the Google Sheets database.")

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
        print(f"\tError: '{jira_key_column}' or '{database_key_column}' column not found in the data sources.")
        return pd.DataFrame()

    # Find tasks in Jira that are not in the database
    new_tasks_df = jira_df[~jira_df[jira_key_column].isin(database_df[database_key_column])]
    print(f"\tFound {len(new_tasks_df)} new tasks in Jira not present in the database.")
    return new_tasks_df

def update_backend_frontend_status():
    """
    Check each task in Key Issues -> Backend/Frontend, find it in Database -> all-tasks,
    update the status if changed, and remove it if the status is one of the specified values.
    """
    print("Step 7: Updating and cleaning tasks in Backend/Frontend")

    # Load data from Backend/Frontend and all-tasks sheets
    backend_frontend_df = read_google_sheet(SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_MAINSHEET)
    all_tasks_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)

    # Define statuses that require removal
    removal_statuses = {
        "Beta", "PENDING TO DEPLOY", "Ready to Launch", "Waiting to Deploy",
        "UAT / Waiting Customer", 
        "New UI Beta", 
        "Won't Do", "Done"
    }

    # Counter for tracking changes and removals
    updated_count = 0
    removed_count = 0

    # Iterate over each task in the Backend/Frontend sheet
    for index, backend_task in backend_frontend_df.iterrows():
        ticket_id = backend_task["Ticket"]  # Unique identifier
        backend_status = backend_task["Status"]

        # Find the corresponding row in all-tasks based on the unique ticket identifier
        all_tasks_row = all_tasks_df[all_tasks_df["Ticket"] == ticket_id]
        
        if not all_tasks_row.empty:
            latest_status = all_tasks_row.iloc[0]["Status"]

            # Check if the status has changed
            if backend_status != latest_status:
                # Update the status in Backend/Frontend DataFrame
                backend_frontend_df.loc[index, "Status"] = latest_status
                updated_count += 1

                # If the updated status is in the removal list, mark the task for removal
                if latest_status in removal_statuses:
                    backend_frontend_df.drop(index, inplace=True)
                    removed_count += 1
    
     # Regenerate hyperlinks in the Ticket column
    base_url = "https://niceteam.atlassian.net/browse/"
    for index, row in backend_frontend_df.iterrows():
        ticket_id = row["TicketId"]
        if ticket_id:
            url = base_url + ticket_id
            backend_frontend_df.at[index, "Ticket"] = f'=HYPERLINK("{url}", "{ticket_id}")'

    # Write the updated Backend/Frontend DataFrame back to Google Sheets
    client = authorize_google_sheets()
    backend_frontend_sheet = client.open_by_key(SPREADSHEET_KEY_ISSUES_ID).worksheet(SPREADSHEET_KEY_ISSUES_MAINSHEET)
    backend_frontend_sheet.clear()
    backend_frontend_sheet.append_rows([backend_frontend_df.columns.values.tolist()] + backend_frontend_df.values.tolist(), value_input_option="USER_ENTERED")
    
    # Print summary of the operation
    print(f"\tUpdated statuses for {updated_count} tasks in Backend/Frontend.")
    print(f"\tRemoved {removed_count} tasks from Backend/Frontend due to specified statuses.")

def reorder_backlog_backend_tasks_insert_to_key_issues():
    """
    Filter and reorder backend/frontend tasks from the Database (all-tasks) sheet,
    and upsert the top 25 tasks to Key Issues -> Backend/Frontend based on priority and SLA Overdue Days.
    """
    print("Step 8: Reordering and inserting top backend/frontend tasks into Key Issues")

    # Load all-tasks data from Database
    all_tasks_df = read_google_sheet(SPREADSHEET_DATABASE_ID, SPREADSHEET_DATABASE_MAINSHEET)
    # Load Backend/Frontend sheet data from Key Issues for upsert
    backend_frontend_df = read_google_sheet(SPREADSHEET_KEY_ISSUES_ID, SPREADSHEET_KEY_ISSUES_MAINSHEET)

    # Filter tasks: status is To Do or In Progress, and DevTeam is NOT Plugin
    filtered_tasks = all_tasks_df[
        (all_tasks_df["Status"].isin(["Backlog"
                                      , "Todo - Backend"
                                      , "In Dev - Backend"
                                      , "Waiting PR - Backend"
                                      , "QA - Backend"
                                      , "Todo - Frontend"
                                      , "In Dev - Frontend"
                                      , "QA - Frontend"
                                      , "To Do"
                                      , "In Progress"
                                      , "Requires Engineering assessment"])) &
        (all_tasks_df["DevTeam"] != "Plugin") & 
        (~all_tasks_df["Ticket"].str.startswith("PRODREQ-"))
    ]

    # Sort by Priority and SLA Overdue Days
    priority_order = {"5-Blocker": 5, "4-Critical": 4, "3-Major": 3, "2-Minor": 2, "1-Trivial": 1}
    filtered_tasks["PriorityOrder"] = filtered_tasks["Priority"].map(priority_order)
    filtered_tasks = filtered_tasks.sort_values(
        by=["PriorityOrder", "SLAOverdueDays"],
        ascending=[False, False]
    ).drop(columns=["PriorityOrder"])

    # Limit to top 25 tasks
    top_tasks_df = filtered_tasks.head(25)

    # Upsert these tasks to the Backend/Frontend sheet in Key Issues
    upserted_count = 0
    for index, task in top_tasks_df.iterrows():
        ticket_id = task["Ticket"]
        
        # Check if task already exists in the Backend/Frontend sheet
        existing_task = backend_frontend_df[backend_frontend_df["Ticket"] == ticket_id]
        
        if(existing_task.empty):
            # Insert new task
            backend_frontend_df = pd.concat([backend_frontend_df, pd.DataFrame([task])], ignore_index=True)
            upserted_count += 1

    # Clean up the DataFrame before writing to Google Sheets
    backend_frontend_df.replace([float('inf'), -float('inf')], 0, inplace=True)
    backend_frontend_df.fillna("", inplace=True)

    # Create hyperlinks for the Ticket column
    base_url = "https://niceteam.atlassian.net/browse/"
    backend_frontend_df["Ticket"] = backend_frontend_df.apply(
        lambda row: f'=HYPERLINK("{base_url}{row["Ticket"]}", "{row["Ticket"]}")' if pd.notna(row["Ticket"]) else "",
        axis=1
    )

    # Write the updated Backend/Frontend DataFrame back to Google Sheets
    client = authorize_google_sheets()
    backend_frontend_sheet = client.open_by_key(SPREADSHEET_KEY_ISSUES_ID).worksheet(SPREADSHEET_KEY_ISSUES_MAINSHEET)
    backend_frontend_sheet.clear()
    backend_frontend_sheet.append_rows([backend_frontend_df.columns.values.tolist()] + backend_frontend_df.values.tolist(), value_input_option="USER_ENTERED")
    
    # Print summary of the operation
    print(f"\tUpserted top 25 tasks to Backend/Frontend. Total new tasks inserted: {upserted_count}")

# Example usage
if __name__ == "__main__":
    append_new_tasks_to_database()
