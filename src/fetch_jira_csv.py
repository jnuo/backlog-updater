import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from config.settings import JIRA_CSV_PATH

def read_jira_csv(file_path=JIRA_CSV_PATH):
    """
    Reads the Jira export CSV file and returns it as a DataFrame.
    
    Parameters:
        file_path (str): The path to the Jira CSV file.
    
    Returns:
        pd.DataFrame: The data from the CSV file as a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        print("Jira CSV file loaded successfully.")
        return df
    except FileNotFoundError:
        print("Error: CSV file not found at specified path.")
        return pd.DataFrame()

def print_csv_summary(df):
    """
    Prints a summary of the Jira CSV data.
    
    Parameters:
        df (pd.DataFrame): The DataFrame containing the CSV data.
    """
    print("Summary of Jira CSV Data:")
    print("Number of tasks:", len(df))
    print("Columns:", df.columns.tolist())
    print("Sample data:")
    print(df.head())

# Example usage
if __name__ == "__main__":
    jira_df = read_jira_csv()
    print_csv_summary(jira_df)
