# src/main.py
import data_processing

def main():
    # Step 1: Append new tasks to the database
    data_processing.append_new_tasks_to_database()
    
    # Step 2: Update task statuses and based on the latest Jira data
    data_processing.update_task_statuses()

    # Step 3: Add resolve dates for newly resolved tasks
    data_processing.update_resolved_dates()

if __name__ == "__main__":
    main()
