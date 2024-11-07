# src/main.py
import data_processing

def main():
    # Step 1: Append new tasks to the database
    # data_processing.append_new_tasks_to_database()
    
    # Step 2: Update task statuses and based on the latest Jira data
    # data_processing.update_task_statuses()

    # Step 3: Add resolve dates for newly resolved tasks
    # data_processing.update_resolved_dates()

    # Step 4: Categorize plugin / backend / frontend
    # data_processing.categorize_tasks_by_team()

    # Step 5: Sync Plugin tasks (Database -> Key Issues)
    # data_processing.sync_plugin_tasks()
    
    # Step 6: Remove Done tasks from Key Issues - move them to Database
    # data_processing.move_done_tasks_to_archive()

    # Step 7: Update and clean tasks in Key Issues: Backend/Frontend
    # data_processing.update_backend_frontend_status()

    # Step 8: Reorder backend/frontend tasks in Database, and try to insert top issues to Key Issues
    data_processing.reorder_backlog_backend_tasks_insert_to_key_issues()
    
    # 9	update summary (for backend+frontend)	
    # 10	maybe update summary for plugin	
    # 11	sort by range - Priority + SLA days overdue	
    # 12	Filter (NOT plugin & todo & in progress)	
    # 13	last controls & send to CE CSMs	
    # 14	send 'excom' email to Till	
    # 15	send task list to Ferran & Devs	

if __name__ == "__main__":
    main()
