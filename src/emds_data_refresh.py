# this script is meant to refresh the tables emds_invoice_summary and emds_invoice_line on the emds sql server side
# it will be executed in the step function easebase_emds_load while sql server is still up
import pyodbc
import sys
import time
import re
#set the import path for db
sys.path.append('./db')

from db.emds_conn import emds_conn
from db.easebase_conn import easebase_conn

#print(pyodbc.drivers()) #shows a list of available sql server drivers, useful for troubleshooting
# Connect to your databases
emds_conn = emds_conn()
emds_cursor = emds_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

# Define the constants
run_id = int(time.time())
phase = 's_refresh'
log_table = 'logging.eb_log'
schema = 'uc4k'
automation_logging = 'logging.daily_proc_automation'
channel = 'emds'

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)

#use uc4k database on sql server
emds_cursor.execute(f"USE {schema}")

# Query the logging.daily_proc_automation table to retrieve the table_or_proc_nm column
eb_cursor.execute(f"SELECT table_or_proc_nm FROM {automation_logging} WHERE schema_nm = '{schema}' and phase = '{phase}' and is_active = true;")
# Fetch all the rows and store the table_or_proc_nm values in a Python list
table_or_proc_nm_list = [row[0] for row in eb_cursor.fetchall()]

for proc in table_or_proc_nm_list:
    eb_cursor.execute(f"SELECT target_table FROM {automation_logging} WHERE schema_nm = '{schema}' and table_or_proc_nm = '{proc}' and is_active = true;")
    targettable = [row[0] for row in eb_cursor.fetchall()]
    try:
        # Update the log record set prior stuck in running to "failed"
        priorsql=f"""
            UPDATE {log_table}
            SET run_status = 'failed', end_ts = CURRENT_TIMESTAMP
            WHERE  run_source = '{proc}' and channel = '{channel}' and run_status = 'running';
        """
        eb_cursor.execute(priorsql)
        eb_conn.commit()

        # Log the start of processing
        rsql=f"""
            INSERT INTO {log_table}
            (run_id, channel, phase, run_source, run_target, run_status, start_ts)
            VALUES ({run_id}, '{channel}', '{phase}', '{proc}', '{targettable[0]}', 'running', CURRENT_TIMESTAMP);
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()
        
        #Execute the proc
        emds_cursor.execute(f"EXECUTE {proc}")

         # Update the log record for this run_id and proc to success
        rsql=f"""
            UPDATE {log_table}
            SET run_status = 'success', end_ts = CURRENT_TIMESTAMP
            WHERE run_id = {run_id} AND run_source = '{proc}' and channel = '{channel}';
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

    except Exception as e:
        #if table != 'pusers':
                err = remove_non_letters(str(e))
                rsql=f"""
                    UPDATE {log_table}
                    SET run_status = 'failed', error_desc = '{err[:255]}', end_ts = CURRENT_TIMESTAMP
                    WHERE run_id = {run_id} AND run_source = '{proc}' and channel = '{channel}';
                """
                eb_cursor.execute(rsql)
                eb_conn.commit()


# Remember to close the connection when you're done
emds_conn.close()
eb_conn.close()