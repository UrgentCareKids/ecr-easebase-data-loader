# please add new procs in line 20 array procs and then the matching table into the proc_to_targettable data dictionary in line 22

import psycopg2
from psycopg2 import sql
import sys
import time
import re

from db.easebase_conn import easebase_conn

# Define the constants
run_id = int(time.time())
phase = 'rpt_refresh'
log_table = 'logging.eb_log'
schema = 'rpt.'
channel = 'easebase'
procs = ['rpt_refresh_mpi()','rpt_refresh_patient()']
# Create a dictionary to store the associations which proc is connected to which target table
proc_to_targettable = {
    'rpt_refresh_mpi()':'rpt_mpi',
    'rpt_refresh_patient()':'rpt_patient'
}

# Connect to your databases
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)


for proc in procs:
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
            VALUES ({run_id}, '{channel}', '{phase}', '{proc}', '{schema}{proc_to_targettable[proc]}', 'running', CURRENT_TIMESTAMP);
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()
        
        #Execute the proc
        eb_cursor.execute(f"CALL {schema}{proc};")

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
eb_conn.close()
