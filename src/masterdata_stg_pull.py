import psycopg2
from psycopg2 import sql
import sys
import boto3
import os
import time

#set the import path for db
sys.path.append('./db')

from db.masterdata_conn import masterdata_conn
from db.easebase_conn import easebase_conn
from datetime import datetime
import re
import csv

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Connect to your databases
md_conn = masterdata_conn()
md_cursor = md_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

# Define the constants
run_id = int(time.time())
phase = 's_load'
schema = 'stg.'
table_name_prefix = 's_ptsrv_'
log_table = 'logging.eb_log'
channel = 'masterdata'
backup_schema='stg_backup.'
automation_logging = 'logging.daily_proc_automation'
#tables = ['mat_tmp_fast_demographics', 'mat_fast_search_json', 'mahler_event_cx', 'facility_org_cx', 'mahler_id_cx', 'mstr_hl7_interface_id_cx', 'mstr_guarantor_hl7_interface_id_cx', 'mstr_intake_transaction_router']
#use the mount in the task for the connection
dir_path = '/easebase/'  # Mac directory path

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)

# Query the logging.daily_proc_automation table to retrieve the table_or_proc_nm column
eb_cursor.execute(f"SELECT table_or_proc_nm FROM {automation_logging} WHERE channel = '{channel}' and is_active = true;")

# Fetch all the rows and store the table_or_proc_nm values in a Python list
table_or_proc_nm_list = [row[0] for row in eb_cursor.fetchall()]

for table in table_or_proc_nm_list:
    target_table = f'{table_name_prefix}{table}'
    
    try:
        # Update the log record set prior stuck in running to "failed"
        priorsql=f"""
            UPDATE {log_table}
            SET run_status = 'failed', end_ts = CURRENT_TIMESTAMP
            WHERE  run_source = '{table}' and channel = '{channel}' and run_status = 'running';
        """
        eb_cursor.execute(priorsql)
        eb_conn.commit()

        # Log the start of processing
        rsql=f"""
            INSERT INTO {log_table}
            (run_id, channel, phase, run_source, run_target, run_status, start_ts)
            VALUES ({run_id}, '{channel}', '{phase}', '{table}', '{schema}{target_table}', 'running', CURRENT_TIMESTAMP);
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

        # Fetch column names and types from the patient service  database
        md_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        columns_data = md_cursor.fetchall()
        columns_names = ', '.join([column[0] for column in columns_data])
        #columns_with_types = ', '.join([f"{column[0]} integer[]" if column[1] == 'ARRAY' else f"{column[0]} {column[1]}" for column in columns_data])
        columns_with_types = []
        for column in columns_data:
            if column[1] == 'ARRAY':
                columns_with_types.append(f"{column[0]} integer[]")
            else:
                columns_with_types.append(f"{column[0]} {column[1]}")
        columns_with_types_str = ', '.join(columns_with_types)


        # Check if the target table exists in the easebase database
        eb_cursor.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")
        table_exists = eb_cursor.fetchone()[0]


        # Create backup table in the easebase database 
        if table_exists:
            # If the table exists, create a backup table
            backup_table = f'{backup_schema}{target_table}'
            eb_cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
            eb_cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {schema}{target_table}")
            
    
        # Create new table in the easebase databas
        
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types_str})")
      

        # Create new table in the easebase database
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types_str})")

        # Write the data to a tab-delimited file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        with open(file_path, 'w', newline='') as tsvfile:
    # Write the data
            md_cursor.copy_to(tsvfile, table, sep='|', null='')


# Open the tab-delimited file and load it into the PostgreSQL database
        with open(file_path, 'r') as f:
            print(f"starting copy {schema}{target_table}")
            eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' NULL as ''", f)
        eb_conn.commit()

        print(f"{schema}{target_table} complete...")
        
        os.remove(file_path)

        # Update the log record for this run_id and table to success
        rsql=f"""
            UPDATE {log_table}
            SET run_status = 'success', end_ts = CURRENT_TIMESTAMP
            WHERE run_id = {run_id} AND run_source = '{table}' and channel = '{channel}';
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

    except Exception as e:
        #if table != 'pusers':
                err = remove_non_letters(str(e))
                rsql=f"""
                    UPDATE {log_table}
                    SET run_status = 'failed', error_desc = '{err[:255]}', end_ts = CURRENT_TIMESTAMP
                    WHERE run_id = {run_id} AND run_source = '{table}' and channel = '{channel}';
                """
                eb_cursor.execute(rsql)
                eb_conn.commit()
            
        #continue
                   # Update all previous log records for this run_id and table to not be the latest
        

# Remember to close the connection when you're done
md_conn.close()
eb_conn.close()
