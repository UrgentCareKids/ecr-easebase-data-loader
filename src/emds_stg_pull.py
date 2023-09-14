import pyodbc
import psycopg2
from psycopg2 import sql
import sys
import time
import re
import os
import csv

#set the import path for db
sys.path.append('./db')

from db.emds_conn import emds_conn
from db.easebase_conn import easebase_conn
from datetime import datetime

#print(pyodbc.drivers()) #shows a list of available sql server drivers, useful for troubleshooting
# Connect to your databases
emds_conn = emds_conn()
emds_cursor = emds_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()


# Define the constants
run_id = int(time.time())
phase = 's_load'
schema = 'stg.'
source_schema = 'uc4k.dbo.'
database = 'uc4k'
table_name_prefix = 's_'
log_table = 'logging.eb_log'
channel = 'emds'
backup_schema='stg_backup.'
tables = ['emds_invoice_summary', 'emds_invoice_line'] #tmp_anna_test_limited_invoice_summary for testing
#use the mount in the task for the connection
dir_path = '/easebase/'

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)

emds_cursor.execute(f'USE {database}')

for table in tables:
    target_table = f'{table_name_prefix}{table}'
    
    try:
        # Update the log record set prior stuck in running to "failed"
        priorsql=f"""
            UPDATE {log_table}
            SET run_status = 'failed', end_ts = CURRENT_TIMESTAMP
            WHERE  run_source = '{source_schema}{table}' and channel = '{channel}' and run_status = 'running';
        """
        eb_cursor.execute(priorsql)
        eb_conn.commit()

        # Log the start of processing
        rsql=f"""
            INSERT INTO {log_table}
            (run_id, channel, phase, run_source, run_target, run_status, start_ts)
            VALUES ({run_id}, '{channel}', '{phase}', '{source_schema}{table}', '{schema}{target_table}', 'running', CURRENT_TIMESTAMP);
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

       # Fetch all rows from the emds table
        print(f'SELECT * FROM {source_schema}{table}')
        emds_cursor.execute(f'SELECT * FROM {source_schema}{table}')
        rows = emds_cursor.fetchall()
 
        #order column list and column data type by the positions they appear on emds table
        emds_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position")
        columns_data = emds_cursor.fetchall()

        columns_names = ', '.join([column[0] for column in columns_data])

        columns_with_types = []
        for column in columns_data:
            if column[1].lower() == 'nvarchar':
                columns_with_types.append(f"{column[0]} character varying")
            elif column[1].lower() == 'char':
                columns_with_types.append(f"{column[0]} char(10)")
            elif column[1].lower() == 'money':
                 columns_with_types.append(f"{column[0]} numeric")
            elif column[1].lower() == 'tinyint':
                 columns_with_types.append(f"{column[0]} smallint")
            else:
                columns_with_types.append(f"{column[0]} {column[1]}")
               
        # Join the list elements into a comma-separated string
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
            
    
        # Create new table in the easebase database
        
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types_str})")
      

        # Write the data to a tab-delimited file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        with open(file_path, 'w', newline='') as tsvfile:
            tsv_writer = csv.writer(tsvfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    
            # Write the header row
            tsv_writer.writerow([column[0] for column in emds_cursor.description])
    
            # Write the data rows
            for row in rows:
                # Handle NULL values and replace them with an empty string
                row = [str(value) if value is not None else '' for value in row]
                tsv_writer.writerow(row)


# Open the tab-delimited file and load it into the PostgreSQL database
        with open(file_path, 'r') as f:
            print(f"starting copy {schema}{target_table}")
            eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' CSV HEADER", f)
        eb_conn.commit()

        print(f"{schema}{target_table} complete...")
        
        os.remove(file_path)

        # Update the log record for this run_id and table to success
        rsql=f"""
            UPDATE {log_table}
            SET run_status = 'success', end_ts = CURRENT_TIMESTAMP
            WHERE run_id = {run_id} AND run_source = '{source_schema}{table}' and channel = '{channel}';
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

    except Exception as e:
        #if table != 'pusers':
                print(e)
                err = remove_non_letters(str(e))
                rsql=f"""
                    UPDATE {log_table}
                    SET run_status = 'failed', error_desc = '{err[:255]}', end_ts = CURRENT_TIMESTAMP
                    WHERE run_id = {run_id} AND run_source = '{source_schema}{table}' and channel = '{channel}';
                """
                eb_cursor.execute(rsql)
                eb_conn.commit()
            
        #continue
                   # Update all previous log records for this run_id and table to not be the latest
        

# Remember to close the connection when you're done
emds_conn.close()
eb_conn.close()

