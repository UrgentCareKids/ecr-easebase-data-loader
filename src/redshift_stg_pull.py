import os, sys
import boto3
import time
from datetime import datetime
import re
import csv

from db.redshift_conn import redshift_conn
from db.easebase_conn import easebase_conn

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Connect to your databases
rd_conn = redshift_conn()
rd_cursor = rd_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

# Define the constants
run_id = int(time.time())
phase = 's_load'
schema = 'stg.'
table_name_prefix = 's_redshift_'
log_table = 'logging.eb_log'
channel = 'redshift'
backup_schema='stg_backup.'
tables = ['podium_review', 'pond_podium_feedback', 'gsh_visit_order_pivot', 'podium_feedback']
#use the mount in the task for the connection
dir_path = '/easebase/'  # Mac directory path

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)


for table in tables:
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

        # Fetch all rows from the redshift table
        rd_cursor.execute(f'SELECT * FROM "{table}"')
        rows = rd_cursor.fetchall()

        # Fetch column names and types from the redshift table
        rd_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position")
        columns_data = rd_cursor.fetchall()
        columns_names = ', '.join([column[0] for column in columns_data])
        
        columns_with_types = ', '.join([f"{(column[0])} {(column[1])}" for column in columns_data])


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
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types})")
      

        # Create new table in the easebase database
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types})")

 # Write the data to a tab-delimited file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        with open(file_path, 'w', newline='') as tsvfile:
            writer = csv.writer(tsvfile, delimiter='|')
            for row in rows:
                writer.writerow(row)

        # Open the tab-delimited file and load it into the PostgreSQL database
        with open(file_path, 'r') as f:
           # next(f)  # Skip the header row.
            eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' CSV", f)


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
                print(e)
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
rd_conn.close()
eb_conn.close()

