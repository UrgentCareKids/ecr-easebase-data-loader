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
table_name_prefix = 's_patient_service_'
log_table = 'logging.eb_log'
channel = 'masterdata'
backup_schema='stg_backup.'
tables = ['mat_tmp_fast_demographics', 'mahler_event_cx'] #, 'mahler_id_cx', 'mstr_hl7_interface_id_cx', 'mstr_guarantor_hl7_interface_id_cx', 'mstr_intake_transaction_router']
#use the mount in the task for the connection
dir_path = '/easebase/'


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

        #print("are we even getting here?")
        # Fetch all rows from the patient service database
        md_cursor.execute(f"SELECT * FROM {table}")
        rows = md_cursor.fetchall()
        #rows = md_cursor.fetchone()
        print(f"Total rows: {len(rows)}")

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
        print(columns_with_types_str) 

        # Check if the target table exists in the easebase database
        eb_cursor.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")
        table_exists = eb_cursor.fetchone()[0]
       # print(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")

        # Create backup table in the easebase database 
        if table_exists:
            # If the table exists, create a backup table
            backup_table = f'{backup_schema}{target_table}_bck'
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
            writer = csv.writer(tsvfile, delimiter='|')
            for row in rows:
                writer.writerow(row)

        # Open the tab-delimited file and load it into the PostgreSQL database
        with open(file_path, 'r') as f:
            try:
                # Attempt to load the data, catching the out-of-range error
                eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' CSV HEADER", f)
            except psycopg2.DataError as e:
                # Handle the out-of-range error
                print("Caught error:", e)
                print("Replacing problematic pg_dob values with NULL...")
                f.seek(0)  # Reset the file pointer
                
                # Create a temporary file with modified content
                temp_file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}_temp.tsv")
                with open(temp_file_path, 'w', newline='') as temp_file:
                    for line in f:
                        values = line.strip().split('|')
                        pg_dob_index = columns_names.split(', ').index('pg_dob')
                        pg_dob_value = values[pg_dob_index]
                        try:
                            year = int(pg_dob_value.split('-')[0]) if pg_dob_value else 0
                            print(year)
                            if year > 10000:
                                values[pg_dob_index] = '\\N'
                        except ValueError:
                            values[pg_dob_index] = '\\N'
                        temp_file.write('|'.join(values) + '\n')
                
                # Load the modified file into the PostgreSQL database
                eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' CSV HEADER", temp_file_path)
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
                print(e)
        #continue
                   # Update all previous log records for this run_id and table to not be the latest
        

# Remember to close the connection when you're done
md_conn.close()
eb_conn.close()
