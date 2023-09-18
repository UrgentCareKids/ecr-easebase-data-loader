import os, sys
import boto3
import time
from datetime import datetime
import re
import csv
import gzip
from io import BytesIO

from db.redshift_conn import redshift_conn, get_aws_credentials
from db.easebase_conn import easebase_conn

# Initialize AWS S3 client
s3 = boto3.client('s3')
# Get AWS credentials in order to unload to S3 from Secrets Manager
aws_credentials = get_aws_credentials()

# Connect to your databases
rd_conn = redshift_conn()
rd_cursor = rd_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

# Define the constants
s3_bucket = 'uc4k-db'
s3_prefix= 'easebase/s_loads/s_redshift/'
run_id = int(time.time())
phase = 's_load'
schema = 'stg.'
table_name_prefix = 's_redshift_'
log_table = 'logging.eb_log'
channel = 'redshift'
backup_schema='stg_backup.'
tables = ['gsh_invoice_summary','pond_locations','pond_organizations','podium_review', 'pond_podium_feedback', 'gsh_visit_order_pivot', 'podium_feedback', 'gsh_invoice_summary', 'gsh_invoice_line']
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


         # Use the UNLOAD command to export data to S3
        unload_command = f"""
            UNLOAD ('SELECT * FROM {table}')
            TO 's3://{s3_bucket}/{s3_prefix}{table}_{run_id}'    
            CREDENTIALS 'aws_access_key_id={aws_credentials['access_key_id']};aws_secret_access_key={aws_credentials['secret_access_key']}'   
            DELIMITER '|'
            ALLOWOVERWRITE
            ESCAPE
            PARALLEL OFF
            HEADER;
        """
        rd_cursor.execute(unload_command)


# Read CSV headers out of S3 file and create target and backup table on easebase
        # Use the list_objects_v2 method to list files in the specified subfolder
        list = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        # Check each object to see if it contains the current run_id in its key (filename)
        current_file = [obj['Key'][len(s3_prefix):] for obj in list.get('Contents', []) if str(run_id) in obj['Key']]
        #get response for current file
        response = s3.get_object(Bucket=s3_bucket, Key=s3_prefix + current_file[0])


        # Read the first line of the file (which should be the header)
        header = response['Body'].readline().decode('utf-8')
        # Split the header into individual columns based on the '|' delimiter
        columns = header.split('|')
        # Remove new line breaks from each column name and append " VARCHAR"
        columns_with_varchar = [column.strip('\n') + ' VARCHAR' for column in columns]
        # Join the columns into a single string
        columns_names_with_types = ', '.join(columns_with_varchar)
        # Print the list of columns
        #print(f'Columns: {columns_names_with_types}')


        # create list of columns without types for each column name
        stripped_columns = [column.strip('\n') for column in columns]
        columns_names = ', '.join(stripped_columns)

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
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_names_with_types})")
        

# Read Data into Easebase Table

        # Download CSV file from S3
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        s3.download_file(s3_bucket, s3_prefix + current_file[0], file_path)

        # Import CSV data into PostgreSQL
        with open(file_path, 'r') as f:
            print(f"starting copy {schema}{target_table}")
            eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' NULL as '' HEADER", f)
        eb_conn.commit()

        print(f"{schema}{target_table} complete...")
        
        #remove local copy
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

