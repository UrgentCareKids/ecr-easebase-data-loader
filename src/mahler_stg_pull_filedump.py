import psycopg2
import subprocess
from psycopg2 import sql
import sys
import boto3
import os
import time
import tempfile

#set the import path for db
sys.path.append('./db')

from db.mahler_conn import mahler_conn, get_mahler_param
from db.easebase_conn import easebase_conn
from datetime import datetime
import re
import csv

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Connect to your databases
m_conn = mahler_conn()
m_cursor = m_conn.cursor()
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

# Get all table names in your database
m_cursor.execute("SHOW TABLES")
tables = m_cursor.fetchall()

# Define the constants
run_id = int(time.time())
automation_logging = 'logging.daily_proc_automation'
phase = 's_load'
schema = 'stg'
table_name_prefix = 's_mahler_'
log_table = 'logging.eb_log'
channel = 'mahler'
backup_schema='stg_backup'
#use the mount in the task for the connection
dir_path = '/easebase/'

def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)

def sanitize_pg_name(pg_name):
    # Remove or replace special characters in the column name
    sanitized_pg_name = re.sub(r'[^a-zA-Z0-9_]+', '_', pg_name)
    if sanitized_pg_name[0].isdigit():
        sanitized_pg_name = "_" + sanitized_pg_name
    return sanitized_pg_name


for table in tables:
    table = table[0]
    pg_table = sanitize_pg_name(table)
    target_table = f'{table_name_prefix}{pg_table}'

    try:
        # Update the log record set prior stuck in running to "failed"
        print(f"{schema}.{target_table} initialization...")
        priorsql=f"""
            UPDATE {log_table}
            SET run_status = 'failed', end_ts = CURRENT_TIMESTAMP
            WHERE  run_source = '{table}' and channel = '{channel}' and run_status = 'running';
        """
        eb_cursor.execute(priorsql)
        eb_conn.commit()


        # Check if the table name already exists in the log table
        check_sql = f"SELECT COUNT(*) FROM {automation_logging} WHERE table_or_proc_nm = '{table}';"
        eb_cursor.execute(check_sql)
        count = eb_cursor.fetchone()[0]

        # If the table name doesn't exist, insert the row
        if count == 0:
            insert_sql = f"""
                INSERT INTO {automation_logging}
                (channel, phase, schema_nm, table_or_proc_nm, target_table)
                VALUES ('{channel}', '{phase}', '{schema}', '{table}', '{target_table}');
            """
            eb_cursor.execute(insert_sql)
            eb_conn.commit()
        # Log the start of processing
        rsql=f"""
            INSERT INTO {log_table}
            (run_id, channel, phase, run_source, run_target, run_status, start_ts)
            VALUES ({run_id}, '{channel}', '{phase}', '{table}', '{schema}.{target_table}', 'running', CURRENT_TIMESTAMP);
        """
        eb_cursor.execute(rsql)
        eb_conn.commit()

        # Fetch all rows from the mahler database
        # if table != 'pusers':
        #     m_cursor.execute(f"SELECT * FROM `{table}`")
        #     rows = m_cursor.fetchall()
        # else: 
        #     m_cursor.execute(f"SELECT id,username,firstname,lastname,middlename,accessLevel, date_added, active, lastLogin, module_access, practiceID, npi_number, licenses, additional_username, additional_identifier, fax_number, last_modified, region_ids, regions, taxonomy FROM `{table}`")
        #     rows = m_cursor.fetchall()

        # Fetch column names and types from the mahler database
        if table != 'pusers':
            m_cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns_data = m_cursor.fetchall()
        else:
            m_cursor.execute(f"SHOW COLUMNS FROM `{table}` WHERE field IN ('id', 'username', 'firstname', 'lastname', 'middlename', 'accessLevel', 'date_added', 'active', 'lastLogin', 'module_access', 'practiceID', 'npi_number', 'licenses', 'additional_username', 'additional_identifier', 'fax_number', 'last_modified', 'region_ids', 'regions', 'taxonomy');")
            columns_data = m_cursor.fetchall()
            
        columns_names = ', '.join([sanitize_pg_name(column[0]) for column in columns_data])

        # Transform MySQL types to PostgreSQL types
        def map_data_types(data_type):
             # Define a dictionary to map MySQL to PostgreSQL data types
            map_dict = {
                "int": "bigint", # or integer if you know the numbers aren't very big
                "tinyint": "smallint", 
                "smallint": "smallint", 
                "mediumint": "integer",
                "bigint": "bigint",
                "float": "double precision",
                "double": "double precision",
                "decimal": "decimal",
                "date": "date",
                "datetime": "timestamp without time zone",
                "timestamp": "timestamp without time zone",
                "time": "time without time zone",
                "year": "integer",
                "char": "character",
                "varchar": "text",
                "binary": "bytea",
                "varbinary": "bytea",
                "tinyblob": "bytea",
                "tinytext": "text",
                "blob": "bytea",
                "text": "text",
                "mediumblob": "bytea",
                "mediumtext": "text",
                "longblob": "bytea",
                "longtext": "text",
                "enum": "text",
                "set": "text",
            }

            # Use the map_dict dictionary to map the data types
            return map_dict.get(data_type, "text") # Default to "text" if data type is not found
        
        columns_with_types = ', '.join([f"{sanitize_pg_name(column[0])} {map_data_types(column[1])}" for column in columns_data])

        # Check if the target table exists in the easebase database
        eb_cursor.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")
        table_exists = eb_cursor.fetchone()[0]
       # print(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")

        # Create backup table in the easebase database
        if table_exists:
            # If the table exists, create a backup table
            backup_table = f'{backup_schema}.{target_table}'
            eb_cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
            eb_cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {schema}.{target_table}")
        
      
        # Create new table in the easebase database
        
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}.{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}.{target_table} ({columns_with_types})")
   
        print(f"{schema}.{target_table} file dump starting...")
        # Write the data to a tab-delimited file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        #old memory intensive code    
        # with open(file_path, 'w', newline='') as tsvfile:
        #     writer = csv.writer(tsvfile, delimiter='|')
        #     for row in rows:
        #         writer.writerow(row)

        def export_mysql_to_tsv(table, m_cursor, file_path):
            #file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
            export_sql = f"""
            SELECT *
            INTO OUTFILE '{file_path}'
            FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '"' 
            LINES TERMINATED BY '\n'
            FROM {table};
            """
            m_cursor.execute(export_sql)

        # Example usage
        try:
            export_mysql_to_tsv(table, m_cursor, file_path)
        except Exception as e:
             print(e)

        # Open the tab-delimited file and load it into the PostgreSQL database
        with open(file_path, 'r') as f:
           # next(f)  # Skip the header row.
            eb_cursor.copy_expert(f"COPY {schema}.{target_table} FROM STDIN DELIMITER '|' CSV HEADER", f)
        
        print(f"{schema}.{target_table} complete...")
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
m_conn.close()
eb_conn.close()
