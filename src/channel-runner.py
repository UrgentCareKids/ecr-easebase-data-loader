import boto3
import csv
import os
import time
from db.mahler_conn import mahler_conn
from db.easebase_conn  import easebase_conn
from datetime import datetime

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

#use the mount in the task for the connection
dir_path = '/easebase/'

#use unix time to identify the run id, append the channel name after
run_id = f"{int(time.time())}_mahler"
phase = 's_load' # update the phase for different ecr steps
table_name_prefix = 'stg.s_mahler_'
log_table = 'logging.eb_log'

# For each table
for table in tables:
    table = table[0]  # because fetchall() returns a list of tuples
       # Get all data from the table
    eb_cursor.execute("""
        INSERT INTO {log_table}
        (run_id, phase, run_source, run_target, run_status, latest_msg, start_ts)
        VALUES ({run_id}, {phase}, {table}, {table_name_prefix}{table}, 'running', true, CURRENT_TIMESTAMP);
    """) #logger 
    m_cursor.execute(f"SELECT * FROM `{table}`;")
    rows = m_cursor.fetchall()
    
    try:
     

        # Write the data to a csv file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.csv")
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in rows:
                writer.writerow(row)


        # Upload the csv file to S3 -- will be changed
        # s3_key = f"easebase/s_loads/s_mahler/{os.path.basename(file_path)}"
        # with open(file_path, 'rb') as data:
        #     s3.upload_fileobj(data, 'uc4k-db', s3_key)

        # If you want to keep the files locally, comment out the next line
        os.remove(file_path)

    except Exception as e:
        if table != 'pusers':
                sql="""
                    UPDATE {log_table}
                    SET latest_msg = false
                    WHERE run_id = {run_id} AND run_source = {table} AND latest_msg = true;
                    """
                eb_cursor.execute(sql)
                eb_conn.commit()  # Commit the log record update to the database

                sql="""
                    UPDATE {log_table}
                    SET run_status = 'failure', error_desc = %s, latest_msg = true, end_ts = CURRENT_TIMESTAMP
                    WHERE run_id = {run_id} AND run_source = {table};
                """, (str(e))
                eb_cursor.execute()
                eb_conn.commit()
        continue
    finally:
                 # Update all previous log records for this run_id and table to not be the latest
        eb_cursor.execute("""
            UPDATE {log_table}
            SET latest_msg = false
            WHERE run_id = {run_id} AND run_source = {table} AND latest_msg = true;
        """)
        eb_conn.commit()  # Commit the log record update to the database

        #then i am going to log table success
        eb_cursor.execute("""
            UPDATE {log_table}
            SET run_status = 'success', latest_msg = true, end_ts = CURRENT_TIMESTAMP
            WHERE run_id = {run_id} AND run_source = {table};
        """)

# Remember to close the connection when you're done
m_conn.close()
