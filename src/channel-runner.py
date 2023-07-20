import boto3
import csv
import os
from db.mahler_conn import mahler_connect
from datetime import datetime

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Connect to your database
conn = mahler_connect()
cursor = conn.cursor()

# Get all table names in your database
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

dir_path = '/easebase/'
# For each table
for table in tables:
    table = table[0]  # because fetchall() returns a list of tuples
    try:
        # Get all data from the table
        print(f"Pulling from {table}")
        cursor.execute(f"SELECT * FROM `{table}`;")
        rows = cursor.fetchall()

        # Write the data to a csv file in the specified directory
        file_path = os.path.join(dir_path, f"{table}_{datetime.now().strftime('%Y_%m_%d')}.csv")
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in rows:
                writer.writerow(row)

        # Upload the csv file to S3
        s3_key = f"easebase/s_loads/s_mahler/{os.path.basename(file_path)}"
        with open(file_path, 'rb') as data:
            s3.upload_fileobj(data, 'uc4k-db', s3_key)

        # If you want to keep the files locally, comment out the next line
        os.remove(file_path)

    except Exception as e:
        print(f"An error occurred with table {table}: {str(e)}")
        continue

# Remember to close the connection when you're done
conn.close()
