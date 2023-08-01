import boto3
import datetime
import csv
from io import StringIO
import psycopg2
from db.easebase_conn import easebase_conn


s3 = boto3.client("s3")
bucket = 'uc4k-db'
prefix= 'easebase/s_loads/s_mahler/'
eb_conn = easebase_conn()
eb_cursor = eb_conn.cursor()

#find key with pattern (unkown) but ending with today's date, for each key loop through to get the file name
# # step 1: hardcode array and filter out today's date, grab table name  

result = s3.get_object(Bucket = bucket, Key='easebase/s_loads/s_mahler/sa-facilities_2023_07_26.csv')
#print(result['Body'].read().decode('utf-8'))

# Use the list_objects_v2 method to list files in the specified subfolder
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

# Extract the list of filenames from the response
file_list = [obj['Key'][len(prefix):] for obj in response['Contents']]

#assign today's date
today_date = datetime.datetime.now().strftime('%Y_%m_%d')
print(today_date)

#return only the filenames with today's date in it
todays_files = [file for file in file_list if today_date in file]
print('Todays files', todays_files)

#grab the table names from those without the date
table_names = [file.split('_' + today_date)[0] for file in todays_files]
print(table_names)


file_key = 'easebase/s_loads/s_mahler/advanced_charts_2023_07_31.csv'

# Download the file from S3
response = s3.get_object(Bucket=bucket, Key=file_key)
content = response['Body'].read().decode('utf-8')
string_buffer = StringIO(content)
next(string_buffer)  # Skip the header row.

# Grab the table name from the filename
schema_name = 'stg'
table_name = 's_mahler_advanced_charts'
fully_qualified_table_name = f'{schema_name}.{table_name}'
print('Table Destination: ', fully_qualified_table_name)

try:
    print(string_buffer, fully_qualified_table_name, sep=',')
    eb_cursor.copy_from(string_buffer, fully_qualified_table_name, sep=',')
except Exception as e:
    print('Error loading data from file', file_key, 'into table', fully_qualified_table_name, ':', e)

# for file in todays_files:
#     print('File to be loaded: ', file)
#     # Download the file from S3
#     response = s3.get_object(Bucket=bucket, Key=file)
#     content = response['Body'].read().decode('utf-8')
#     string_buffer = StringIO(content)
#     next(string_buffer)  # Skip the header row.

#     # Grab the table name from the filename
#     table_name = file.split('_' + today_date)[0]
#     print('Table Destination: ', table_name)

#     try:
#         eb_cursor.copy_from(string_buffer, table_name, sep=',')
#     except Exception as e:
#         print('Error loading data from file', file, 'into table', table_name, ':', e)


# for file in todays_files:
#     print('Files to be loaded: ', file)
#     with open(file, 'r') as f:
#     # Notice that we don't need the csv module.
#         next(f) # Skip the header row.
#         table_name = f.split('_' + today_date)[0]
#         print('Table Destinations: ', table_name)
#         eb_cursor.copy_from(f, {table_name}, sep=',')

# for file in todays_files:
#     print(file)
#     with StringIO(file) as f:
#         reader = csv.reader(f)
#         header_row = next(reader)  # assign header = column names
#         print('headers: %s' % (header_row)) 
#         table_name = f.split('_' + today_date)[0]
#         for row in reader:
#             cur.execute(
#             "INSERT INTO users VALUES (%s, %s, %s, %s)",
#             row
#             )
#             query = f"INSERT INTO {table_name} col1, col2, col3 VALUES (%s, %s, %s)"
#             #cur.execute(query, (col1, col2, col3))
#             print(query)

eb_conn.commit
eb_conn.close()