# Freshservice API pull to get Assets
# API documentation: https://api.freshservice.com
# API key obtained from Freshservice: https://support.freshservice.com/en/support/solutions/articles/50000000306-where-do-i-find-my-api-key-

import requests
import base64
import boto3
import json
import os
import sys
import time
from datetime import datetime
import psycopg2
from dateutil.parser import isoparse
import re
from db.easebase_conn import easebase_conn
import csv

# variables 
all_assets = []

# Define the constants
run_id = int(time.time())
phase = 's_load'
schema = 'stg.'
table_name_prefix = 's_freshservice_'
log_table = 'logging.eb_log'
channel = 'freshservice'
backup_schema='stg_backup.'
target_table = 's_freshservice_assets'
dir_path = '/easebase/'

#Get Api Key and other API call Parameters
ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
param = ssm.get_parameter(Name='eb-etl-easebase-freshservice', WithDecryption=True )
api_key = json.loads(param['Parameter']['Value'])['api_key']

api_url_assets = 'https://urgentcarekids.freshservice.com/api/v2/assets'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ' + base64.b64encode(api_key.encode()).decode()
}


# Define Functions
def remove_non_letters(input_string):
    return re.sub(r'[^a-zA-Z ]', '', input_string)

# Helper function to get the data type of a value
def get_data_type(value):
    if value is None:
        return "text"  # Default to "str" if the value is None
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "bigint"
    elif isinstance(value, float):
        return "double precision"
    elif isinstance(value, datetime):
        return "timestamp without time zone"
    return "text"

# Helper function to convert timestamp to the appropriate format
def format_timestamp(timestamp_str):
    try:
        # Using dateutil.parser to parse the ISO 8601 timestamp with timezone
        timestamp = isoparse(timestamp_str)
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error formatting timestamp: {e}")
        return None

# get Asset Payload
response = requests.get(api_url_assets, headers=headers)
try:
    #Create Connection
    eb_conn = easebase_conn()
    eb_cursor = eb_conn.cursor()

 # Log the start of processing
    rsql=f"""
        INSERT INTO {log_table}
        (run_id, channel, phase, run_source, run_target, run_status, start_ts)
        VALUES ({run_id}, '{channel}', '{phase}', '{target_table}', '{schema}{target_table}', 'running', CURRENT_TIMESTAMP);
        """
    eb_cursor.execute(rsql)
    eb_conn.commit()

    if response.status_code == 200:
        all_assets = response.json()
        print('API call was successful')
            
        # Access the "assets" list
        assets_list = all_assets.get("assets", [])

        

#Get Column Names with Types

        # Dictionary to store the data types for each key
        data_types_dict = {}

        # Initialize a set to store the keys (use a set to ensure uniqueness)
        keys_set = set()

        # Loop through each dictionary in the "assets" list and get their keys
        for asset in assets_list:
            keys_set.update(asset.keys())

        # Accessing the keys of the dictionaries inside the "assets" list
        asset_keys_list = list(assets_list[0].keys())  # Assuming you want the keys of the first asset dictionary

        # Scan the values for each key in the list and determine their data types
        for key in asset_keys_list:
            data_types = {get_data_type(asset.get(key)) for asset in all_assets.get("assets", [])}
            #If there are multiple data types for a key, and one of them is "int", default to "str"
            if "bigint" in data_types and len(data_types) > 1:
                data_types_dict[key] = "text"
            else:
                # Convert the set to a single string representation
                data_types_string = ', '.join(data_types)
                data_types_dict[key] = data_types_string

        # Generate the flat string with keys and data types
        columns_with_types = ', '.join(f'{key} {value}' for key, value in data_types_dict.items())

        # Check if the target table exists in the easebase database
        eb_cursor.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{target_table}'")
        table_exists = eb_cursor.fetchone()[0]

#Create Easebase Tables
  
        # Create backup table in the easebase database
        if table_exists:
            # If the table exists, create a backup table
            backup_table = f'{backup_schema}{target_table}'
            eb_cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
            eb_cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {schema}{target_table}")
            print(f"CREATE TABLE {backup_table} AS SELECT * FROM {schema}{target_table}")

        # Create new table in the easebase databas
        print(f"CREATE TABLE {schema}{target_table} ({columns_with_types})")
        eb_cursor.execute(f"DROP TABLE IF EXISTS {schema}{target_table}")
        eb_cursor.execute(f"CREATE TABLE {schema}{target_table} ({columns_with_types})")
        eb_conn.commit()

    #Insert Asset Data into Easebase

    # Write the data to a tab-delimited file in the specified directory
        file_path = os.path.join(dir_path, f"{target_table}_{datetime.now().strftime('%Y_%m_%d')}.tsv")
        print(file_path)
        with open(file_path, 'w', newline='') as tsvfile:
            writer = csv.writer(tsvfile, delimiter='|')
            #Write rows
            for row in assets_list:
                writer.writerow(row.values())
        print("CSV file created successfully.")

        # Open the tab-delimited file and load it into the PostgreSQL database
        # with open(file_path, 'r') as f:
        #     try:
        #         # Attempt to load the data, catching the out-of-range error
        #         eb_cursor.copy_expert(f"COPY {schema}{target_table} FROM STDIN DELIMITER '|' CSV HEADER", f)
        #     except psycopg2.DataError as e:
        #         # Handle the out-of-range error
        #         print("Caught error:", e)
        #         print("Replacing problematic pg_dob values with NULL...")
        #         f.seek(0)  # Reset the file pointer




        # #Generate the parameterized SQL INSERT statement
        # columns = ', '.join(asset_keys_list)
        # values = ', '.join('%s' for _ in asset_keys_list)
        # rsql = f"INSERT INTO {schema}{target_table} ({columns}) VALUES ({values})"

        # # Loop through each dictionary in the "assets" list and extract their values
        # for asset in assets_list:
        #     # Convert the timestamp strings to datetime objects
        #     for key, value in asset.items():
        #         if isinstance(value, str) and value.endswith('Z'):
        #             asset[key] = datetime.fromisoformat(value[:-1])

        #     # Get the values as a tuple to use in the query
        #     asset_values = tuple(asset.values())

        # # Generate the values part of the INSERT statement with formatted data
        # #values_string = ", ".join(f"({', '.join(map(str, row_values))})" for row_values in values_list)

        #     rsql = f"INSERT INTO {schema}{target_table} ({columns}) VALUES ({values})"
        #     # Get the values as a tuple to use in the parameterized query
        #     asset_values = tuple(asset[key] for key in asset_keys_list)
        #     try:
        #         # Execute the INSERT statement with the asset values
        #         eb_cursor.execute(rsql, asset_values)
        #         eb_conn.commit()
        #         print('Data Insertion was successful')
        #     except Exception as e:
        #     # Handle any exceptions that may occur during insertion
        #         err = remove_non_letters(str(e))
        #         rsql=f"""
        #             UPDATE {log_table}
        #             SET run_status = 'failure', error_desc = '{err[:255]}', end_ts = CURRENT_TIMESTAMP
        #             WHERE run_id = {run_id} AND run_source = '{target_table}' and channel = '{channel}';
        #             """
        #         eb_cursor.execute(rsql)
        #         eb_conn.commit()
        #         print(f"Failed to insert data. Error: {e}")

    else:
        print(f"Failed to fetch assets. Status code: {response.status_code}")
        print(f"Response content: {response.text}")
        #Update Logging
        rsql=f"""
        UPDATE {log_table}
        SET run_status = 'failed', error_desc = '{response.text[:255]}', end_ts = CURRENT_TIMESTAMP
        WHERE run_id = {run_id} AND run_source = '{target_table}' and channel = '{channel}';
        """
    eb_cursor.execute(rsql)
    eb_conn.commit()
except Exception as e:
    #if table != 'pusers':
    err = remove_non_letters(str(e))
    rsql=f"""
        UPDATE {log_table}
        SET run_status = 'failed', error_desc = '{err[:255]}', end_ts = CURRENT_TIMESTAMP
        WHERE run_id = {run_id} AND run_source = '{target_table}' and channel = '{channel}';
        """
    eb_cursor.execute(rsql)
    eb_conn.commit()

eb_cursor.close()
