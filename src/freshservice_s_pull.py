# Freshservice API pull to get Assets
# API documentation: https://api.freshservice.com
# API key obtained from Freshservice: https://support.freshservice.com/en/support/solutions/articles/50000000306-where-do-i-find-my-api-key-

import requests
import base64
import boto3
import json
import os
import sys

# variables
schema = 'stg'
all_assets = []

# Get Easebase Module
current_dir = os.path.dirname(os.path.abspath(__file__)) # Get the absolute path of the current directory
parent_dir = os.path.abspath(os.path.join(current_dir, '..')) # Go up one level to the parent directory and append it to sys.path
sys.path.append(parent_dir)
from db.easebase_conn import easebase_conn


#Get Api Key and other API call Parameters
ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
param = ssm.get_parameter(Name='eb-etl-easebase-freshservice', WithDecryption=True )
api_key = json.loads(param['Parameter']['Value'])['api_key']

api_url_assets = 'https://urgentcarekids.freshservice.com/api/v2/assets'

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ' + base64.b64encode(api_key.encode()).decode()
}



# Define functions
def get_payload_assets():
    response = requests.get(api_url_assets, headers=headers)

    if response.status_code == 200:
        all_assets = response.json()
        print('API call was successful')
        # Process the assets as needed
        insert_into_easebase('s_freshservice_assets_json',all_assets)
        print('Data Load was successful')
    else:
        print(f"Failed to fetch assets. Status code: {response.status_code}")
        print(f"Response content: {response.text}")


def insert_into_easebase(tbl_nm, payload):
    _targetconnection = easebase_conn()
    print('Connection established')
    cur = _targetconnection.cursor()
    payload = json.dumps(payload)
    print('Payload assigned')
    insert_query = f"insert into {schema}.{tbl_nm} (payload) values(json_parse('{payload}'));"
    cur.execute(insert_query,)
    print('Payload loaded into Easebase')
    _targetconnection.commit()

def clear_table(tbl_nm):
    _targetconnection = easebase_conn()
    cur = _targetconnection.cursor()
    cur.execute(f"delete from {schema}.{tbl_nm};")
    _targetconnection.commit()


def log(tbl_nm, schema, message, status):
    _targetconnection = easebase_conn()
    cur = _targetconnection.cursor()
    cur.execute(f"insert into public.etl_log(tbl_nm, schema_nm, message, status) values( '{tbl_nm}', '{schema}', '{message}', '{status}');")
    _targetconnection.commit()


def main():
    try:
        #log('assets_json',schema,'process kicked off', 'info')
        #phase 1 json load
        get_payload_assets()
        #log('assets_json',schema,'process kicked off', 'info')
        #kick off update proc:
        #refresh_table('assets')
        print('Successfully loaded')

    except Exception as e:
        print(e)
        #log('assets_json',schema,e, 'error')


main()
    
