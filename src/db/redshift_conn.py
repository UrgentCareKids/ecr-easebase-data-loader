import psycopg2
import boto3
import json
import os


def redshift_conn():
    ssm = boto3.client('ssm')
    #ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
    param = ssm.get_parameter(Name='db_redshift_dev', WithDecryption=True )
    db_request = json.loads(param['Parameter']['Value']) 

    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    conn.autocommit = False
    return conn


def get_aws_credentials():
    secrets_manager = boto3.client('secretsmanager')
    secret_name = 'dev/etl'

    try:
        response = secrets_manager.get_secret_value(
            SecretId=secret_name
        )
        secret_string = response['SecretString']
        db_request = json.loads(secret_string)
        return db_request
    except Exception as e:
        print(f"Error retrieving AWS credentials from Secrets Manager: {e}")
        return None

   