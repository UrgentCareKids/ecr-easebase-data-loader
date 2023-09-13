import pyodbc
import boto3
import json
import os


def emds_conn():
    ssm = boto3.client('ssm')
    #ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
    param = ssm.get_parameter(Name='db_sqlserver_emds', WithDecryption=True )
    db_request = json.loads(param['Parameter']['Value']) 
    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']


    # Define the ODBC connection string for SQL Server
    # conn_str = (
    #     f"DRIVER={ODBC Driver 18 for SQL Server};"
    #     f"SERVER={hostname},{portno};"
    #     f"DATABASE={dbname};"
    #     f"UID={dbusername};"
    #     f"PWD={dbpassword};"
    # )

    server = db_request['host']
    database = db_request['database']
    username = db_request['user']
    password = db_request['password']

    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;' 

    try:

        conn = pyodbc.connect(connection_string)
        #cursor = conn.cursor()
        print("Connected to SQL Server successfully!")
        return conn

    except Exception as e:
        print(f"Error: {str(e)}")


    # Connection string
    # conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' +
    #     hostname+';DATABASE='+dbname+';UID='+dbusername+';PWD=' + dbpassword+'; Encrypt=yes')
    # cursor = conn.cursor()

    #conn = pyodbc.connect(conn_str)
    #conn.autocommit = False
    #return conn
