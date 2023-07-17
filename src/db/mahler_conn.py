import pymysql
import paramiko
import boto3
import base64
import tempfile

def ssh_connect(secret_name, region_name):
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_binary = response['SecretBinary']
    decoded_binary_secret = base64.b64decode(secret_binary)
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(decoded_binary_secret)
        temp_file_path = temp_file.name
        print("Temporary File Path:", temp_file_path)

    private_key = paramiko.RSAKey.from_private_key_file(temp_file_path)

    parameter_name = "db_mysql_mahler"
    ssm_client = boto3.client("ssm", region_name=region_name)
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    parameter_value = eval(parameter_value)  # Convert string representation to dictionary

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=parameter_value['ssh_hostname'],
        username=parameter_value['ssh_username'],
        pkey=private_key
    )

    return ssh_client

def mahler_conn():
    secret_name = "ecr-easebase-mahler-db"
    region_name = "us-east-2"
    parameter_name = "db_mysql_mahler"

    ssm_client = boto3.client("ssm", region_name=region_name)
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    parameter_value = eval(parameter_value)  # Convert string representation to dictionary

    ssh_client = ssh_connect(secret_name, region_name)

    # Perform SSH operations if needed

    conn = pymysql.connect(
        host=parameter_value['host'],
        port=parameter_value['port'],
        user=parameter_value['user'],
        password=parameter_value['password'],
        database=parameter_value['database']
    )
    conn.autocommit = False
    return conn

if __name__ == "__main__":
    mahler_conn()
