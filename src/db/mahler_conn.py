import os
import subprocess
import tempfile
import boto3
import mysql.connector  # make sure you have installed this module
import time
import json

def get_ssh_key_from_parameter_store(parameter_name):
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    ssh_key = response['Parameter']['Value']

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(ssh_key.encode('utf-8'))
        ssh_key_path = temp_file.name

    os.chmod(ssh_key_path, 0o600)

    return ssh_key_path

def get_mahler_param(param_name):
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameter(Name='db_mysql_mahler', WithDecryption=True)
    param_response = json.loads(response['Parameter']['Value'])[param_name]
    return param_response


def connect_to_mysql_through_ssh(ssh_host, ssh_username, ssh_parameter_name, mysql_host, mysql_username, mysql_password, mysql_database):
    ssh_key_path = get_ssh_key_from_parameter_store(ssh_parameter_name)

    try:
        ssh_command = [
            'ssh', '-i', ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-L', '3306:' + mysql_host + ':3306',
            '-N', '-f', '-l', ssh_username, ssh_host
        ]

        # Start the SSH tunnel process
        process = subprocess.Popen(ssh_command)
        print(f"SSH command started. PID: {process.pid}")
        process.wait()
        if process.returncode != 0:
            raise Exception(f'SSH command failed with return code: {process.returncode}')

        # Wait for the SSH tunnel to establish
        time.sleep(5)

        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user=mysql_username,
            passwd=mysql_password,
            database=mysql_database
        )
        
        return connection

    finally:
        #this doesn't seem to close the SSH tunnel, if you run sudo lsof -i :3306 in the terminal it still shows up
        process.terminate()
        os.remove(ssh_key_path)

def mahler_conn():
    ssh_host=get_mahler_param('ssh_hostname')
    ssh_username=get_mahler_param('ssh_username')
    ssh_parameter_name='db_mahler_msqltunnel_key'
    mysql_host='localhost'
    mysql_username=get_mahler_param('user')
    mysql_password=get_mahler_param('password')
    mysql_database=get_mahler_param('database')
   
    return connect_to_mysql_through_ssh(
        ssh_host,
        ssh_username,
        ssh_parameter_name,
        mysql_host,
        mysql_username,
        mysql_password,
        mysql_database
    )

    

# Example usage
