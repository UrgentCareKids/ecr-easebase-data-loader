import os
import subprocess
import tempfile
import boto3
import mysql.connector  # make sure you have installed this module

def get_ssh_key_from_parameter_store(parameter_name):
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    ssh_key = response['Parameter']['Value']

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(ssh_key.encode('utf-8'))
        ssh_key_path = temp_file.name

    os.chmod(ssh_key_path, 0o600)

    return ssh_key_path

def connect_to_mysql_through_ssh(ssh_host, ssh_username, ssh_parameter_name, mysql_host, mysql_username, mysql_password, mysql_database):
    ssh_key_path = get_ssh_key_from_parameter_store(ssh_parameter_name)

    try:
        ssh_command = [
            'ssh', '-v', '-i', ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-L', '3306:' + mysql_host + ':3306',
            '-N', '-f', '-l', ssh_username, ssh_host
        ]

        process = subprocess.Popen(ssh_command)
        process.wait()
        if process.returncode != 0:
            raise Exception(f'SSH command failed with return code: {process.returncode}')

        connection = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user=mysql_username,
            passwd=mysql_password,
            database=mysql_database
        )
        
        return connection

    finally:
        os.remove(ssh_key_path)

# Example usage
conn = connect_to_mysql_through_ssh(
    ssh_host='mahlerdbview.mahlerhealth.com',
    ssh_username='goodsidehealthserveruser',
    ssh_parameter_name='db_mahler_msqltunnel_key',
    mysql_host='localhost',
    mysql_username='goodsidedbuser52',
    mysql_password='Ki4jJvkkKslekKVdjkDJkeiV42JKJK429JjkV19JKjksvlx',
    mysql_database='goodsidehealthDBview'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM client_billing")
rows = cursor.fetchall()
with open('output.txt', 'w') as f:
    for row in rows:
        f.write(', '.join([str(elem) for elem in row]))
        f.write('\n')


cursor.close()
conn.close()
