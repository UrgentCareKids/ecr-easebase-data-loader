import os
import subprocess
import tempfile
import boto3


def get_ssh_key_from_parameter_store(parameter_name):
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    ssh_key = response['Parameter']['Value']

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(ssh_key.encode('utf-8'))
        ssh_key_path = temp_file.name

    # Change permissions of the ssh key file
    os.chmod(ssh_key_path, 0o600)

    return ssh_key_path


def connect_to_mysql_through_ssh(ssh_host, ssh_username, ssh_parameter_name, mysql_host, mysql_username, mysql_password):
    # Get the SSH private key file from AWS Parameter Store
    ssh_key_path = get_ssh_key_from_parameter_store(ssh_parameter_name)

    # Create a temporary known_hosts file to avoid strict key checking
    known_hosts_path = os.path.join(tempfile.gettempdir(), 'known_hosts')
    with open(known_hosts_path, 'w') as known_hosts_file:
        known_hosts_file.write(f"{ssh_host} ssh-rsa <RSA public key>")

    try:
        # SSH command to establish the tunnel
        ssh_command = [
            'ssh', '-v', '-i', ssh_key_path,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=' + known_hosts_path,
            '-L', '3306:' + mysql_host + ':3306',
            '-N', '-f', '-l', ssh_username, ssh_host
        ]

        # Start the SSH tunnel and check for errors
        process = subprocess.Popen(ssh_command)
        process.wait()
        if process.returncode != 0:
            raise Exception(f'SSH command failed with return code: {process.returncode}')

        # MySQL connection command
        mysql_command = [
            'mysql',
            '-u', mysql_username,
            '-p' + mysql_password,
            '-h', '127.0.0.1',
            '-P', '3306'
        ]

        # Connect to MySQL through the SSH tunnel
        subprocess.call(mysql_command)

        # Print success message
        print("Connected to MySQL database through SSH tunnel.")

    finally:
        # Clean up temporary files after MySQL connection is closed
        pass

    os.remove(ssh_key_path)
    os.remove(known_hosts_path)


# Example usage
connect_to_mysql_through_ssh(
    ssh_host='mahlerdbview.mahlerhealth.com',
    ssh_username='goodsidehealthserveruser',
    ssh_parameter_name='db_mahler_msqltunnel_key',
    mysql_host='localhost',
    mysql_username='goodsidedbuser52',
    mysql_password='Ki4jJvkkKslekKVdjkDJkeiV42JKJK429JjkV19JKjksvlx'
)
