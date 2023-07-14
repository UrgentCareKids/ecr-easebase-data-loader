import subprocess
import psycopg2
import sys
from db.easebase_conn import easebase_conn

def handler(filename, *args):
    command = ['python3', filename] + list(args)
    result = subprocess.run(command, capture_output=True, text=True)
    print(result.stdout)

    # _targetconnection = easebase_conn()


handler('src/hello.py')
handler('src/hello2.py', 'yooooo')