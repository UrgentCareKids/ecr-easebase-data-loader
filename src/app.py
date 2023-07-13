import subprocess
import psycopg2
from db.easebase_conn import easebase_conn

def handler(filename):
    # subprocess.run(['python3', filename], stdout=subprocess.PIPE, text=True)

    _targetconnection = easebase_conn()
    print(_targetconnection)


handler('test')