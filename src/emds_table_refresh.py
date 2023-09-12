# this script is meant to refresh the tables emds_invoice_summary and emds_invoice_line on the emds sql server side
# it will be executed in the step function easebase_emds_load while sql server is still up
import pyodbc
import sys
print("hello I made it here")
#set the import path for db
sys.path.append('./db')

from db.emds_conn import emds_conn

#print(pyodbc.drivers())
# Connect to your databases
emds_conn = emds_conn()
emds_cursor = emds_conn.cursor()


emds_cursor.execute("USE uc4k")
emds_cursor.execute("EXEC EXECUTE dbo.emds_gen_invoice_summary")