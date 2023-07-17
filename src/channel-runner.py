#welcome to the easebase minion
from db.mahler_conn import connect_to_mysql_through_ssh

# Use the connection
conn = connect_to_mysql_through_ssh(
    ssh_host='mahlerdbview.mahlerhealth.com',
    ssh_username='goodsidehealthserveruser',
    ssh_parameter_name='db_mahler_msqltunnel_key',
    mysql_host='localhost',
    mysql_username='goodsidedbuser52',
    mysql_password='Ki4jJvkkKslekKVdjkDJkeiV42JKJK429JjkV19JKjksvlx',
    mysql_database='goodsidehealthDBview'
)

# Now you can use 'conn' object to interact with the database
cursor = conn.cursor()
cursor.execute("SELECT * FROM client_billing")
result = cursor.fetchall()
for row in result:
    print(row)

# Remember to close the connection when you're done - ChatGPT
conn.close()



# next steps:
# 1. use the connection to easebase to log the event.
# 2. Move into runner format (i.e. seperate file for the channel rather than code in this file) - reintroduce passthrough code
# 3. test with multiple files
# 4. add a section/design for passthrough sql files???
