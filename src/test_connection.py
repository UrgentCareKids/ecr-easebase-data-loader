from db.mahler_conn import mahler_conn

# Retrieve the database connection object
conn = mahler_conn()

# Create a cursor object
cursor = conn.cursor()

try:
    # Execute the query
    cursor.execute("SELECT * FROM claims_reporting_data limit 10")

    # Fetch all rows
    rows = cursor.fetchall()

    # Print the results
    for row in rows:
        print(row)

except Exception as e:
    print("An error occurred:", str(e))

finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
