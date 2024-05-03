import pymysql
import pandas as pd
import random

host = "localhost"
username = "squwsun"
password = "@2001n12Y28"
database_name = "test"

# Connect to MySQL database
connection = pymysql.connect(host=host,
                             user=username,
                             password=password,
                             database=database_name,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        names = ["Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry",
                 "Isabella", "Jack", "Katherine", "Liam", "Mia", "Nathan", "Olivia",
                 "Peter", "Quinn", "Rachel", "Samuel", "Tiffany"]
        for i in range(20):
            # SQL query to insert a row into a table
            sql_query = "INSERT INTO users (id, name, email, age) VALUES (%s, %s, %s, %s);"
            
            # Table name and values to insert into the table
            values = (i + 1, names[i], names[i] + '@sample.com', random.uniform(16, 50))
            
            # Execute the INSERT query
            cursor.execute(sql_query, values)
            
            # Commit the transaction
            connection.commit()
        
        print("Rows inserted successfully!")
        
except Exception as e:
    print("Error:", e)

finally:
    # Close the connection
    connection.close()