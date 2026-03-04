

import psycopg2 


conn = psycopg2.connect( 
    user='rahimsharifov',
    host='localhost',
    port=5432,
    database='postgres'
)
conn.autocommit = True

cur = conn.cursor()
cur.execute("DROP DATABASE IF EXISTS datawarehouse;")
print("dataware house dropped succesfully")

cur.execute("CREATE DATABASE datawarehouse")
print("Datawarehouse created succesfully")


cur.close()
conn.close()

new_conn = psycopg2.connect( 
    user='rahimsharifov',
    host='localhost',
    port=5432,
    database='datawarehouse'
)

print(new_conn)
new_cur = new_conn.cursor()

new_cur.execute("CREATE SCHEMA BRONZE")
new_cur.execute("CREATE SCHEMA SILVER")
new_cur.execute("CREATE SCHEMA GOLD")

new_conn.commit()

new_cur.close()
new_conn.close()