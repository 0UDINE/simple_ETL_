import pandas as pd
import sqlite3

#S3 bucket path for downloading the INSTRUCTOR.csv 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/INSTRUCTOR.csv'
csv_file_path = '/INSTRUCTOR.csv'
db_name = 'STAFF.db'
table_name = 'INSTRUCTOR'
table_name2 = 'Departments'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
conn = sqlite3.connect(db_name)


df = pd.read_csv(csv_file_path,names=attribute_list) # added the names arg to add header to the DF

df.to_sql(table_name,conn,if_exists='replace',index=False)
print('Table is ready')

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) AS NBR_STAFF FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}


data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
conn.close()