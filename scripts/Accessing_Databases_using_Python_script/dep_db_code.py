import pandas as pd
import sqlite3

#S3 bucket path for downloading the Departments.csv 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv'
csv_file_path = '/Departments.csv'
db_name = 'STAFF.db'
table_name2 = 'Departments'
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
conn = sqlite3.connect(db_name)

data_dict = {
    'DEPT_ID': 9 ,
    'DEP_NAME' : 'Quality Assurance',
    'MANAGER_ID' : '30010',
    'LOC_ID' : 'L0010'
}

df = pd.read_csv(csv_file_path,names=attribute_list)

df.loc[3] = data_dict

df.to_sql(table_name2,conn,if_exists='replace',index=False)
print('Data appended successfully')
query_statement = f"SELECT * FROM {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)