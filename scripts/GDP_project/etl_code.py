import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime
import sqlite3
import os


def extract(url, table_attribs):
    # creation of a empty df
    df = pd.DataFrame(columns=table_attribs)
    data = requests.get(url).text
    soup = BeautifulSoup(data, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')
    count = 0

    for row in rows[3:]:
        col = row.find_all('td')
        data_dict = {'Country': col[0].text.strip(),
                     'GDP_USD_billion': col[2].text.strip()}
        df.loc[count] = data_dict
        count += 1
    return df


def transform(df):
    # Step 1: Replace underscores with NaN (use np.nan, not string 'NaN')
    df['GDP_USD_billion'] = df['GDP_USD_billion'].replace('_', np.nan)

    # Step 2: Remove commas (replace with empty string, not space)
    df['GDP_USD_billion'] = df['GDP_USD_billion'].str.replace(',', '', regex=False)

    # Step 3: Convert to float (handles NaN gracefully)
    df['GDP_USD_billion'] = pd.to_numeric(df['GDP_USD_billion'], errors='coerce')
    df['GDP_USD_billion'] = df['GDP_USD_billion'] / 1000
    df['GDP_USD_billion'] = df['GDP_USD_billion'].round(2)

    return df


def load_to_json(df, json_path):
    df.to_json(json_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    query_results = pd.read_sql(query_statement, sql_connection)
    print("The query:\n" + query_statement + "\n")
    print("The query result:\n", query_results, "\n")


def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Fixed: format string
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    with open('etl_project_log.txt', "a") as f:
        f.write(timestamp + ' ' + message + '\n')


if __name__ == '__main__':
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    json_path = './Countries_by_GDP.json'
    database_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'
    sql_connection = sqlite3.connect(database_name)

    log_progress("ETL job stared")
    try:
        log_progress("extract phase started")
        df = extract(url, ['Country', 'GDP_USD_billion'])
        print("extracted data : \n")
        print(df)
        if not df.empty:
            log_progress("data extraction complete. Initiating transformation process")
            log_progress("Transform phase started")
            df = transform(df)
            print("Tranformed data : \n")
            print(df)
            log_progress("Data transforlation complete. Initiatin loading process")
            log_progress("Load phase started")
            log_progress("load the data to a json file format :")
            load_to_json(df, json_path)
            if os.path.exists(json_path):
                log_progress("the file " + json_path + "has been successufully created")
                if os.path.getsize(json_path) > 0:
                    log_progress("the file " + json_path + "has successufully loaded the transformed data")
                else:
                    log_progress("the file " + json_path + "has failed to load the data")
            else:
                log_progress("the file " + json_path + "has failed to create the json file")

            log_progress("SQL Connection initiated")
            log_progress("load the data to the db " + database_name + "." + table_name + " . Running the query:")
            load_to_db(df, sql_connection, table_name)
            log_progress("Load phase ended")
            log_progress("ETL Job Completed Successfully")
            log_progress(
                "Running a query on the database table to display only the entries with more than a 1000 billion USD economy :")
            query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billion > 1000"
            run_query(query_statement, sql_connection)
        else:
            log_progress("No data extracted. ETL process terminated")
    except Exception as e:
        log_progress(f"ETL Job Failed: {str(e)}")

    log_progress("ETL Job Ended")
    sql_connection.close()










