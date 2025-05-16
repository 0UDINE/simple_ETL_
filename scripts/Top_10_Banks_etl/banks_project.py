import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sqlite3


def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Fixed: format string
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    with open('code_log.txt', "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    # create an empty dataframe
    df = pd.DataFrame(columns=table_attribs)
    # extract the html code of the page in the url
    page_code = requests.get(url).text
    # transform it to an identifiable structure by using BeautifulSoup lib so I an extract the wanted data
    soup = BeautifulSoup(page_code, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')
    count = 0

    for row in rows[1:]:  # using slicing ,I skipped the table header
        columns = row.find_all('td')
        # extract the content of the second tag
        bank_name = columns[1].find_all('a')[1].text.strip()
        market_cap = columns[2].text.strip()
        # store the extracted data in this dict so I can append it to the dataframe
        data_dict = {'Name': bank_name, 'MC_USD_Billion': market_cap}
        df.loc[count] = data_dict  # append the extracted data into the dataframe
        # increment the counter so the next time i can append the data_dict without overwrite the existed stored data
        count += 1

    return df


def transform(df, csv_path):
    exchange_rate_df = pd.read_csv('./exchange_rate.csv')
    exchange_rate_dict = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, output_path):
    return df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    return df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_result = pd.read_sql(query_statement, sql_connection)
    print(query_result)


if __name__ == '__main__':

    log_progress('Preliminaries complete. Initiating ETL process.....')
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = ['Name', 'MC_USD_Billion']
    csv_path = './exchange_rate.csv'
    output_path = './Largest_banks_data.csv'
    db_name = 'Banks.db'
    table_name = 'Largest_banks'
    query_statement_1 = 'SELECT * FROM Largest_banks'
    query_statement_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    query_statement_3 = 'SELECT Name from Largest_banks LIMIT 5'
    try:
        log_progress('Initiating Extraction process.....')
        log_progress('Extract phase started')
        df = extract(url, table_attribs)
        print("extracted data : \n")
        print(df)
        if not df.empty:
            log_progress("Data extraction complete. Initiating Transformation process")
            log_progress("Transform phase started")
            df = transform(df, csv_path)
            print("Tranformed data : \n")
            print(df)
            log_progress("Data transformation complete. Initiatin loading process")
            log_progress("Load phase started")
            log_progress("load the data to a CSV file format :")
            load_to_csv(df, output_path)
            if os.path.exists(output_path):
                log_progress("the file " + output_path + "has been successufully created")
                if os.path.getsize(output_path) > 0:
                    log_progress("the file " + output_path + "has successufully loaded the transformed data")
                else:
                    log_progress("the file " + output_path + "has failed to load the data")
            else:
                log_progress("the file " + output_path + "has failed to create the json file")

            log_progress("SQL Connection initiated")
            sql_connection = sqlite3.connect(db_name)
            log_progress("load the data to the db " + db_name + "." + table_name + " . Running the query.......")
            load_to_db(df, sql_connection, table_name)
            log_progress('Data loaded to Database as a table, Executing queries')
            run_query(query_statement_1, sql_connection)
            run_query(query_statement_2, sql_connection)
            run_query(query_statement_3, sql_connection)
        else:
            log_progress("No data extracted. ETL process terminated")
    except Exception as e:
        log_progress(f"ETL Job Failed: {str(e)}")

    log_progress("ETL Job Ended")
    log_progress('Process Complete')
    sql_connection.close()
    log_progress('Server Connection closed')












