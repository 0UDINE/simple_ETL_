import requests
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_100'
csv_path = '/top_100_films.csv'
df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0

try:
    html_page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).text
    data = BeautifulSoup(html_page, 'html.parser')

    table = data.find('table', {'class': 'wikitable'})

    if table:
        rows = table.find_all('tr')

    # Skip the header row
    for row in rows[1:]:
        if count < 100:
            col = row.find_all('td')
            if len(col) >= 4:  # only rows with at least 3 columns

                data_dict = {'Film': col[1].text.strip(),
                             'Year': 0 if col[2].text.strip() == 'unranked' else int(col[2].text.strip()),
                             'Rotten Tomatoes\' Top 100': col[3].text.strip()
                             }
                df.loc[count] = data_dict
                count += 1
            else:
                break

    print(
        df[df['Year'] >= 2000])  # Filter the output to print only the films released in the 2000s (year 2000 included)
except Exception as e:
    print(e)

# Storing the data
df.to_csv(csv_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
