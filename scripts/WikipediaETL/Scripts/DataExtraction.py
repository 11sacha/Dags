import pandas as pd
import json
import requests
from bs4 import BeautifulSoup

current_date = datetime.now()
date = current_date.day
month = current_date.month

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()

def get_wikipedia_page(url):

    print('Getting wikipedia page from: ', url)
    try:
        response = requests.get(url, timeout=12)
        response.raise_for_status()

        return response.text
    except requests.RequestException as e:
        print(f"There's been an error: {e}")

def get_wikipedia_data(html):

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all('table', {"class": "wikitable sortable"})[0]
    table_rows = table.find_all('tr')

    return table_rows

