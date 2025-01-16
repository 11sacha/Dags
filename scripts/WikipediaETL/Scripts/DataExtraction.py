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

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦️'):
        text = text.split(' ♦️')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')

def run_process(**kwargs):

    try:
        url = kwargs['url']
        html = get_wikipedia_page(url)
        rows = get_wikipedia_data(html)

        data = []

        for i in range(1, len(rows)):
            tds = rows[i].find_all('td')
            values = {
                'rank': i,
                'stadium': clean_text(tds[0].text),
                'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
                'region': clean_text(tds[2].text),
                'country': clean_text(tds[3].text),
                'city': clean_text(tds[4].text),
                'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
                'home_team': clean_text(tds[6].text),
            }
            data.append(values)

        json_rows = json.dumps(data)
        kwargs['ti'].xcom_push(key='rows', value=json_rows)

        return 'Data extracted'

    except Exception as e:
        print(f"There's been an error: {e}")