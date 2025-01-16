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

def run_process(**kwargs):
    data = kwargs['ti'].xcom_pull(key=rows, task_ids='extraction')
