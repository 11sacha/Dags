import sys
import numpy as np
import pandas as pd
import praw
from praw import Reddit
from dotenv import load_dotenv
from datetime import datetime

current_date = datetime.now()
date = current_date.day
month = current_date.month

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
print(ProjectPath)
FilesFolderPath = os.path.join(ProjectPath, r'Files')
file_name = f'Posts_{month}_{date}.csv'
ExtractedData = os.path.join(FilesFolderPath, file_name)
TransformedFilePath = os.path.join(FilesFolderPath, 'TransformedData.csv')


def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]), post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)
    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

def run_process():

    df = pd.read_csv(ExtractedData)

    transform_data(df)

    load_data_to_csv(df, TransformedFilePath)
