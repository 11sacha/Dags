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

def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id = client_id,
            client_secret = client_secret,
            user_agent = user_agent
        )
        print('Conection to Reddit successful.')
        return reddit
    except Exception as e:
        print(f"There's been an error: {e}")
        sys.exit(1)

def get_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):

    POST_FIELDS = (
        'id',
        'title',
        'score',
        'num_comments',
        'author',
        'created_utc',
        'url',
        'over_18',
        'edited',
        'spoiler',
        'stickied'
    )

    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(
        time_filter = time_filter,
        limit = limit
    )

    posts_list = []

    for post in posts:
        post_dict = vars(post)
        post = { key: post_dict[key] for key in POST_FIELDS }
        posts_list.append(post)

    return posts_list

def extract_data(subreddit: str, time_filter='day', limit=None):

    load_dotenv()

    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    #Connecting to reddit
    instance = connect_reddit(client_id, client_secret, "script:SachaGuimareyExtractor:v1.0 (by u/SnooFloofs157)")

    #Extracion
    posts = get_posts(instance, subreddit, time_filter, limit)

    posts_df = pd.DataFrame(posts)

    file_name = f'Posts_{month}_{date}.csv'

    posts_df.to_csv(os.path.join(FilesFolderPath, file_name))
    print(f'File saved in {FilesFolderPath}')

def run_process():

    extract_data('dataengineering', 'day', 100)






