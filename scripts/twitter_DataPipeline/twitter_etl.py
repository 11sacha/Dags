import tweepy
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import boto3
import os

ProjectPath = os.path.dirname(__file__)
print(ProjectPath)
OutputPath = os.path.join(ProjectPath, r'Output')
print(OutputPath)

def run_process():

    load_dotenv()

    #Import corresponding credentials
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
    ACCESS_SECRET = os.getenv('ACCESS_SECRET')
    CONSUMER_KEY = os.getenv('CONSUMER_KEY')
    CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
    BEARER_TOKEN = os.getenv('BEARER_TOKEN')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')


    client = tweepy.Client(
        bearer_token=BEARER_TOKEN,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_SECRET,
    )

    username = "elonmusk"
    #username = input('Type twitter username..')
    max_count = 100
    response = client.get_user(username=username)

    if response.data:
        user_id = response.data.id
        print(f"User ID for {username}: {user_id}")
    else:
        print("User not found.")

    tweets = client.get_users_tweets(
                                id=user_id,
                                max_results=max_count,
                                tweet_fields=["created_at", "public_metrics"],
                                exclude=["retweets"],
                                )

    tweet_list = []
    if tweets.data:
        for tweet in tweets.data:
            #text = tweet._json['full_text']

            redefined_tweet = {
                'user': username,
                'text': tweet.text,
                "favorite_count": tweet.public_metrics["like_count"],
                "retweet_count": tweet.public_metrics["retweet_count"],
                "created_at": tweet.created_at,
            }

            tweet_list.append(redefined_tweet)

    #Save file on local repo
    df = pd.DataFrame(tweet_list)
    file_name = f'Tweets_{username}_{datetime.now().strftime("%Y%m%d")}.csv'
    file_path = os.path.join(OutputPath, file_name)
    df.to_csv(file_path, index=False)

    print(f'Tweets save to {file_path}')

    #Upload file to S3 BUCKET
    print('Uploading to S3 Bucket..')
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    bucket_name = 'twitter-datapipeline'

    with open(file_path, 'rb') as f:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=f)

    print(f'Tweets uploaded to S3 as {file_name}')

    
#run_process()

