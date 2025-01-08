### Loading Data into S3 Bucket ###

import sys
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import os
import s3fs
import traceback

current_date = datetime.now()
date = current_date.day
month = current_date.month

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
FilesFolderPath = os.path.join(ProjectPath, r'Files')
TransformedFilePath = os.path.join(FilesFolderPath, 'TransformedData.csv')

load_dotenv()

#Import corresponding credentials
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

def s3_connection():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                                key=AWS_ACCESS_KEY,
                                secret=AWS_SECRET_KEY)
        return s3
    except Exception as e:
        print(f"There's been an error: {e}")

def create_bucket(s3: s3fs.S3FileSystem, bucket: str):
    try:
        if not s3.exists(bucket):
            s3.makedir(bucket)
            print('Bucket created successfully.')
        else:
            print("Bucket already exists")

    except Exception as e:
        print(f"There's been an error: {e}")

def upload_s3(s3: s3fs.S3FileSystem, file_name: str, bucket: str, file_path: str):
    try:
        with open(file_path, 'rb') as f:
            s3.put_object(Bucket=bucket, Key=file_name, Body=f)
        print(f'File uploaded to S3 as {file_name}')

    except FileNotFoundError:
        print(f"There's been an error..")
        traceback.print_exc()


def run_process():

    bucket_name = 'reddit-etl97'
    file_name = 'TransformedData.csv'

    s3 = s3_connection()

    create_bucket(s3,bucket_name)

    upload_s3(s3, file_name, bucket_name, TransformedFilePath)




