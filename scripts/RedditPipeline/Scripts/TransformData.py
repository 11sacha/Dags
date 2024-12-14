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