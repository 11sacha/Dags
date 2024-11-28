import os
import datetime
import pandas as pd
from dotenv import load_dotenv

FilePath = os.path.dirname(__file__)
print(FilePath)
os.chdir(FilePath)
os.chdir('../')
ProjectPath = os.getcwd()
print(ProjectPath)
FilesFolderPath = os.path.join(ProjectPath, r'Files')
print(FilesFolderPath)

