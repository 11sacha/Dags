import os
import warnings
import pandas as pd
from sqlalchemy import create_engine, Sequence, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

# Define relative routes
FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
FilesFolderPath = os.path.join(ProjectPath, r'Files')
BooksFile = os.path.join(FilesFolderPath, 'BooksFile.csv')

#load_dotenv()
#DatabasePath = os.getenv(DB_PATH)
DatabasePath = '/Users/sachaguimarey/Documents/DE/personal_db'

print('Connecting to SQLite database...')
# SQLite connection string (using absolute path to the database file)
engine = create_engine(f"sqlite:///{DatabasePath}")
print('Connection successful!')

Base = declarative_base()

class AmazonBooks(Base):
    __tablename__ = 'amazonbooks'
    id = Column(Integer, Sequence('books_id_seq', optional=True), primary_key=True)  # Sequence is optional in SQLite
    title = Column(String)
    author = Column(String)
    price = Column(Integer)
    rating = Column(String)

df = pd.read_csv(BooksFile)

Base.metadata.create_all(engine)

try:
    #with engine.connect() as conn:
        # Save the DataFrame to the SQLite database
        df.to_sql('amazonbooks', con=engine, index=False, if_exists='append')
        print("Data saved successfully!")
except Exception as e:
    print(f"There's been an error: {e}")
