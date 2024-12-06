import os
import warnings
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Sequence, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

warnings.filterwarnings('ignore')

def order_df_columns(df, new_order):
    return df.reindex(columns=new_order)

def run_process():
    # Define relative routes
    FilePath = os.path.dirname(__file__)
    os.chdir(os.path.join(FilePath, '../'))
    ProjectPath = os.getcwd()
    print(ProjectPath)
    FilesFolderPath = os.path.join(ProjectPath, r'Files')
    BooksFile = os.path.join(FilesFolderPath, 'BooksFile.csv')

    load_dotenv()

    Database = os.getenv('DB_NAME')
    Username = os.getenv('DB_USER')
    Password = os.getenv('DB_PASSWORD')
    Schema = 'dags'

    print(Database)

    print('Connecting to db...')
    engine = create_engine(
        f"postgresql+psycopg2://{Username}:{Password}@localhost:5432/{Database}?options=-csearch_path%3Ddags"
    )
    print('Connection successful!')

    print('Data transformation begins..')
    Base = declarative_base()

    class AmazonBooks(Base):
        __tablename__ = 'amazonbooks'
        __table_args__ = {'schema': Schema}
        id = Column(Integer, Sequence('books_id_seq'), primary_key=True)
        title = Column(String)
        author = Column(String)
        price = Column(Integer)
        rating = Column(String)

    df = pd.read_csv(BooksFile)
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    df['title'] = df['Title']
    df['author'] = df['Author']
    df['price'] = df['Price']
    df['rating'] = df['Rating']

    new_order = (['title', 'author', 'price', 'rating'])

    df = order_df_columns(df, new_order)

    print('Data transformation finished.')

    try:
        with engine.connect() as conn:
            
        #with engine.begin() as conn:
            df.to_sql('amazonbooks', con=conn, schema=Schema, index=False, if_exists='append')
            print("Data Saved!")
        #print('Data saved successfully!')
    except Exception as e:
        print(f"There's been an error: {e}")
