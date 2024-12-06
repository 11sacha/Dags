from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.AmazonBooksETL.Scripts import GetBooks, InsertDataDB 

with DAG(   dag_id = 'AMB',
            start_date = datetime(2021, 1,1),
            schedule = '0 10 * * 1',
            tags=['Amazon', 'ETL'],
            catchup = False) as dag:

            task1 = PythonOperator(
                task_id = "get_books",
                python_callable = GetBooks.run_process,
                provide_context = True,
                owner = 'SG'
            )

            task2 = PythonOperator(
                task_id = "insert_data_into_db",
                python_callable = InsertDataDB.run_process,
                provide_context = True,
                owner = 'SG'
            )

task1 > task2