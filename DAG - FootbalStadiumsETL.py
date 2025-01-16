from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.WikipediaETL.Scripts import DataExtraction

with DAG(   dag_id = 'FSP',
            start_date = datetime(2021, 1,1),
            schedule = '0 10 * * 1',
            tags=['Amazon', 'ETL'],
            catchup = False) as dag:

            extraction = PythonOperator(
                task_id = "get_data",
                python_callable = DataExtraction.run_process,
                provide_context = True,
                op_kwargs={"url" : "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
                owner = 'SG'
            )

extraction