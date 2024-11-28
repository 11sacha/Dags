from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.twitter_DataPipeline import twitter_etl

with DAG(   dag_id = 'TDP',
            start_date = datetime(2021, 1,1),
            schedule = '0 10 * * 1',
            tags=['Twitter', 'DataPipeline'],
            catchup = False) as dag:

            task1 = PythonOperator(
                task_id = "twitter_data_pipeline",
                python_callable = twitter_etl.run_process,
                provide_context = True,
                owner = 'SG'
            )