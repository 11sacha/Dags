from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.RedditPipeline.Scripts import ExtractData, TransformData, LoadingData

with DAG(   dag_id = 'RDT',
            start_date = datetime(2021, 1,1),
            schedule = '0 10 * * 1',
            tags=['Reddit', 'ETL'],
            catchup = False) as dag:

            extract = PythonOperator(
                task_id = "extract",
                python_callable = ExtractData.run_process,
                provide_context = True,
                owner = 'SG'
            )

            transform = PythonOperator(
                task_id = "transform_data",
                python_callable = TransformData.run_process,
                provide_context = True,
                owner = 'SG'
            )

            loading_s3_bucket = PythonOperator(
                task_id = "loading_s3_bucket",
                python_callable = LoadingData.run_process,
                provide_context = True,
                owner = 'SG'
            )

extract > transform > loading_s3_bucket
