import pandas as pd
import json

current_date = datetime.now()
date = current_date.day
month = current_date.month

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
OutPath = os.path.join(ProjectPath, r'Output')


def run_process(**kwargs):

    data = kwargs['ti'].xcom_pull(key='rows', task_ids=transformation)

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = 'StadiumData_' + date + '_' + month + '.csv'

    data.to_csv(os.path.join(OutPath, file_name))