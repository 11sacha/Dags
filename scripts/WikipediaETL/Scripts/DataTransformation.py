import pandas as pd
import json
from geopy import Nominatim

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

current_date = datetime.now()
date = current_date.day
month = current_date.month

FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()

def  get_coordinates(country, city):
    geolocator = Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

def run_process(**kwargs):
    data = kwargs['ti'].xcom_pull(key=rows, task_ids='extraction')

    data = json.loads(data)

    stadium_df = pd.DataFrame(data)
    stadium_df['location'] = stadium_df.apply(lambda x: get_coordinates(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "Data transformation finished."