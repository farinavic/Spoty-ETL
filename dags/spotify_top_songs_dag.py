# filename: dags/spotify_top_songs_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import dotenv as env
import requests as r
import os
import base64
import json
import datetime
import pandas as pd

# Cargar variables de entorno desde el archivo .env
env.load_dotenv(dotenv_path='/usr/local/airflow/.env')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'spotify_top_songs',
    default_args=default_args,
    description='DAG para extraer top canciones de Spotify y cargarlas en Redshift',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

def get_token():
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    auth_string = client_id + ':' + client_secret
    auth_bytes = auth_string.encode('utf-8')
    auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')

    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + auth_base64,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {'grant_type': 'client_credentials'}

    result = r.post(url, headers=headers, data=data)
    result.raise_for_status()
    json_result = result.json()
    token = json_result['access_token']
    return token

def get_auth_header(token):
    return {'Authorization': 'Bearer ' + token}

def search_artist(token, artist_name):
    url = 'https://api.spotify.com/v1/search'
    headers = get_auth_header(token)
    query = f'?q={artist_name}&type=artist&limit=1'
    query_url = url + query
    result = r.get(query_url, headers=headers)
    json_result = json.loads(result.content)['artists']['items']

    if len(json_result) == 0:
        print('Artist not found')
        return None

    return json_result[0]

def get_top_songs_by_artist(token, artist_id):
    url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=AR'
    headers = get_auth_header(token)
    result = r.get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def extract_data():
    artist_names = ['Wos', 'Trueno', 'Nathy Peluso']
    token = get_token()
    total_songs = {'results': []}

    for artist in artist_names:
        artist_result = search_artist(token, artist)
        artist_id = artist_result['id']
        artist_songs = get_top_songs_by_artist(token, artist_id)
        total_songs['results'].append(artist_songs)

    now = datetime.date.today().strftime('%Y-%m-%d')

    data = {'song_PK': [], 'song_id': [], 'song_singer': [], 'song_name': [], 'song_album': [], 'song_duration': [], 'song_popularity': [], 'song_release_date': [], 'record_date': []}
    
    i = 0
    for artist_result in total_songs['results']:
        singer = artist_names[i]
        for track in artist_result['tracks']:
            song_pk = (track['id'] + '|' + str(track['popularity']))
            id = track['id']
            name = track['name']
            album = track['album']['name']
            duration = track['duration_ms']
            popularity = track['popularity']
            release_date = track['album']['release_date']

            name = name.strip('(),')
            name = name.replace("'",'')
            album = album.strip('(),')
            album = album.replace("'",'')
            duration = str(datetime.timedelta(milliseconds=duration))[2:].split(".")[0]

            data['song_PK'].append(song_pk)
            data['song_id'].append(id)
            data['song_singer'].append(singer)
            data['song_name'].append(name)
            data['song_album'].append(album)
            data['song_duration'].append(duration)
            data['song_popularity'].append(popularity)
            data['song_release_date'].append(release_date)
            data['record_date'].append(now)
        i += 1

    df = pd.DataFrame(data)
    df.to_csv('/tmp/spotify_top_songs.csv', index=False)

def load_data():
    rs_user = os.getenv('RS_USER')
    rs_password = os.getenv('RS_PASSWORD')

    engine = create_engine(f'postgresql://{rs_user}:{rs_password}@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')
    conn = engine.connect()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS top10
    (
        song_pk VARCHAR(255) PRIMARY KEY,
        song_id VARCHAR(255),
        song_singer VARCHAR(255),
        song_name VARCHAR(255),
        song_album VARCHAR(255),
        song_duration TIME,
        song_popularity INT,
        song_release_date DATE,
        record_date DATE
    )
    """
    conn.execute(create_table_query)

    df = pd.read_csv('/tmp/spotify_top_songs.csv')
    df.to_sql('top10', conn, index=False, if_exists='append')

    remove_dups_query = """
    DELETE FROM top10
    WHERE song_pk IN (
        SELECT song_pk
        FROM top10
        GROUP BY song_pk
        HAVING COUNT(*) > 1
    )
    AND record_date NOT IN (
        SELECT MIN(record_date)
        FROM top10 AS t2
        WHERE t2.song_pk = top10.song_pk
        GROUP BY song_pk
    );
    """
    conn.execute(remove_dups_query)

    remove_anomalies_query = """
    DELETE FROM top10
    WHERE song_name IS NULL
       OR song_album IS NULL
       OR song_duration = '00:00'
       OR song_popularity < 1
       OR song_release_date < '1900-01-01';
    """
    conn.execute(remove_anomalies_query)

with dag:
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_data_task >> load_data_task
