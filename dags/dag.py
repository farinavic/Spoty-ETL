from datetime import timedelta, datetime
import os
import base64
import requests as r
import json
import pandas as pd
import sqlalchemy as sqla

import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago



# Ruta del DAG
dag_path = os.getcwd()

#Cargar credenciales de la API de Spotify
with open(dag_path+'/keys/'+"CLIENT_ID_SP.txt",'r') as f:
    client_id=f.read()
with open(dag_path+'/keys/'+"CLIENT_SECRET_SP.txt",'r') as f:
    client_secret=f.read()
with open(dag_path+'/keys/'+"USER_RS.txt",'r') as f:
    rs_user=f.read()
with open(dag_path+'/keys/'+"PW_RS.txt",'r') as f:
   rs_password= f.read()


# Credenciales de conexión a Redshift
redshift_conn = {
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'username': rs_user,
    'database': 'data-engineer-database',
    'port': '5439',
    'pwd': rs_password
}

# Argumentos para el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 30),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

spotify_dag = DAG(
    dag_id='Spotify-ETL',
    default_args=default_args,
    description='Carga de top 10 de artistas seleccionados de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

# Funciones de ETL

def get_token():

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

# Función de extracción
def extraer_data(exec_date):
    artist_names = ['Wos', 'Trueno', 'Nathy Peluso']
    token = get_token()
    total_songs = {'results': []}
    for artist in artist_names:
        artist_result = search_artist(token, artist)
        artist_id = artist_result['id']
        artist_songs = get_top_songs_by_artist(token, artist_id)
        total_songs['results'].append(artist_songs)
    
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
        json.dump(total_songs, json_file)

# Función de transformación
def transformar_data(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data = json.load(json_file)

    artist_names = ['Wos', 'Trueno', 'Nathy Peluso']
    now = datetime.today().strftime('%Y-%m-%d')
    data = {
        'song_PK': [], 'song_id': [], 'song_singer': [], 'song_name': [], 'song_album': [],
        'song_duration': [], 'song_popularity': [], 'song_release_date': [], 'record_date': []
    }

    i = 0
    for artist_result in loaded_data['results']:
        singer = artist_names[i]
        for track in artist_result['tracks']:
            song_pk = track['id'] + '|' + str(track['popularity'])
            id = track['id']
            name = track['name']
            album = track['album']['name']
            duration = track['duration_ms']
            popularity = track['popularity']
            release_date = track['album']['release_date']
            duration = str(timedelta(milliseconds=duration))[2:].split(".")[0]

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
    df.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False)

# Función de conexión a Redshift
def conexion_redshift(exec_date):
    print(f"Conectándose a la BD en la fecha: {exec_date}")
    try:
        conn = psycopg2.connect(
            host=redshift_conn["host"],
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port=redshift_conn["port"])
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

# Función de carga de datos
def cargar_data(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records = pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    
    conn = psycopg2.connect(
        host=redshift_conn["host"],
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port=redshift_conn["port"])

    cur = conn.cursor()
    columns = [
        'song_PK', 'song_id', 'song_singer', 'song_name', 'song_album',
        'song_duration', 'song_popularity', 'song_release_date', 'record_date'
    ]
    values = [tuple(x) for x in records.to_numpy()]
    insert_sql = f"INSERT INTO top10 ({', '.join(columns)}) VALUES %s"
    cur.execute("BEGIN")
    psycopg2.extras.execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")

# Definición de tareas en el DAG
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

task_31 = PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag
)

task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

# Definición del orden de tareas
task_1 >> task_2 >> task_31 >> task_32
