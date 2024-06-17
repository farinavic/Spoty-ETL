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
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Ruta del DAG
dag_path = os.getcwd()
config_file_path= dag_path+'/keys/'+'config.json'

def read_config(config_file_path):
    with open(config_file_path, 'r') as file:
        config = json.load(file)
        CLIENT_ID_SP = config["CLIENT_ID_SP"]
        CLIENT_SECRET_SP = config["CLIENT_SECRET_SP"]
        DW_USER = config["DW_USER"]
        DW_PW = config["DW_PW"]
        SENDGRID_API_KEY = config["SENDGRID_API_KEY"]
        EMAIL_TO = config["EMAIL_TO"]
        EMAIL_FROM = config["EMAIL_FROM"]
        
    return CLIENT_ID_SP, CLIENT_SECRET_SP, DW_USER, DW_PW, SENDGRID_API_KEY, EMAIL_TO, EMAIL_FROM

CLIENT_ID_API, CLIENT_SECRET_API, DW_USER, DW_PW, SENDGRID_API_KEY, EMAIL_TO, EMAIL_FROM = read_config(config_file_path)


# Conexión al Datawarehouse
redshift_conn = {
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'username': DW_USER,
    'database': 'data-engineer-database',
    'port': '5439',
    'pwd': DW_PW
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
    auth_string = CLIENT_ID_API + ':' + CLIENT_SECRET_API
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
def extract_data(exec_date):
    artist_names = ['Wos', 'Trueno', 'Nathy Peluso']
    token = get_token()
    total_songs = {'results': []}
    for artist in artist_names:
        artist_result = search_artist(token, artist)
        artist_id = artist_result['id']
        artist_songs = get_top_songs_by_artist(token, artist_id)
        total_songs['results'].append(artist_songs)
    
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path + '/raw_data/' + "data_" + date.strftime('%Y-%m-%d-%H') + ".json", "w") as json_file:
        json.dump(total_songs, json_file)

# Función de transformación
def transform_data(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path + '/raw_data/' + "data_" + date.strftime('%Y-%m-%d-%H') + ".json", "r") as json_file:
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
    df.to_csv(dag_path + '/processed_data/' + "data_" + date.strftime('%Y-%m-%d-%H') + ".csv", index=False)

# Función de conexión al Datawarehouse
def conexion_dw(exec_date):
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
def load_data(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records = pd.read_csv(dag_path + '/processed_data/' + "data_" + date.strftime('%Y-%m-%d-%H') + ".csv")
    
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

# Función para enviar el correo si hay cambios en los datos
def send_email_if_update(exec_date):
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    current_filename = f"{dag_path}/raw_data/data_{date.strftime('%Y-%m-%d-%H')}.json"
    compare_file=f"{dag_path}/raw_data/compare.json"
    
    # Load the current data
    with open(current_filename, 'r') as current_file:
        current_data = json.load(current_file)
    
    # Load the previous data from compare.json
    with open(compare_file, 'r') as previous_file:
        previous_data = json.load(previous_file)

    # Compare current data with previous data
    changes = []
    current_songs = {(track['id'], track['popularity']) for artist in current_data['results'] for track in artist['tracks']}
    previous_songs = {(track['id'], track['popularity']) for artist in previous_data['results'] for track in artist['tracks']}
    
    # Check for new or changed songs
    new_or_changed_songs = current_songs - previous_songs
    
    for artist in current_data['results']:
        for track in artist['tracks']:
            if (track['id'], track['popularity']) in new_or_changed_songs:
                changes.append(f"Song: {track['name']}, Popularity: {track['popularity']}, Artist: {track['artists'][0]['name']}")

    if changes:
        # Send an email with changes
        message = Mail(
            from_email=EMAIL_FROM,
            to_emails=EMAIL_TO,
            subject='Spotify Top Songs Updates',
            html_content='<br>'.join(changes)
        )
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(response.status_code)
    #print(current_data+ previous_data + 'PRINTPRINT')

    # Update the compare.json file with the current data
    with open(compare_file, 'w') as compare_file:
        json.dump(current_data, compare_file)



# Definición de tareas en el DAG
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

dw_connection_task = PythonOperator(
    task_id="dw_connection",
    python_callable=conexion_dw,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=spotify_dag,
)

send_email_if_update_task = PythonOperator(
    task_id='send_email_if_update',
    python_callable=send_email_if_update,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=spotify_dag
)

# Definición del orden de tareas
extract_data_task >> transform_data_task >> dw_connection_task >> load_data_task
extract_data_task >> transform_data_task >> send_email_if_update_task
