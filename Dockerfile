# Usamos la imagen oficial de Python como base
FROM python:3.9-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalamos Airflow y las dependencias necesarias
RUN pip install apache-airflow==2.2.2 \
    && pip install python-dotenv requests psycopg2-binary SQLAlchemy==1.4.38

# Copiamos los archivos de tu proyecto al contenedor
COPY . /app
# Copiamos el archivo del DAG a la carpeta de dags de Airflow
#COPY dag.py /app/airflow/dags/
COPY dag.py /opt/airflow/dags/

# Configuramos las variables de entorno necesarias para Airflow
ENV AIRFLOW_HOME=/app/airflow

# Inicializamos la base de datos de Airflow y creamos el usuario administrador
RUN airflow db init \
    && airflow users create \
        --username airflow \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email airflowadmin@example.com \
        --password airflow



# Establecemos el comando predeterminado para iniciar el webserver de Airflow
CMD ["airflow", "webserver", "--port", "8080"]
