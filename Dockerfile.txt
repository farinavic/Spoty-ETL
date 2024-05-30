# filename: Dockerfile
FROM python:3.8-slim

# Instalar dependencias
RUN pip install apache-airflow==2.5.1 pandas python-dotenv requests psycopg2-binary SQLAlchemy==1.4.38

# Crear directorios para Airflow
RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/logs /usr/local/airflow/plugins

# Copiar los DAGs y el archivo .env a la carpeta de Airflow
COPY dags /usr/local/airflow/dags
COPY .env /usr/local/airflow/

# Establecer variables de entorno necesarias
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Inicializar Airflow y crear un usuario admin
RUN airflow db init && \
    airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.com

# Comando para iniciar el webserver
CMD ["airflow", "webserver"]
