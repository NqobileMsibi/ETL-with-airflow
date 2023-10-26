from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.postgres import GoogleCloudStorageToPostgresOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_to_postgres',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:
    # Define variables
    gcs_bucket = 'moviedataclean'
    gcs_file = 'user_purchase_clean.csv'
    schema = 'movie'
    table = 'user_purchase'
    delimiter = ','
    quotechar = '"'
    headers = 1
    
    # Task to load the CSV data into PostgreSQL
    load_csv = GoogleCloudStorageToPostgresOperator(
        task_id='load_csv',
        postgres_conn_id='postgres_conn',
        bucket=gcs_bucket,
        filename=gcs_file,
        schema=schema,
        table=table,
        delimiter=delimiter,
        quotechar=quotechar,
        headers=headers,
    )
    
    # Set the task dependencies
    load_csv
