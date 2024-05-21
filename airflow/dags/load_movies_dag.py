import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

BASE_URL = 'https://github.com/chrisdamba/adaptive-ads/raw/main/dbt/seeds/imdb_movie_dataset'
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'imdb_dataset')

GENRES = [
    'action', 'adventure', 'animation', 'biography', 'comedy', 'crime',
    'documentary', 'drama', 'family', 'fantasy', 'film-noir', 'history',
    'horror', 'music', 'musical', 'mystery', 'news', 'romance', 'scifi',
    'short', 'sports', 'thriller', 'war', 'western'
]


def convert_to_parquet(csv_file, parquet_file):
    if not csv_file.endswith('csv'):
        raise ValueError('The input file is not in csv format')

    table = pv.read_csv(csv_file)
    pq.write_table(table, parquet_file)


def upload_to_gcs(file_path, bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)


def check_file_exists(url):
    import requests
    response = requests.head(url)
    return response.status_code == 200


with DAG(
        dag_id='load_imdb_movie_datasets',
        default_args=default_args,
        description='Load IMDb movie datasets to GCS as Parquet files',
        schedule_interval="@once",
        start_date=datetime(2024, 5, 21),
        catchup=False,
        tags=['imdb', 'movies', 'parquet']
) as dag:
    for genre in GENRES:
        csv_filename = f'{genre}.csv'
        parquet_filename = csv_filename.replace('csv', 'parquet')

        csv_outfile = f'{AIRFLOW_HOME}/{csv_filename}'
        parquet_outfile = f'{AIRFLOW_HOME}/{parquet_filename}'
        table_name = genre
        file_url = f'{BASE_URL}/{csv_filename}'

        check_file_task = ShortCircuitOperator(
            task_id=f'check_{genre}_file_exists',
            python_callable=check_file_exists,
            op_kwargs={'url': file_url}
        )

        download_task = BashOperator(
            task_id=f'download_{genre}_file',
            bash_command=f"curl -sSLf {file_url} > {csv_outfile}"
        )

        convert_task = PythonOperator(
            task_id=f'convert_{genre}_to_parquet',
            python_callable=convert_to_parquet,
            op_kwargs={
                'csv_file': csv_outfile,
                'parquet_file': parquet_outfile
            }
        )

        upload_task = PythonOperator(
            task_id=f'upload_{genre}_to_gcs',
            python_callable=upload_to_gcs,
            op_kwargs={
                'file_path': parquet_outfile,
                'bucket_name': GCP_GCS_BUCKET,
                'blob_name': f'{table_name}/{parquet_filename}'
            }
        )

        remove_task = BashOperator(
            task_id=f'remove_{genre}_files_from_local',
            bash_command=f'rm {csv_outfile} {parquet_outfile}'
        )

        create_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'create_{genre}_external_table',
            table_resource={
                'tableReference': {
                    'projectId': GCP_PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': table_name,
                },
                'externalDataConfiguration': {
                    'sourceFormat': 'PARQUET',
                    'sourceUris': [f'gs://{GCP_GCS_BUCKET}/{table_name}/*.parquet'],
                },
            }
        )

        check_file_task >> download_task >> convert_task >> upload_task >> remove_task >> create_external_table_task
