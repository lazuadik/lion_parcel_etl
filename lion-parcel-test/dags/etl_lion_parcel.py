from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'andhika_rafi',
    'start_date': datetime(2026, 1, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path untuk temporary file
TEMP_DIR = '/tmp/airflow_etl'
TEMP_FILE = f'{TEMP_DIR}/retail_data.parquet'

def extract_from_postgres(**context):
    # Extract data dari PostgreSQL
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_source_conn')
    
    query = """
        SELECT * FROM retail_transaction 
    """
    
    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        print("No data extracted")
        # Tetap save file kosong
        df.to_parquet(TEMP_FILE, index=False)
        return 0
    
    # Save ke parquet file 
    df.to_parquet(TEMP_FILE, index=False)
    print(f"Extracted {len(df)} rows from PostgreSQL")
    return len(df)

def transform_data(**context):
    # Transform data sebelum load
    # Baca dari file
    if not os.path.exists(TEMP_FILE):
        print("No temp file found")
        return 0
    
    df = pd.read_parquet(TEMP_FILE)
    
    if df.empty:
        print("No data to transform")
        return 0
    
    # Handle NaT/None untuk kolom tanggal
    date_cols = ['created_at', 'updated_at', 'deleted_at']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Save hasil transform (overwrite file yang sama)
    df.to_parquet(TEMP_FILE, index=False)
    print(f"Transformed {len(df)} rows")
    return len(df)

def load_to_bigquery(**context):
    # Load data ke BigQuery menggunakan strategi Overwrite 
    # 1. Baca dari file parquet
    if not os.path.exists(TEMP_FILE):
        print("No temp file found")
        return 0
    
    df = pd.read_parquet(TEMP_FILE)
    
    if df.empty:
        print("No data to load")
        return 0
    
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    
    project_id = "lion-parcel-dwh"
    dataset_id = "retail_data"
    final_table = "retail_transaction"
    
    # 2. Kirim data menggunakan to_gbq dengan if_exists='replace'
    df.to_gbq(
        destination_table=f"{dataset_id}.{final_table}",
        project_id=project_id,
        if_exists='replace',
        credentials=bq_hook.get_credentials()
    )
    
    print(f"Successfully loaded {len(df)} rows to BigQuery via Overwrite strategy")
    
    # 3. Cleanup temp file
    if os.path.exists(TEMP_FILE):
        os.remove(TEMP_FILE)
    
    return len(df)

with DAG(
    'etl_lion_parcel_transactions',
    default_args=default_args,
    description='Hourly ETL for Lion Parcel retail transactions with soft delete handling',
    schedule='@hourly',
    catchup=False,
    tags=['lion_parcel', 'etl', 'retail']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_from_postgres,
        provide_context=True
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_to_bigquery,
        provide_context=True
    )
    
    extract_task >> transform_task >> load_task