import datetime as dt
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import psycopg2
import os

# Fungsi untuk mengambil data dari PostgreSQL
def fetch_data_from_postgresql():
    # Koneksi ke PostgreSQL menggunakan PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Mengambil data dari table_m3
    query = "SELECT * FROM table_m3"
    df = pg_hook.get_pandas_df(query)
    
    # Simpan data mentah ke CSV
    raw_data_path = 'C:/Users/Lenovo/Documents/codingan/project_m3/P2M3_fahmi_data_raw.csv'
    df.to_csv(raw_data_path, index=False)

# Fungsi untuk membersihkan data
def clean_data():
    # Membaca data dari CSV
    raw_data_path = 'C:/Users/Lenovo/Documents/codingan/project_m3/P2M3_fahmi_data_raw.csv'
    df = pd.read_csv(raw_data_path)
    
    # Menghapus duplikat
    df.drop_duplicates(inplace=True)
    
    # Normalisasi nama kolom
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
    
    # Mengatasi missing values, misalnya dengan mengisi nilai yang hilang dengan median
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col].fillna(df[col].median(), inplace=True)
    
    # Simpan data yang sudah dibersihkan ke CSV
    clean_data_path = 'C:/Users/Lenovo/Documents/codingan/project_m3/P2M3_fahmi_data_clean.csv'
    df.to_csv(clean_data_path, index=False)

# Definisi default_args
default_args = {
    'owner': 'Fahmi',
    'start_date': datetime(2024, 9, 11, 7, 28, 0) - timedelta(hours=7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definisi DAG
with DAG(
    "H8_eda_pipeline",
    description='H8 EDA pipeline',
    schedule_interval='30 6 * * *',
    default_args=default_args, 
    catchup=False
) as dag:

    # Task untuk mengambil data dari PostgreSQL
    fetch_task = PythonOperator(
        task_id='fetch_data_from_postgresql',
        python_callable=fetch_data_from_postgresql
    )

    # Task untuk melakukan data cleaning
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    # Dependencies antar task
    fetch_task >> clean_task
