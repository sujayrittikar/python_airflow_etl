from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from anime_etl import export_data

default_args = {
    'owner': 'Sujay Rittikar',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 5),
    'email': ['28sujrit03@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('anime_dag', default_args=default_args, description='ETL for top 50 Animes')

run_etl = PythonOperator(task_id='complete_anime_etl', python_callable=export_data, dag=dag)

run_etl