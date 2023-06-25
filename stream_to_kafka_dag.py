from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from stream_to_kafka import start_streaming

start_date = datetime(2018, 12, 21, 12, 12)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('random_people_names', default_args=default_args, schedule_interval='0 1 * * *', catchup=False) as dag:


    data_stream_task = PythonOperator(
    task_id='data_stream',
    python_callable=start_streaming,
    dag=dag,
    )

    data_stream_task