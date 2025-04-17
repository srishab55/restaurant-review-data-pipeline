from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
#from src.transformation.transform_restaurants import main
import sys,os
#from src.load.load_to_warehouse import load_parquet_to_warehouse
import subprocess
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with DAG(
    dag_id='restaurant_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL for restaurant data from JSON to warehouse',
) as dag:

    # def convert_json_to_parquet():
    #     subprocess.run(["python", "adhoc_scripts/json_chunks_to_parquet.py"], check=True)

    # convert_task = PythonOperator(
    #     task_id='convert_json_to_parquet',
    #     python_callable=convert_json_to_parquet,
    # )

    # transform_task = PythonOperator(
    #     task_id='transform_restaurant_data',
    #     python_callable=main,
    # )

    # load_task = PythonOperator(
    #     task_id='load_cleaned_data',
    #     python_callable=load_parquet_to_warehouse,
    # )

    transform_task 