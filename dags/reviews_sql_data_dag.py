from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys
# Adjust the path to your ingestion script if it's in a different directory
INGESTION_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reviews_ingestion.py"
sys.path.insert(0, str(INGESTION_SCRIPT_PATH.parent))

from src.ingestion.mysql_to_local_dump import run_incremental_job

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="reviews_ingestion_dag",
    default_args=default_args,
    description="DAG for incremental ingestion of review data from MySQL to Parquet",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reviews", "ingestion", "mysql"],
) as dag:

    run_ingestion = PythonOperator(
        task_id="run_reviews_incremental_ingestion",
        python_callable=run_incremental_job
    )

    run_ingestion