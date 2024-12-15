from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('batch_pipeline_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Запуск landing_to_bronze (Етап 1 Ч.2)
    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        application='dags/landing_to_bronze.py',
        conn_id='spark_default',
        verbose=1
    )

    # Запуск bronze_to_silver (Етап 2 Ч.2)
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='dags/bronze_to_silver.py',
        conn_id='spark_default',
        verbose=1
    )

    # Запуск silver_to_gold (Етап 3 Ч.2)
    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='dags/silver_to_gold.py',
        conn_id='spark_default',
        verbose=1
    )

    # Послідовність виконання: landing_to_bronze -> bronze_to_silver -> silver_to_gold
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
