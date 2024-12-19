from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Аргументы по умолчанию
default_args = {
    "owner": "eod_airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# DAG
with DAG(
    "eod_batch_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Задача: landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="/opt/airflow/dags/landing_to_bronze.py",
        conn_id="spark-default",
        name="landing_to_bronze_job",
        verbose=True,
        conf={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "4",
        },
    )

    # Задача: bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/dags/bronze_to_silver.py",
        conn_id="spark-default",
        name="bronze_to_silver_job",
        verbose=True,
        conf={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "4",
        },
    )

    # Задача: silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/dags/silver_to_gold.py",
        conn_id="spark-default",
        name="silver_to_gold_job",
        verbose=True,
        conf={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "4",
        },
    )

    # Последовательность задач
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
