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

    # Общие аргументы Spark
    spark_conf = [
        "--master",
        "spark://217.61.58.159:7077",
        "--deploy-mode",
        "client",
        "--conf",
        "spark.executor.memory=2g",
        "--conf",
        "spark.driver.memory=2g",
        "--conf",
        "spark.executor.cores=2",
    ]

    # Етап 1: landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="dags/landing_to_bronze.py",
        name="landing_to_bronze_job",
        application_args=spark_conf,
        verbose=True,
    )

    # Етап 2: bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="dags/bronze_to_silver.py",
        name="bronze_to_silver_job",
        application_args=spark_conf,
        verbose=True,
    )

    # Етап 3: silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="dags/silver_to_gold.py",
        name="silver_to_gold_job",
        application_args=spark_conf,
        verbose=True,
    )

    # Задачи выполняются последовательно
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
