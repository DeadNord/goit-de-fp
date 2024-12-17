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

    # Spark Master для Standalone режима
    spark_master = "spark://217.61.58.159:7077"

    # Задача: landing_to_bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="/opt/airflow/dags/landing_to_bronze.py",
        conn_id=None,
        name="landing_to_bronze_job",
        verbose=True,
        conf={
            "spark.master": spark_master,  # Указываем master здесь
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
        },
    )

    # Задача: bronze_to_silver
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/dags/bronze_to_silver.py",
        conn_id=None,
        name="bronze_to_silver_job",
        verbose=True,
        conf={
            "spark.master": spark_master,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
        },
    )

    # Задача: silver_to_gold
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/dags/silver_to_gold.py",
        conn_id=None,
        name="silver_to_gold_job",
        verbose=True,
        conf={
            "spark.master": spark_master,
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.executor.cores": "2",
        },
    )

    # Последовательность задач
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
