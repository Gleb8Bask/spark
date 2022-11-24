from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='spark_test2',
    catchup = False, 
    schedule_interval='@daily', 
    start_date=datetime(2021, 12, 1), 
    dagrun_timeout=timedelta(minutes=60)
) as dag:

#Скачать
    download = BashOperator(
        task_id='download',
        bash_command='cd /airflow/data/2021-11-26; wget http://89.208.196.213/events/2021-11-26'
    )
#{{ ds }}

#Из CSV в паркет
#    from_csv_to_parquet = SparkSubmitOperator(
#        task_id='csv_to_parquet',
#        conn_id='spark_local',
#        application=f'/opt/airflow/dags/spark_scripts/csv_to_parquet.py',
#        name='spark_csv_to_parquet_app',
#        execution_timeout=timedelta(minutes=2)
#    )

#Преобразовать из Паркета и в Базу
#    transform = SparkSubmitOperator(
#        task_id='transform',
#        conn_id='spark_local',
#        application=f'/opt/airflow/dags/spark_scripts/transform.py',
#        name='spark_transform_app',
#        execution_timeout=timedelta(minutes=2)
#    )

#Из Паркета в Базу
#    from_parquet_to_postgres = SparkSubmitOperator(
#        task_id='parquet_to_postgres',
#        conn_id='postgres_local',
#        application=f'/opt/airflow/dags/spark_scripts/parquet_to_postgres.py',
#        name='spark_parquet_to_postgres_app',
#        execution_timeout=timedelta(minutes=2),
#        packages='org.postgresql:postgresql:42.2.24'
#    )

#Тестирование Базы
#    test_postgres_connection_task = SparkSubmitOperator(
#        task_id='test_postgres_connection',
#        conn_id='postgres_local',
#        application=f'/opt/airflow/dags/spark_scripts/test_postgres_connection.py',
#        name='test_postgres_connection_app',
#        execution_timeout=timedelta(minutes=2),
#        packages='org.postgresql:postgresql:42.2.24'
#    )

    download 
    #>> from_csv_to_parquet
    #>> transform
    #>> from_parquet_to_postgres
    #>> test_postgres_connection_task