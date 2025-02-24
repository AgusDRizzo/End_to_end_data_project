from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
from daily_tasks.create_daily_consumptions import create_daily_consumptions
from daily_tasks.load_daily_data import load_daily_data, wait_for_db




PROJECT_PATH = Variable.get("PROJECT_PATH")

with DAG("dag_1", start_date =datetime(2025, 1, 1), 
        schedule_interval= "5 0 1-20 * *", catchup=False) as dag:

            create_daily_consumptions = PythonOperator(
                    task_id="create_daily_consumptions",
                    python_callable=create_daily_consumptions, 
                    provide_context=True,
                    op_kwargs={'project_path': PROJECT_PATH})
            
            wait_for_db = PythonOperator(
                    task_id="wait_for_db", 
                    python_callable = wait_for_db
            )

           
            load_daily_data = PythonOperator(
                    task_id = "load_daily_data",
                    python_callable=load_daily_data,
                    provide_context=True,
                    op_kwargs={'project_path': PROJECT_PATH},
                    retries=5,  # Retry 5 times
                    retry_delay=timedelta(seconds=30),  # Wait 30 seconds between retries
            )

            validate_daily_consumptions = MySqlOperator(
                    mysql_conn_id = "mysql_conn",
                    task_id = "validate_daily_consumptions", 
                    sql='daily_tasks/validate_daily_consumption.sql'
            )

            clean_daily_data = MySqlOperator(
                    mysql_conn_id = "mysql_conn",
                    task_id = "clean_daily_data", 
                    sql='daily_tasks/clean_daily_data.sql'
            )
            



create_daily_consumptions >> wait_for_db >> load_daily_data >> validate_daily_consumptions >> clean_daily_data