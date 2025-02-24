from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta
from monthly_tasks.create_monthly_payment import create_monthly_payments
from monthly_tasks.load_monthly_data import wait_for_db, load_monthly_data

PROJECT_PATH = "/opt/airflow"

with DAG("dag_2", start_date =datetime(2025, 1, 19), 
        schedule_interval= "5 0 L * *", catchup=False) as dag:

        create_monthly_payments = PythonOperator(
                task_id="create_monthly_payments", 
                python_callable=create_monthly_payments,
                provide_context=True,
                op_kwargs={'project_path':PROJECT_PATH})
        
        wait_for_db = PythonOperator(
                task_id="wait_for_db", 
                python_callable = wait_for_db
            )
        
        load_monthly_data = PythonOperator(
                task_id = "load_monthly_data",
                python_callable=load_monthly_data,
                provide_context=True,
                op_kwargs={'project_path': PROJECT_PATH},
                retries=5,  # Retry 5 times
                retry_delay=timedelta(seconds=30),  # Wait 30 seconds between retries

        )
        




        create_monthly_payments >> wait_for_db >> load_monthly_data  