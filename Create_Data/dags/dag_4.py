from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from monthly_tasks.create_monthly_report import create_monthly_report


PROJECT_PATH = "/opt/airflow"

with DAG("dag_4", start_date =datetime(2025, 1, 19), 
        schedule_interval= "30 0 L * *", catchup=False) as dag:

        create_monthly_report = PythonOperator(
                task_id="create_monthly_reports", 
                python_callable=create_monthly_report,
                provide_context=True,
                op_kwargs={'project_path':PROJECT_PATH})
        



