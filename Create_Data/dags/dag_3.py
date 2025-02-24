from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from monthly_tasks.download_monthly_data import download_monthly_data
from monthly_tasks.predict_churn import predict_churn


PROJECT_PATH = "/opt/airflow"

with DAG("dag_3", start_date =datetime(2025, 1, 19), 
        schedule_interval= "15 0 L * *", catchup=False) as dag:


        
        create_view_1 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_1", 
                        sql='monthly_tasks/sql_scripts/view_1.sql'
                )
        
        create_view_2 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_2", 
                        sql='monthly_tasks/sql_scripts/view_2.sql'
                )
        create_view_3 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_3", 
                        sql='monthly_tasks/sql_scripts/view_3.sql'
                )
        
        create_view_4 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_4", 
                        sql='monthly_tasks/sql_scripts/view_4.sql'
                )
        
        create_view_5 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_5", 
                        sql='monthly_tasks/sql_scripts/view_5.sql'
                )
        
        create_view_6 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_6", 
                        sql='monthly_tasks/sql_scripts/view_6.sql'
                )
        
        create_view_7 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_7", 
                        sql='monthly_tasks/sql_scripts/view_7.sql'
                )
        create_view_8 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_8", 
                        sql='monthly_tasks/sql_scripts/view_8.sql'
                )
        create_view_9 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_9", 
                        sql='monthly_tasks/sql_scripts/view_9.sql'
                )
        create_view_10 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_10", 
                        sql='monthly_tasks/sql_scripts/view_10.sql'
                )
        create_view_11 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_11", 
                        sql='monthly_tasks/sql_scripts/view_11.sql'
                )
        create_view_12 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_12", 
                        sql='monthly_tasks/sql_scripts/view_12.sql'
                )
        create_view_13 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_13", 
                        sql='monthly_tasks/sql_scripts/view_13.sql'
                )
        create_view_14 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_14", 
                        sql='monthly_tasks/sql_scripts/view_14.sql'
                )
        create_view_15 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_view_15", 
                        sql='monthly_tasks/sql_scripts/view_15.sql'
                )

        create_procedure_1 = MySqlOperator(
                        mysql_conn_id = "mysql_conn",
                        task_id = "create_procedure_1", 
                        sql='monthly_tasks/sql_scripts/procedure_1.sql'
                )
        
        create_procedure_2 = MySqlOperator(
                         mysql_conn_id = "mysql_conn",
                         task_id = "create_procedure_2", 
                         sql='monthly_tasks/sql_scripts/procedure_2.sql'
                 )
    
        download_monthly_data = PythonOperator(
                task_id="download_monthly_data",
                python_callable=download_monthly_data, 
                provide_context=True,
                op_kwargs={'project_path': PROJECT_PATH})

        predict_churn = PythonOperator(
                task_id="predict_churn",
                python_callable=predict_churn, 
                provide_context=True,
                op_kwargs={'project_path': PROJECT_PATH}

        )

create_view_1 >> create_view_2 >> create_view_3 >> create_view_4 >> create_view_5 >> create_view_6 >> create_view_7 >> create_view_8 >> create_view_9 >> create_view_10 >> create_view_11 >> create_view_12 >> create_view_13 >> create_view_14 >> create_view_15  >> create_procedure_1 >> create_procedure_2 >> download_monthly_data >> predict_churn