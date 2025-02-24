import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

def download_monthly_data(**context):


    execution_date = context['execution_date']
    base_path = context.get('project_path')

    query = text(
    """WITH open_credit AS 
            (SELECT 
                car.client_id, 
                ROUND(AVG(o.open_credit), 0) AS Avg_Open_To_Buy 
            FROM 
                cards AS car
            LEFT JOIN 
                open_credit_per_month as o
            ON 
                car.card_id = o.card_id
            GROUP BY 
                car.client_id) 
        SELECT 
            j3.Total_Trans_Ct,
            j3.Total_Trans_Amt,
            j2.balance AS Total_Revolving_Bal, 
            j3.total_ct_chng_q4_q1 AS Total_Ct_Chng_Q4_Q1, 
            j2.credit_limit AS Credit_Limit,
            j2.total_relationship_count AS Total_Relationship_Count,
            j1.income_category AS Income_Category, 
            j2.month_inactive AS Months_Inactive_12_mon 
            
        FROM 
            join_1 AS j1
        INNER JOIN 
            join_2 AS j2 ON j1.client_id = j2.client_id 
        INNER JOIN 
            join_3 AS j3 ON j1.client_id = j3.client_id
        INNER JOIN open_credit AS op ON j1.client_id = op.client_id""")

    engine = create_engine('mysql+pymysql://root:admin@db:3306/credit_card_db?connect_timeout=60')
    df = pd.read_sql_query(query, engine)
   
    year = execution_date.year
    month = execution_date.month

    output_file = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks', 
        'monthly_data_csv', 
        f"ML_DATA_{year}_{month}.csv"
    )

    return df.to_csv(output_file, index=False)