import pickle
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine


def predict_churn(**context):
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    base_path = context.get('project_path')
    data_path = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks', 
        'monthly_data_csv', 
        f"ML_DATA_{year}_{month}.csv"
    )
    new_data = pd.read_csv(data_path)
    new_data = new_data[new_data['Total_Ct_Chng_Q4_Q1']>0]

    pickle_file_path = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks', 
        'ml_pipeline.pkl'
    )

    with open(pickle_file_path, 'rb') as f:
        pipeline_bundle = pickle.load(f)

    encoder = pipeline_bundle["encoder"]
    pipeline = pipeline_bundle["pipeline"]
    
    new_data['Income_Category'] = encoder.transform(new_data['Income_Category'])

    predictions = pipeline.predict(new_data)
    new_data['Predictions'] = predictions
    churn_clients = new_data[new_data['Predictions'] == 0]
    churn_clients['client_id'] = churn_clients.index + 1
    churn_clients['date'] = execution_date.strftime('%Y-%m-%d')
    
    churn_clients = churn_clients[['client_id', 'date']]

    engine = create_engine('mysql+pymysql://root:admin@db:3306/credit_card_db?connect_timeout=60')
    churn_clients.to_sql('churn_clients', engine, if_exists='append', index=False)

   