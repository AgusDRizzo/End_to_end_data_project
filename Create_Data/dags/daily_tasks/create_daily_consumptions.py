import pandas as pd
from datetime import datetime
import os
from daily_tasks.MOCK_names import business_names
import random



def create_daily_consumptions(**context):

    base_path = context.get('project_path')
    execution_date = context['execution_date']
    
    cards_file = os.path.join(base_path, 'CSV_Files', 'MOCK_DATA_CARDS.csv')
    clients_file = os.path.join(base_path, 'CSV_Files', 'MOCK_DATA_CLIENTS.csv')
    df_cards = pd.read_csv(cards_file)
    df_clients = pd.read_csv(clients_file)
    max_limit = df_cards["credit_limit"].to_list()
    total_clients = len(df_clients)+1
    cards_list = df_cards.index.to_list() 
    data = []

    for i in range(50):
        card_id = random.choice(cards_list)+1
        max_consumption = int((max_limit[card_id-1]) * 0.3)
        consumption_amount = random.randint(1, max_consumption) 
        consumption_date = execution_date
        business_name = random.choice(business_names)
        data.append({
                            'card_id': card_id,
                            'consumption_amount': consumption_amount,
                            'consumption_date': consumption_date.strftime('%Y-%m-%d'),
                            'business_name': business_name
                        })
    df_consumptions = pd.DataFrame(data)
    output_file = os.path.join(
        base_path, 
        'dags', 
        'daily_tasks', 
        'daily_CSV', 
        f"MOCK_DATA_CONSUMPTIONS_{consumption_date.strftime('%Y-%m-%d')}.csv"
    )
    return df_consumptions.to_csv(output_file, index=False)
   
