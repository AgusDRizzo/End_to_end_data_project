import pandas as pd
from datetime import datetime, timedelta
import calendar
import random
import os

def create_monthly_payments(**context):
  
  base_path = context.get('project_path')
  execution_date = context['execution_date']
  year = execution_date.year
  month = execution_date.month

  number_of_days = calendar.monthrange(year, month)
  number_of_days = number_of_days[1]
  
  list_of_df = []
  count = 0
  for i in range(number_of_days):
  
   date_time = execution_date - timedelta(days=count)
   date_time_str = date_time.strftime('%Y-%m-%d')
   consumption_file_path = os.path.join(
        base_path, 
        'dags', 
        'daily_tasks', 
        'daily_csv', 
        f"MOCK_DATA_CONSUMPTIONS_{date_time_str}.csv"
    )
   try:
      df = pd.read_csv(consumption_file_path)
      list_of_df.append(df)
   except:
     continue
   count += 1

  df_concat = pd.concat(list_of_df)
  cards_debtless_path = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks',  
        'cards_debtless.csv'
    )
  cards_debtless = pd.read_csv(cards_debtless_path)
  cards_debtless_list = cards_debtless[cards_debtless.columns[0]].to_list()

  data = []



  cards_with_consumptions = df_concat["card_id"].unique().tolist()
  for card in cards_with_consumptions:
    if card in cards_debtless_list:
      card_id = card
      # El pago de estas tarjetas es igual a la suma de los consumos que hayan realizado en el mes (deuda 0).
      df_filtered = df_concat[df_concat['card_id']==card]
      payment_amount = df_filtered.groupby("card_id")["consumption_amount"].sum().values[0]
      # el pago se realiza entre el ultimo dia del mes y los 8 anteriores.
      start_date = execution_date - timedelta(days=10)
      end_date = execution_date 
      days_between = (end_date - start_date).days
      payment_date = start_date - timedelta(days=random.randint(0, days_between))
      # Se almacenan los datos del pago en un diccionario
      data.append({
                  'card_id': card_id,
                  'payment_amount': payment_amount,
                  'payment_date': payment_date.strftime('%Y-%m-%d'),
              })
    else:
      card_id = card
      # En este caso estas tarjetas pagan solo un porcentaje de la suma de los consumos al azar (deuda positiva al azar).
      random_decimal = random.randint(0, 90)/100
      df_filtered = df_concat[df_concat['card_id']==card]
      payment_amount = (df_filtered.groupby("card_id")["consumption_amount"].sum().values[0])*random_decimal
      payment_amount = int(payment_amount)
      # el pago se realiza entre el ultimo dia del mes y los 8 dias santeriores.
      start_date = execution_date - timedelta(days=10)
      end_date = execution_date 
      days_between = (end_date - start_date).days
      payment_date = start_date - timedelta(days=random.randint(0, days_between))
      data.append({
                  'card_id': card_id,
                  'payment_amount': payment_amount,
                  'payment_date': payment_date.strftime('%Y-%m-%d')
          })
  # Se almacenan los datos del diccionario en un dataframe
  df_monthly_payments = pd.DataFrame(data)
  df_monthly_payments['payment_date'] = pd.to_datetime(df_monthly_payments['payment_date'])
  output_file = os.path.join(
        base_path, 
        'dags', 
        'monthly_tasks', 
        'monthly_payments_csv', 
        f"monthly_payment_{year}_{month}.csv"
    )
  return df_monthly_payments.to_csv(output_file, index=False)


