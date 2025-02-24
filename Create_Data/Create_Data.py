from dags.daily_tasks.MOCK_names import business_names, male_names, female_names, last_names
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Funcion para crear clientes y tarjetas de credito asociadas.
def generate_credit_cards(total_clients, max_cards, start_date, max_limit, min_limit):

    data = []
    data_2 = []
    data_3 = []
    used_card_numbers = set()

    # Calculate number of clients needed (assuming average of 2 cards per client)
    # estimated_clients = total_cards // cards_per_client
    client_ids = list(range(1, total_clients))
    cards_assigned = 0
    client_index = 0
    
    for client in client_ids:
        
        client_gender = random.choice(['Male', 'Female'])
        if client_gender == 'Male':
          client_name = random.choice(male_names)
          client_last_name = random.choice(last_names)

        else:
          client_name = random.choice(female_names)
          
        client_last_name = random.choice(last_names)
        educational_level = random.choice(["Uneducated", "High-School", "College", "Graduate", "Post-Graduate", "Doctorate", "Unknown" ])
        income_category = random.choice(["Less than $40K", "$40K - $60K", "$60K - $80K", "$80K - $120K", "$120K +", "Unknown"])
        phone_number = str(random.randint(10**7, (10**8)-1))
        time_format = "%Y-%m-%d"
        date_on = datetime.strptime(start_date, time_format)
        date_end = date_end = datetime.now()
        days_between = (date_end - date_on).days
        date_on_contract = date_on + timedelta(days=random.randint(0, days_between))
        age = random.randint(18, 90)
        marital_status = random.choice(['Single', 'Married', 'Divorced', "Unknown"])
        email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
        email = client_name + "." + client_last_name + "@" + random.choice(email_domains)

        data_2.append({
                'first_name': client_name,
                'last_name': client_last_name,
                'email': email,
                'gender': client_gender,
                'educational_level': educational_level,
                'income_category': income_category,
                'phone number': phone_number,
                'date_on': date_on_contract.strftime('%Y-%m-%d'),
                'age': age,
                'marital_status': marital_status
            })
        num_cards = random.randint(1, max_cards)

        for num_cards in range(num_cards):
            # Generate unique card number
            while True:
                card_number = str(random.randint(10**13, (10**14)-1))
                if card_number not in used_card_numbers:
                    used_card_numbers.add(card_number)
                    break

            # Generate card type
            card_type = random.choice(['VISA', 'MASTERCARD'])

            # Generate card category (1-4)
            card_category = random.randint(0, 3)
            factor = max_limit/4

    

            lower_limit = min_limit + card_category * factor
            upper_limit = lower_limit + factor
            credit_limit = int(random.randint(lower_limit, upper_limit))

            
            # Generate emission date

            emission_date = date_on_contract + timedelta(days=random.randint(0, 365))

            # Generate expiry date
            
            expiry_date = emission_date + timedelta(days=1825)

            data.append({
                'client_id': client,
                'card_number': card_number,
                'card_type': card_type,
                'card_category': card_category,
                'date_emission': emission_date.strftime('%Y-%m-%d'),
                'date_expiry': expiry_date.strftime('%Y-%m-%d'),
                'credit_limit': credit_limit
            })

            
    total_dependants = total_clients*2
    for i in range(total_dependants):
        client_id = random.choice(client_ids)
        dependant_name = random.choice(male_names+female_names)
        dependant_last_name = random.choice(last_names)
        dependant_age = random.randint(18, 90)
        
        data_3.append({
                'client_id': client_id,
                'dependant_name': dependant_name,
                'dependant_last_name': dependant_last_name,
                'dependant_age': dependant_age
                
            })

        

    # Convert to DataFrame and sort
    df = pd.DataFrame(data)
    df["card_category"].replace(to_replace=[0, 1, 2, 3], value=["Blue", "Silver", "Gold", "Platinum"], inplace=True)
    df_2 = pd.DataFrame(data_2)
    df_3 = pd.DataFrame(data_3)
    df = df.sort_values('client_id')
    data_frames  = [df, df_2, df_3]
    

    return data_frames

# Función para generar un determinado número de consumos por mes durante todo un año al azar.
def generate_consumptions(df, total_consumptions, year1):
  data = []
  df["date_emission"] = pd.to_datetime(df["date_emission"])
  possible_cards = df[df["date_emission"] < datetime(year1, 1, 1)].index
  possible_cards = possible_cards.to_list()
  max_limit = df["credit_limit"]
  max_limit = max_limit.to_list()
  year_range_in_years = (datetime.now().year-year1)+1
  year_range_in_months = (((datetime.now().year-year1)+1)*12)-(12-datetime.now().month)
  consumptions_per_month = int(total_consumptions/year_range_in_months)
  
  total_month_count = 0
  year_ = year1
  for i in range(year_range_in_years):     
    for x in range(1, 13):
      month = x
      if total_month_count < year_range_in_months:
        
        for i in range(consumptions_per_month):

            card_id = random.choice(possible_cards)+1
            max_consumption = int((max_limit[card_id-1]) * 0.3)
            consumption_amount = random.randint(1, max_consumption)
            start_date = datetime(year_, month, 1)
            end_date = datetime(year_, month, 20)
            days_between = (end_date - start_date).days
            consumption_date = start_date + timedelta(days=random.randint(0, days_between))
            business_name = random.choice(business_names)
            data.append({
                          'card_id': card_id,
                          'consumption_amount': consumption_amount,
                          'consumption_date': consumption_date.strftime('%Y-%m-%d'),
                          'business_name': business_name
                      })
        total_month_count = total_month_count + 1
       
   
      else:
        break
      total_month_count = total_month_count
    year_ = year_ + 1
    
        
          
        # Convert to DataFrame
  df_consumptions = pd.DataFrame(data)
  df_consumptions['consumption_date'] = pd.to_datetime(df_consumptions['consumption_date'])




  return df_consumptions

# Función para generar pagos.
# Existen 2 tipos de clientes, los que pagan la deuda total de la tarjeta todos los meses (clientes debtless), y los que no pagan el total de la tarjeta todos los meses y arrastran deuda (debtful)
def generate_payments(df_consumptions, df, total_clients):
  data = []
  # Elegimos tarjetas de credito cuyos dueños la pagan todos los meses sin arrastrar deuda. Seteamos el seed en 2, para que esos clientes sean siempre los mismos.
  random.seed(2)
  # El 20% de la cartera de clientes seran clientes que no arrastran deuda.
  clients_debtless = random.sample(range(1,total_clients), k=int(total_clients*0.2))
  cards_debtless = []
  for client in clients_debtless:
    card = df[df["client_id"]==client].index.to_list()
    cards_debtless.append(card)
  
  cards_debtless = [element for sublist in cards_debtless for element in sublist]
  cards_debtless_df = pd.DataFrame(cards_debtless)
  cards_debtless_df.to_csv("dags/monthly_tasks/cards_debtless.csv", index=False)

  random.seed()
  
  consumption_years = list(df_consumptions["consumption_date"].dt.year.unique())
  for y in consumption_years:
    df_consumptions_filtered_by_year = df_consumptions[df_consumptions["consumption_date"].dt.year == y]
    consumption_month = list(df_consumptions_filtered_by_year["consumption_date"].dt.month.unique())
    for m in consumption_month:

      month = m
      df_consumptions_filtered_by_month = df_consumptions_filtered_by_year[(df_consumptions_filtered_by_year["consumption_date"].dt.month == month)]
      cards_with_consumptions = df_consumptions_filtered_by_month["card_id"].unique().tolist()


      for card in cards_with_consumptions:
        if card in cards_debtless:
          card_id = card
          # El pago de estas tarjetas es igual a la suma de los consumos que hayan realizado en el mes (deuda 0).
          payment_amount = df_consumptions_filtered_by_month[df_consumptions_filtered_by_month["card_id"]==card].groupby(df_consumptions_filtered_by_month["card_id"])["consumption_amount"].sum().values[0]
          # el pago se realiza entre el día 20 y el 28 de cada mes
          start_date = datetime(y, month, 20)
          end_date = datetime(y, month, 28)
          days_between = (end_date - start_date).days
          payment_date = start_date + timedelta(days=random.randint(0, days_between))
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
          payment_amount = (df_consumptions_filtered_by_month[df_consumptions_filtered_by_month["card_id"]==card].groupby(df_consumptions_filtered_by_month["card_id"])["consumption_amount"].sum().values[0])*random_decimal
          payment_amount = int(payment_amount)
          # el pago se realiza entre el día 20 y el 28 de cada mes
          start_date = datetime(y, month, 20)
          end_date = datetime(y, month, 28)
          days_between = (end_date - start_date).days
          payment_date = start_date + timedelta(days=random.randint(0, days_between))
          data.append({

                      'card_id': card_id,
                      'payment_amount': payment_amount,
                      'payment_date': payment_date.strftime('%Y-%m-%d')

              })
    
  # Se almacenan los datos del diccionario en un dataframe
  df_payments = pd.DataFrame(data)
  df_payments['payment_date'] = pd.to_datetime(df_payments['payment_date'])
  
  return df_payments



 #Creo una función que permite simular una serie de consumos y una serie de pagos en un determinado mes.
def generate_consumptions_and_payments(df, total_consumptions, year1, total_clients):
   df_consumptions = generate_consumptions(df, total_consumptions, year1)
  
   df_payments = generate_payments(df_consumptions, df, total_clients)
   return [df_consumptions, df_payments]


def menu_generate_consumptions_and_payments():

  
  total_clients = 500
  max_cards = 6
  year_start = datetime.now() - timedelta(days=2000)
  year_start = year_start.year
  
  start_date = str(year_start)+"-01-01"
  min_limit = 100
  max_limit = 5000
  total_consumptions = 5000
  year1 = datetime.now() - timedelta(days=730)
  year1 = year1.year
  
  df_cards_clients = generate_credit_cards(total_clients, max_cards, start_date, max_limit, min_limit)
  df_consumption_and_payments = generate_consumptions_and_payments(df_cards_clients[0], total_consumptions, year1, total_clients)
  
  
  return df_consumption_and_payments[0].to_csv("CSV_Files/MOCK_DATA_CONSUMPTIONS.csv", index=False), df_consumption_and_payments[1].to_csv("CSV_Files/MOCK_DATA_PAYMENTS.csv", index=False), df_cards_clients[0].to_csv("CSV_Files/MOCK_DATA_CARDS.csv", index=False), df_cards_clients[1].to_csv("CSV_Files/MOCK_DATA_CLIENTS.csv", index=False), df_cards_clients[2].to_csv("CSV_Files/MOCK_DATA_DEPENDANTS.csv", index=False)