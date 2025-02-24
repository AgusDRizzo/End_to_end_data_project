import logging
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime


def wait_for_db():
    max_retries = 10
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempting to connect to MySQL (attempt {attempt + 1}/{max_retries})...")
            engine = create_engine('mysql+pymysql://root:admin@db:3306/credit_card_db')
            conn = engine.connect()
            conn.close()
            logging.info("Connected to MySQL successfully!")
            return
        except Exception as e:
            logging.error(f"MySQL is not ready yet: {e}")
            time.sleep(retry_delay)
    raise Exception("MySQL did not become ready in time!")

def load_monthly_data():
    year = datetime.now().year
    month = datetime.now().month
    try:
        logging.info("Attempting to connect to MySQL...")
        engine = create_engine('mysql+pymysql://root:admin@db:3306/credit_card_db?connect_timeout=60')
        logging.info("Connected successfully to MySQL!")

        consumption_date = datetime.now()
        file_path = f"/opt/airflow/dags/monthly_tasks/monthly_payments_csv/monthly_payment_{year}_{month}.csv"
        logging.info(f"Reading data from {file_path}...")
        df = pd.read_csv(file_path)

        logging.info("Writing data to MySQL...")
        df.to_sql('payments', engine, if_exists='append', index=False)
        logging.info("Data written successfully!")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise
