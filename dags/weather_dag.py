from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine

# Thay API_KEY của bạn vào đây (Lấy từ OpenWeatherMap)
API_KEY = "YOUR_API_KEY_HERE"
CITY = "Hanoi"
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/weather_db"

default_args = {
    'owner': 'tran_mai_duong',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        weather_info = {
            'city': data['name'],
            'temp': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'description': data['weather'][0]['description'],
            'collected_at': datetime.now()
        }
        return weather_info
    else:
        raise Exception(f"API Error: {response.status_code}")

def load_to_db(ti):
    data = ti.xcom_pull(task_ids='extract_weather_task')
    df = pd.DataFrame([data])
    
    engine = create_engine(DB_CONN)
    
    df.to_sql('fact_weather', engine, if_exists='append', index=False)
    print("--- LOAD DỮ LIỆU THÀNH CÔNG ---")

with DAG(
    'weather_automated_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='extract_weather_task',
        python_callable=extract_weather
    )

    task2 = PythonOperator(
        task_id='load_to_db_task',
        python_callable=load_to_db
    )

    task1 >> task2