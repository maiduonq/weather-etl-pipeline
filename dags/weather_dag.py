from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine

# --- CẤU HÌNH (THAY API KEY CỦA BẠN VÀO ĐÂY) ---
API_KEY = "a5b4710b0ddc9ff59ae7188a2ef44b15" 
CITY = "Hanoi"
# Kết nối đến Postgres bên trong Docker
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/weather_db"

default_args = {
    'owner': 'tran_mai_duong',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_weather():
    """Trích xuất dữ liệu từ API"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'city_name': data['name'],
            'country': data['sys']['country'],
            'temp': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'description': data['weather'][0]['description'],
            'collected_at': datetime.now()
        }
    else:
        raise Exception(f"API Error: {response.status_code}")

def load_to_star_schema(ti):
    """Nạp dữ liệu vào mô hình Star Schema (Fact & Dimension)"""
    # 1. Lấy dữ liệu từ bước Extract
    raw_data = ti.xcom_pull(task_ids='extract_weather_task')
    engine = create_engine(DB_CONN)
    
    # --- XỬ LÝ BẢNG DIMENSION (dim_city) ---
    # Lưu thông tin tĩnh về thành phố
    city_id = 1 # Định danh cho Hanoi
    dim_city_df = pd.DataFrame([{
        'city_id': city_id,
        'city_name': raw_data['city_name'],
        'country': raw_data['country']
    }])
    # Dùng if_exists='replace' để đảm bảo thông tin dim luôn duy nhất
    dim_city_df.to_sql('dim_city', engine, if_exists='replace', index=False)
    
    # --- XỬ LÝ BẢNG FACT (fact_weather) ---
    # Lưu thông tin đo lường biến đổi theo thời gian
    fact_weather_df = pd.DataFrame([{
        'city_id': city_id, # Khóa ngoại liên kết sang dim_city
        'temp': raw_data['temp'],
        'humidity': raw_data['humidity'],
        'pressure': raw_data['pressure'],
        'description': raw_data['description'],
        'collected_at': raw_data['collected_at']
    }])
    # Dùng if_exists='append' để lưu lịch sử
    fact_weather_df.to_sql('fact_weather', engine, if_exists='append', index=False)
    
    print("--- SUCCESS: DATA LOADED INTO STAR SCHEMA (DIM_CITY & FACT_WEATHER) ---")

# --- ĐỊNH NGHĨA DAG ---
with DAG(
    'weather_automated_pipeline',
    default_args=default_args,
    description='ETL Weather data with Star Schema',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather_task',
        python_callable=extract_weather
    )

    load_task = PythonOperator(
        task_id='load_to_db_task',
        python_callable=load_to_star_schema
    )

    extract_task >> load_task