from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

dag = DAG('load_transport_data_fixed', 
          default_args=default_args, 
          schedule_interval=None, 
          catchup=False)

# 1. Создание таблиц (оставьте ваш SQL как есть)
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql='''
    -- ваш SQL код создания таблиц
    ''',
    dag=dag
)

# 2. Функция для загрузки CSV
def load_csv_files(**context):
    """Загружает CSV файлы в PostgreSQL"""
    
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    # Путь к данным внутри контейнера Airflow
    data_dir = "/opt/airflow/data/"
    
    # Список файлов для загрузки
    files_tables = [
        ("main_id.csv", "main_id"),
        ("tranport_nodes.csv", "transport_nodes"),
        ("difficult_node.csv", "difficult_node"),
        ("pass_mes.csv", "pass_mes"),
        ("zapravki.csv", "zapravki"),
        ("nerovnosti.csv", "nerovnosti"),
        ("zariad.csv", "zariad"),
        ("parking.csv", "parking")
    ]
    
    for csv_file, table_name in files_tables:
        csv_path = os.path.join(data_dir, csv_file)
        
        if os.path.exists(csv_path):
            print(f"Загружаем {csv_file} в таблицу {table_name}...")
            
            try:
                # Открываем CSV файл
                with open(csv_path, 'r', encoding='utf-8') as f:
                    # Пропускаем заголовок
                    next(f)
                    cursor.copy_from(f, table_name, sep=',', null='')
                
                conn.commit()
                print(f"✓ {csv_file} успешно загружен")
                
            except Exception as e:
                conn.rollback()
                print(f"✗ Ошибка при загрузке {csv_file}: {e}")
        else:
            print(f"⚠ Файл {csv_file} не найден по пути {csv_path}")
    
    cursor.close()
    conn.close()

# 3. Задача загрузки данных
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_csv_files,
    dag=dag
)

# 4. Порядок выполнения
create_tables >> load_data