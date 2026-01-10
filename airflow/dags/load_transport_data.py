from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

dag = DAG('load_transport_data', default_args=default_args, schedule_interval=None, catchup=False)

# Создание таблиц (связи через globalid)
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS main_id (
        globalid BIGINT PRIMARY KEY,
        name TEXT
    );
    CREATE TABLE IF NOT EXISTS transport_nodes (
        globalid BIGINT PRIMARY KEY,
        name TEXT,
        type TEXT,
        lon DECIMAL(10,8),
        lat DECIMAL(10,8)
    );
    CREATE TABLE IF NOT EXISTS difficult_node (
        tpuname TEXT,
        admarea TEXT,
        district TEXT,
        nearstation TEXT,
        holder TEXT,
        globalid BIGINT REFERENCES main_id(globalid),
        lon DECIMAL(10,8),
        lat DECIMAL(10,8)
    );
    CREATE TABLE IF NOT EXISTS pass_mes (
        id BIGINT PRIMARY KEY,
        year INT,
        month INT,
        transporttype TEXT,
        passengertraffic BIGINT,
        globalid BIGINT REFERENCES main_id(globalid),
        monthnum INT,
        data DATE
    );
    CREATE TABLE IF NOT EXISTS zapravki (
        id BIGINT PRIMARY KEY,
        globalid BIGINT REFERENCES main_id(globalid),
        name TEXT,
        shortname TEXT,
        admarea TEXT,
        district TEXT,
        address TEXT,
        owner TEXT,
        testdate DATE,
        lon DECIMAL(10,8),
        lat DECIMAL(10,8)
    );

    CREATE TABLE IF NOT EXISTS nerovnosti (
        name TEXT,
        globalid BIGINT REFERENCES main_id(globalid),
        admarea TEXT,
        district TEXT,
        location TEXT,
        lon DECIMAL(10,8),
        lat DECIMAL(10,8),
        material TEXT
    );
    
    CREATE TABLE IF NOT EXISTS zariad (
        globalid BIGINT REFERENCES main_id(globalid),
        name TEXT,
        balanceholder TEXT,
        admarea TEXT,
        district TEXT,
        address TEXT,
        lon DECIMAL(10,8),
        lat DECIMAL(10,8)
    );
    
    CREATE TABLE IF NOT EXISTS parking (
        id BIGINT PRIMARY KEY,
        name TEXT,
        placeid TEXT,
        admarea TEXT,
        district TEXT,
        address TEXT,
        countspaces INT,
        globalid BIGINT REFERENCES main_id(globalid),
        lon DECIMAL(10,8),
        lat DECIMAL(10,8),
        type_park TEXT
    );
    
    
    """,
    dag=dag
)

# Загрузка CSV через COPY (быстрее INSERT)
load_csvs = BashOperator(
    task_id='load_csvs',
    bash_command="""
    psql -U airflow -d airflow -h postgres -c "
    COPY main_id FROM '/opt/airflow/data/main_id.csv' WITH CSV HEADER;
    COPY transport_nodes FROM '/opt/airflow/data/tranport_nodes.csv' WITH CSV HEADER;
    COPY difficult_node FROM '/opt/airflow/data/difficult_node.csv' WITH CSV HEADER;
    COPY pass_mes FROM '/opt/airflow/data/pass_mes.csv' WITH CSV HEADER;
    COPY zapravki FROM '/opt/airflow/data/zapravki.csv' WITH CSV HEADER;
    COPY nerovnosti FROM '/opt/airflow/data/nerovnosti.csv' WITH CSV HEADER;
    COPY zariad FROM '/opt/airflow/data/zariad.csv' WITH CSV HEADER;
    COPY parking FROM '/opt/airflow/data/parking.csv' WITH CSV HEADER;

    "
    """,
    dag=dag
)

create_tables >> load_csvs