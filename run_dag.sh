#!/bin/bash

# Переходим в папку с docker-compose
cd /workspaces/dwh_transport/airflow

echo "Unpausing DAG load_transport_data..."
docker compose exec airflow-webserver airflow dags unpause load_transport_data

echo "Triggering DAG..."
docker compose exec airflow-webserver airflow dags trigger load_transport_data

echo "Checking status..."
docker compose exec airflow-webserver airflow dags list-runs -d load_transport_data