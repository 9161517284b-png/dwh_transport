#!/bin/bash

cd /workspaces/dwh_transport/airflow
echo "Listing all DAGs:"
docker compose exec airflow-webserver airflow dags list

echo -e "\nChecking if load_transport_data exists:"
docker compose exec airflow-webserver airflow dags list | grep -i transport