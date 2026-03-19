#!/bin/bash
set -e

echo ">>> Init Airflow DB..."
airflow db init

echo ">>> Création user admin..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@crypto.com || true

echo ">>> Copie fichiers dbt..."
cp -r /opt/dbt-source/. /opt/airflow/dbt/

echo ">>> Init terminée !"