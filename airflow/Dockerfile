FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y git && apt-get clean
RUN mkdir -p /opt/airflow/dbt && \
    chown -R 50000:0 /opt/airflow/dbt && \
    chmod -R 775 /opt/airflow/dbt

COPY init.sh /init.sh
RUN chmod +x /init.sh

USER airflow
RUN pip install --no-cache-dir \
    dbt-bigquery==1.8.0 \
    google-cloud-bigquery==3.25.0