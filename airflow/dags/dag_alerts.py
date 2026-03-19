from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import os

GCP_PROJECT    = os.getenv("GCP_PROJECT",    "crypto-platform-dev-490610")
BQ_DATASET_MART= os.getenv("BQ_DATASET_MART","crypto_mart")

default_args = {
    "owner":            "crypto-platform",
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}


def check_spikes(**context):
    """Détecte les variations > 0.5% sur les 15 dernières minutes."""
    client = bigquery.Client(project=GCP_PROJECT)

    query = f"""
        SELECT
            symbol,
            price_change_pct,
            vwap,
            window_end
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
        WHERE ABS(price_change_pct) > 0.5
        ORDER BY ABS(price_change_pct) DESC
    """

    results = client.query(query).result()
    alerts = []

    for row in results:
        alert = {
            "symbol":           row.symbol,
            "price_change_pct": row.price_change_pct,
            "vwap":             row.vwap,
            "window_end":       str(row.window_end),
        }
        alerts.append(alert)
        print(f"🚨 SPIKE DETECTED: {row.symbol} {row.price_change_pct:+.4f}% @ {row.vwap:.4f}")

    context["task_instance"].xcom_push(key="alerts", value=alerts)
    return f"{len(alerts)} alertes détectées"


def log_alerts(**context):
    """Log les alertes — extensible vers Slack/email."""
    alerts = context["task_instance"].xcom_pull(
        task_ids="check_spikes",
        key="alerts"
    )
    if not alerts:
        print("✅ Aucune variation significative détectée")
        return

    print(f"📊 Rapport d'alertes — {len(alerts)} symboles en mouvement :")
    for alert in alerts:
        direction = "📈" if alert["price_change_pct"] > 0 else "📉"
        print(
            f"{direction} {alert['symbol']:10s} | "
            f"{alert['price_change_pct']:+.4f}% | "
            f"VWAP: {alert['vwap']:.4f}"
        )


with DAG(
    dag_id="crypto_alerts",
    description="Détecte les spikes de prix toutes les 5 minutes",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["alerts", "crypto", "monitoring"],
) as dag:

    check = PythonOperator(
        task_id="check_spikes",
        python_callable=check_spikes,
        provide_context=True,
    )

    log = PythonOperator(
        task_id="log_alerts",
        python_callable=log_alerts,
        provide_context=True,
    )

    check >> log