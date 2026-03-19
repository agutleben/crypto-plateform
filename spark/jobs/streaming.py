import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType, BooleanType, TimestampType
)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_RAW         = "crypto.raw.trades"
KAFKA_TOPIC_METRICS     = "crypto.metrics"

# ── Schéma du message Kafka ───────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("symbol",                StringType(),  True),
    StructField("price",                 FloatType(),   True),
    StructField("quantity",              FloatType(),   True),
    StructField("trade_time",            LongType(),    True),
    StructField("event_time",            LongType(),    True),
    StructField("is_buyer_market_maker", BooleanType(), True),
    StructField("ingested_at",           StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoStreamingMetrics")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("credentialsFile", "/tmp/gcp-credentials.json")
        .config("parentProject", "crypto-platform-dev-490610")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/tmp/gcp-credentials.json")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_trades(raw_df):
    """Désérialise le JSON Kafka et ajoute une colonne timestamp propre."""
    return (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_ts",
            (F.col("event_time") / 1000).cast(TimestampType())
        )
    )


def compute_metrics(trades_df):
    """
    Fenêtre glissante de 1 minute, slide de 30 secondes.
    Calcule par symbole :
      - VWAP  (Volume Weighted Average Price)
      - volume total
      - nb de trades
      - prix min / max
      - variation % par rapport au prix d'ouverture
    """
    return (
        trades_df
        .withWatermark("event_ts", "30 seconds")
        .groupBy(
            F.window("event_ts", "1 minute", "30 seconds"),
            F.col("symbol")
        )
        .agg(
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity"))
                .alias("vwap"),
            F.sum("quantity")  .alias("volume"),
            F.count("*")       .alias("trade_count"),
            F.min("price")     .alias("price_min"),
            F.max("price")     .alias("price_max"),
            F.first("price")   .alias("price_open"),
            F.last("price")    .alias("price_close"),
        )
        .withColumn(
            "price_change_pct",
            F.round(
                (F.col("price_close") - F.col("price_open"))
                / F.col("price_open") * 100,
                4
            )
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )
def write_to_bigquery(df, epoch_id, table, gcp_project, gcs_bucket):
    """Écrit un micro-batch dans BigQuery via foreachBatch."""
    if df.isEmpty():
        return
    (
        df.write
        .format("bigquery")
        .option("table", table)
        .option("temporaryGcsBucket", gcs_bucket)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_APPEND")
        .option("credentialsFile", "/tmp/gcp-credentials.json")
        .option("parentProject", gcp_project)
        .mode("append")
        .save()
    )
    print(f">>> Batch {epoch_id} écrit dans {table}")

def serialize_to_kafka(metrics_df):
    """Sérialise les métriques en JSON pour le topic Kafka metrics."""
    return (
        metrics_df
        .select(
            F.col("symbol").alias("key"),
            F.to_json(F.struct("*")).alias("value")
        )
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    GCP_PROJECT    = os.getenv("GCP_PROJECT",    "crypto-platform-dev-490610")
    BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "crypto_raw")
    GCS_BUCKET     = os.getenv("GCS_BUCKET",     "crypto-platform-temp-490610")

    print(f">>> GCP_PROJECT    = {GCP_PROJECT}")
    print(f">>> BQ_DATASET_RAW = {BQ_DATASET_RAW}")
    print(f">>> GCS_BUCKET     = {GCS_BUCKET}")
    print(">>> Lecture du stream Kafka...")

    raw_df     = read_kafka_stream(spark)
    trades_df  = parse_trades(raw_df)
    metrics_df = compute_metrics(trades_df)

    # ── Sink 1 : Console (debug) ──────────────────────────────────
    console_query = (
        metrics_df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="15 seconds")
        .start()
    )

    # ── Sink 2 : Kafka topic metrics ──────────────────────────────
    kafka_query = (
        serialize_to_kafka(metrics_df)
        .writeStream
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_METRICS)
        .option("checkpointLocation", "/tmp/spark-checkpoint-kafka")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # ── Sink 3 : BigQuery RAW trades ─────────────────────────────
    bq_raw_query = (
        trades_df.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, epoch_id: write_to_bigquery(
            df, epoch_id,
            f"{GCP_PROJECT}.{BQ_DATASET_RAW}.trades",
            GCP_PROJECT, GCS_BUCKET
        ))
        .option("checkpointLocation", "/tmp/spark-checkpoint-raw")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Sink 4 : BigQuery métriques ───────────────────────────────
    bq_metrics_query = (
        metrics_df.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, epoch_id: write_to_bigquery(
            df, epoch_id,
            f"{GCP_PROJECT}.{BQ_DATASET_RAW}.metrics",
            GCP_PROJECT, GCS_BUCKET
        ))
        .option("checkpointLocation", "/tmp/spark-checkpoint-bq-metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )
    print(f">>> Sink metrics démarré : {bq_metrics_query.status}")

    print(">>> Tous les sinks démarrés, attente des données...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()