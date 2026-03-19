FROM apache/spark:3.5.3

USER root

RUN mkdir -p /home/spark/.ivy2/cache /root/.ivy2/cache && \
    chmod -R 777 /home/spark/.ivy2 /root/.ivy2

RUN curl -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.38.0.jar \
    https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.38.0/spark-bigquery-with-dependencies_2.12-0.38.0.jar && \
    curl -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

USER spark