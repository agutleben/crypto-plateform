from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName('test-bq')     .master('local')     .config('credentialsFile', '/tmp/gcp-credentials.json')     .config('parentProject', 'crypto-platform-dev-490610')     .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

data = [('BTCUSDT', 74000.0, 1.5), ('ETHUSDT', 2300.0, 2.0)]
df = spark.createDataFrame(data, ['symbol', 'price', 'quantity'])

print('>>> Tentative ecriture BigQuery...')
try:
    df.write       .format('bigquery')       .option('table', 'crypto-platform-dev-490610.crypto_raw.test_connection')       .option('temporaryGcsBucket', 'crypto-platform-temp-490610')       .option('createDisposition', 'CREATE_IF_NEEDED')       .option('credentialsFile', '/tmp/gcp-credentials.json')       .option('parentProject', 'crypto-platform-dev-490610')       .mode('append')       .save()
    print('>>> SUCCES - Table creee !')
except Exception as e:
    print(f'>>> ERREUR : {e}')

spark.stop()
