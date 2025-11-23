import sys
import os
import subprocess


os.environ['JAVA_HOME'] = "C:\\jdk11"
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['HADOOP_HOME'] + "\\bin;" + os.environ['PATH']

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_PYTHON_USE_DAEMON'] = 'false'
os.environ['SPARK_WORKER_REUSE'] = 'false'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

print("-" * 40)
print("Consumer Başlatılıyor... (MinIO Yazma Modu)")
print("-" * 40)


spark = SparkSession.builder \
    .appName("TrafficConsumer") \
    .master("local[1]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("city", StringType()),
    StructField("lat", FloatType()),
    StructField("lon", FloatType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed", FloatType()),
    StructField("traffic_intensity", StringType()),
    StructField("timestamp", FloatType())
])

print("Kafka'ya bağlanılıyor")

try:

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "traffic-data") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    final_df = parsed_df.withColumn("processed_at", current_timestamp())

    print("Veri MinIO'ya yazılıyor (Lütfen 1-2 dakika bekleyin veri biriksin)")


    query = final_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "s3a://traffic-data/raw/") \
        .option("checkpointLocation", "s3a://traffic-data/checkpoint/") \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f"\n[HATA]: {e}")