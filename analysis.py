import sys
import os
import time  
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, count, current_timestamp, lit


os.environ['SPARK_PYTHON_USE_DAEMON'] = 'false'
os.environ['SPARK_WORKER_REUSE'] = 'false'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

print(f"Spark başlatılıyor... Python Yolu: {sys.executable}")


DB_URL = "jdbc:postgresql://localhost:5433/smartcity_db"
DB_USER = "db_username"
DB_PASSWORD = "db_sifreniz"
TABLE_NAME = "traffic_stats"


spark = SparkSession.builder \
    .appName("TrafficBatchAnalysisLoop") \
    .master("local[1]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print(">>> Sonsuz döngü başlatılıyor. Durdurmak için CTRL+C yapabilirsin.")
    
   
    while True:
        print("\n--- Yeni Analiz Döngüsü Başlıyor ---")
        
        print("1. Data Lake'ten (MinIO) veriler okunuyor...")
      
        df = spark.read.parquet("s3a://traffic-data/raw/")
        
        if df.rdd.isEmpty():
            print("UYARI: MinIO'da veri var ama içi boş.")
        else:
         

            if "city" not in df.columns:
                df = df.withColumn("city", lit("Istanbul"))

         
            stats_df = df.groupBy("city") \
                .agg(
                    count("*").alias("total_vehicles"),
                    avg("avg_speed").alias("avg_speed")
                ) \
                .withColumn("window_start", current_timestamp()) \
                .withColumn("window_end", current_timestamp()) \
                .withColumn("created_at", current_timestamp())

            final_df = stats_df.select("window_start", "window_end", "city", "total_vehicles", "avg_speed", "created_at")

            print("2. Analiz sonuçları hesaplandı.")
            final_df.show(truncate=False)

            print("3. PostgreSQL veritabanına yazılıyor.")
            
          
            final_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", TABLE_NAME) \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
                
            print(">>> BAŞARILI: Veriler eklendi!")

        print("Bir sonraki tur için 10 saniye bekleniyor.")
        time.sleep(10) # 10 Saniye bekle

except KeyboardInterrupt:
    print("\n[BİLGİ] Kullanıcı tarafından durduruldu (CTRL+C).")

except Exception as e:
    print(f"\n[HATA]: {e}")

finally:
    if 'spark' in locals():
        spark.stop()
        print("Spark durduruldu.")