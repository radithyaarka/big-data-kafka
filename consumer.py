from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType

# Membuat sesi Spark dengan konektor Kafka
spark = SparkSession.builder \
    .appName("SensorDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Mengatur level log Spark ke ERROR agar hanya menampilkan tabel output
spark.sparkContext.setLogLevel("ERROR")

# Mengonfigurasi Kafka stream
sensor_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .load()

# Mendefinisikan skema data suhu (sesuai dengan data yang dikirim dari producer.py)
schema = StructType() \
    .add("sensor", StringType()) \
    .add("temp", IntegerType())

# Mengonversi value dari Kafka (format biner) ke JSON
sensor_df = sensor_data \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.sensor", "data.temp")

# Membuat kolom baru dengan satuan "°C"
sensor_df_with_unit = sensor_df.withColumn("temp", concat(col("temp"), lit("°C")))

# Filter suhu > 80°C
alert_df = sensor_df_with_unit.filter(col("temp").substr(1, 2).cast("int") > 80)

# Menampilkan peringatan dengan suhu dalam "°C"
query = alert_df \
    .select("sensor", "temp") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
