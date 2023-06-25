from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
  .option("subscribe", "rosmsgs") \
  .option("delimeter",",") \
  .option("startingOffsets", "earliest") \
  .load() 


spark.stop()