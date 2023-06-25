from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col

schema = StructType([
                StructField("full_name",StringType(),False),
                StructField("gender",StringType(),False),
                StructField("location",StringType(),False),
                StructField("city",StringType(),False),
                StructField("country",StringType(),False),
                StructField("postcode",IntegerType(),False),
                StructField("latitude",FloatType(),False),
                StructField("longitude",FloatType(),False),
                StructField("email",FloatType(),False)
            ])

spark = SparkSession \
    .builder \
    .appName("SparkStructuredStreaming") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()
  
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
  .option("subscribe", "random_names") \
  .option("delimeter",",") \
  .option("startingOffsets", "earliest") \
  .load()

df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schema).alias("data")).select("data.*")


def write_to_cassandra(writeDF, _):
  writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="random_names", keyspace="spark_streaming")\
    .save()

df1.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .start()\
    .awaitTermination()

my_query = (df1.writeStream
                  .format("org.apache.spark.sql.cassandra")
                  .outputMode("append")
                  .options(table="random_names", keyspace="spark_streaming")\
                  .start())

my_query.awaitTermination()
