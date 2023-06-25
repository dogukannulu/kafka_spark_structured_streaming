import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType
from pyspark.sql.functions import from_json,col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
        # Spark session is established with cassandra and kafka jars. Suitable versions can be found in Maven repository.
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
        logging.info('Spark session created successfully')
    except Exception:
        logging.error("Couldn't create the spark session")

    return spark

def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        # Gets the streaming data from topic random_names
        df = spark_session \
              .readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
              .option("subscribe", "random_names") \
              .option("delimeter",",") \
              .option("startingOffsets", "earliest") \
              .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
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

    df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schema).alias("data")).select("data.*")

    return df

def start_streaming(df):
    """
    Starts the streaming to index office_input in elasticsearch.
    """
    logging.info("Streaming is being started...")
    my_query = (df.writeStream
                  .format("org.apache.spark.sql.cassandra")
                  .outputMode("append")
                  .options(table="random_names", keyspace="spark_streaming")\
                  .start())

    return my_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)

