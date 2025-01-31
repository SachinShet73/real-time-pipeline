from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging
import sys
sys.path.append(".")
from config.config import KafkaConfig, PostgresConfig, SparkConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(spark_config: SparkConfig):
    """Create Spark session with necessary configurations"""
    return (SparkSession.builder
            .appName(spark_config.app_name)
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0")
            .config("spark.sql.streaming.checkpointLocation", spark_config.checkpoint_location)
            .getOrCreate())

def define_schema():
    """Define schema for sensor data"""
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

def process_stream(spark, kafka_config: KafkaConfig, postgres_config: PostgresConfig):
    """Process streaming data from Kafka"""
    
    # Read from Kafka
    stream_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
                .option("subscribe", kafka_config.topic_name)
                .option("startingOffsets", "latest")
                .load())

    # Parse JSON data
    schema = define_schema()
    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert timestamp string to timestamp type
    parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Calculate metrics with 5-minute windows
    metrics_df = (parsed_df
                 .withWatermark("timestamp", "5 minutes")
                 .groupBy(
                     window("timestamp", "5 minutes"),
                     "sensor_id"
                 )
                 .agg(
                     avg("temperature").alias("avg_temperature"),
                     max("temperature").alias("max_temperature"),
                     min("temperature").alias("min_temperature"),
                     avg("humidity").alias("avg_humidity"),
                     avg("pressure").alias("avg_pressure"),
                     count("*").alias("reading_count")
                 ))

    # Write to PostgreSQL
    query = (metrics_df.writeStream
            .outputMode("append")
            .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, postgres_config))
            .start())

    return query

def write_to_postgres(df, epoch_id, postgres_config: PostgresConfig):
    """Write batch to PostgreSQL"""
    if df.isEmpty():
        return
    
    # Flatten window column and prepare for writing
    flattened_df = (df
                   .select(
                       col("window.start").alias("window_start"),
                       col("window.end").alias("window_end"),
                       "sensor_id",
                       "avg_temperature",
                       "max_temperature",
                       "min_temperature",
                       "avg_humidity",
                       "avg_pressure",
                       "reading_count"
                   ))
    
    # Write to PostgreSQL
    (flattened_df.write
     .format("jdbc")
     .option("url", f"jdbc:postgresql://{postgres_config.host}:{postgres_config.port}/{postgres_config.database}")
     .option("driver", "org.postgresql.Driver")
     .option("dbtable", "sensor_metrics")
     .option("user", postgres_config.user)
     .option("password", postgres_config.password)
     .mode("append")
     .save())

def main():
    """Main function to start the streaming process"""
    # Load configurations
    kafka_config = KafkaConfig()
    postgres_config = PostgresConfig()
    spark_config = SparkConfig()
    
    logger.info("Starting Spark Streaming job...")
    
    # Create Spark session
    spark = create_spark_session(spark_config)
    
    try:
        # Process stream
        query = process_stream(spark, kafka_config, postgres_config)
        
        # Wait for termination
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in streaming job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()