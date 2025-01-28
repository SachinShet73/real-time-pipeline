from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from pyspark.sql import SparkSession
import time
import sys
sys.path.append(".")
from config.config import KafkaConfig, PostgresConfig, SparkConfig

def test_kafka():
    print("Testing Kafka connection...")
    config = KafkaConfig()
    try:
        producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers)
        producer.send(config.topic_name, b"test message")
        print("✅ Successfully connected to Kafka")
        producer.close()
    except KafkaError as e:
        print(f"❌ Failed to connect to Kafka: {e}")

def test_postgres():
    print("Testing PostgreSQL connection...")
    config = PostgresConfig()
    try:
        conn = psycopg2.connect(
            dbname=config.database,
            user=config.user,
            password=config.password,
            host=config.host,
            port=config.port
        )
        print("✅ Successfully connected to PostgreSQL")
        conn.close()
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")

def test_spark():
    print("Testing Spark connection...")
    config = SparkConfig()
    try:
        spark = SparkSession.builder \
            .appName("TestConnection") \
            .master(config.master) \
            .getOrCreate()
        print("✅ Successfully created Spark session")
        spark.stop()
    except Exception as e:
        print(f"❌ Failed to create Spark session: {e}")

if __name__ == "__main__":
    # Wait for services to be ready
    print("Waiting for services to be ready...")
    time.sleep(15)
    
    test_kafka()
    test_postgres()
    test_spark()