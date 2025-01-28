from dataclasses import dataclass
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name: str = os.getenv("KAFKA_TOPIC", "events")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "event_processor")

@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "pipeline_db")
    user: str = os.getenv("POSTGRES_USER", "dataeng")
    password: str = os.getenv("POSTGRES_PASSWORD", "password123")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class SparkConfig:
    master: str = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    app_name: str = os.getenv("SPARK_APP_NAME", "RealTimeProcessor")
    checkpoint_location: str = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/checkpoint")