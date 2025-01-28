import json
from confluent_kafka import Consumer
import logging
import sys
sys.path.append(".")
from config.config import KafkaConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def start_consumer(kafka_config: KafkaConfig):
    """Start a simple Kafka consumer for testing"""
    consumer = Consumer({
        'bootstrap.servers': kafka_config.bootstrap_servers,
        'group.id': kafka_config.consumer_group,
        'auto.offset.reset': 'latest'
    })
    
    # Subscribe to topic
    consumer.subscribe([kafka_config.topic_name])
    logger.info(f"Started consuming from topic: {kafka_config.topic_name}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message: {value}")
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed successfully")

if __name__ == "__main__":
    config = KafkaConfig()
    start_consumer(config)