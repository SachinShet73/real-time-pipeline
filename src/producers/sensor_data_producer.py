import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import logging
from typing import Dict, Any
import sys
sys.path.append(".")
from config.config import KafkaConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self, kafka_config: KafkaConfig):
        """Initialize the Kafka producer with configuration"""
        self.config = kafka_config
        self.producer = Producer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'client.id': 'sensor_data_producer'
        })
        logger.info(f"Initialized Kafka producer with bootstrap servers: {self.config.bootstrap_servers}")

    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def generate_sensor_data(self) -> Dict[str, Any]:
        """Generate a single sensor reading"""
        return {
            "sensor_id": f"sensor_{random.randint(1, 5)}",
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 70), 2),
            "pressure": round(random.uniform(980, 1020), 2),
            "timestamp": datetime.now().isoformat()
        }

    def produce_data(self, interval_seconds: float = 1.0):
        """Continuously produce sensor data with a specified interval"""
        try:
            while True:
                data = self.generate_sensor_data()
                # Convert data to JSON string
                message = json.dumps(data)
                
                # Produce message
                self.producer.produce(
                    self.config.topic_name,
                    value=message.encode('utf-8'),
                    callback=self.delivery_report
                )
                
                # Flush producer queue to deliver messages
                self.producer.poll(0)
                
                logger.info(f"Produced sensor data: {data}")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Stopping data production...")
        except Exception as e:
            logger.error(f"Error producing data: {e}")
            raise
        finally:
            # Make sure all messages are sent before closing
            logger.info("Flushing producer...")
            self.producer.flush()
            logger.info("Producer closed successfully")

if __name__ == "__main__":
    config = KafkaConfig()
    producer = SensorDataProducer(config)
    
    logger.info(f"Starting to produce data to topic: {config.topic_name}")
    producer.produce_data(interval_seconds=2.0)  # Produce data every 2 seconds