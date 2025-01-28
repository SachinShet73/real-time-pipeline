from confluent_kafka.admin import AdminClient, NewTopic
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

def setup_kafka_topic(config: KafkaConfig):
    """Create Kafka topic if it doesn't exist"""
    admin_client = AdminClient({
        'bootstrap.servers': config.bootstrap_servers
    })
    
    # Check if topic exists
    topics = admin_client.list_topics().topics
    if config.topic_name in topics:
        logger.info(f"Topic {config.topic_name} already exists")
        return
    
    # Create topic
    new_topic = NewTopic(
        config.topic_name,
        num_partitions=1,
        replication_factor=1
    )
    
    try:
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            future.result()  # Blocks until topic creation is complete
        logger.info(f"Created topic: {config.topic_name}")
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise

if __name__ == "__main__":
    config = KafkaConfig()
    setup_kafka_topic(config)