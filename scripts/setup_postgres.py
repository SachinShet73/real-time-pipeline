import psycopg2
import logging
import sys
sys.path.append(".")
from config.config import PostgresConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_database(config: PostgresConfig):
    """Create necessary tables in PostgreSQL"""
    # SQL to create the sensor metrics table 
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sensor_metrics (
        id SERIAL PRIMARY KEY,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        sensor_id VARCHAR(50) NOT NULL,
        avg_temperature DOUBLE PRECISION,
        max_temperature DOUBLE PRECISION,
        min_temperature DOUBLE PRECISION,
        avg_humidity DOUBLE PRECISION,
        avg_pressure DOUBLE PRECISION,
        reading_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create index on window_start and sensor_id for better query performance
    CREATE INDEX IF NOT EXISTS idx_sensor_metrics_window 
    ON sensor_metrics(window_start, sensor_id);
    """
    
    conn = None
    try:
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(
            dbname=config.database,
            user=config.user,
            password=config.password,
            host=config.host,
            port=config.port
        )
        
        # Create table
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
        
        conn.commit()
        logger.info("Successfully created sensor_metrics table")
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    config = PostgresConfig()
    setup_database(config)