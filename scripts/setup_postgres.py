import psycopg2
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verify_database_connection():
    """Verify database connection and schema"""
    params = {
        'dbname': 'pipeline_db',
        'user': 'dataeng',
        'password': 'password123',
        'host': '127.0.0.1',
        'port': '5432'
    }
    
    logger.info("Attempting to connect to PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**params)
        logger.info("Successfully connected to PostgreSQL")
        
        with conn.cursor() as cur:
            # Create the table
            cur.execute("""
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
                )
            """)
            
            # Create the index
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_sensor_metrics_window 
                ON sensor_metrics(window_start, sensor_id)
            """)
            
            conn.commit()
            logger.info("Successfully created sensor_metrics table and index")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

if __name__ == "__main__":
    verify_database_connection()