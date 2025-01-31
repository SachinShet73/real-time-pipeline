-- Allow trust authentication for local connections
ALTER SYSTEM SET password_encryption TO 'md5';
ALTER SYSTEM SET listen_addresses TO '*';

-- Update authentication method for the user
ALTER USER dataeng WITH PASSWORD 'password123';

-- Create initial schema
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

-- Create index
CREATE INDEX IF NOT EXISTS idx_sensor_metrics_window 
ON sensor_metrics(window_start, sensor_id);