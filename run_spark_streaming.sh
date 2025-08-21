#!/bin/bash

echo "🔥 Starting Spark Streaming Job..."
echo ""

# Method 1: Using Docker
echo "📦 Method 1: Using Docker Container"
echo "docker-compose up spark-streaming"
echo ""

# Method 2: Using existing Spark container
echo "📦 Method 2: Using existing Spark Master container"
echo "docker exec real-time-pipeline-spark-master-1 spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1 --conf spark.cassandra.connection.host=cassandra /opt/spark_stream.py"
echo ""

# Method 3: Manual data generation for testing
echo "📊 Method 3: Manual Data Generation (for testing)"
echo "You can manually add people to test the real-time dashboard:"
echo ""
echo "docker exec cassandra cqlsh -e \"INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture) VALUES (uuid(), 'Test', 'User', 'male', '123 Test St, Test City, TX, USA', '12345', 'test@example.com', 'testuser', '1990-01-01', '$(date -u +%Y-%m-%dT%H:%M:%SZ)', '+1-555-000-0000', 'https://randomuser.me/api/portraits/men/$(shuf -i 1-99 -n 1).jpg');\""
echo ""

echo "🎯 Recommended: Use Method 3 for immediate testing!"
echo "💡 The dashboard will automatically update when new records are added!"
echo ""
echo "📱 Access your dashboard at: http://localhost:3000"
echo "🔧 Access Airflow at: http://localhost:8080"
echo "📊 Access Confluent Control Center at: http://localhost:9021"