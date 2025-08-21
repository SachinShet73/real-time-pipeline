#!/bin/bash

echo "🚀 Starting Real-Time People Dashboard (Development Mode)"
echo "=================================================="

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down

# Build and start all services
echo "🔨 Building and starting services..."
docker-compose up --build -d

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 10

# Check service health
echo "🏥 Checking service health..."
docker-compose ps

echo ""
echo "✅ Application started successfully!"
echo ""
echo "📱 Frontend Dashboard: http://localhost:3000"
echo "🔧 Airflow UI: http://localhost:8080 (admin/admin)"
echo "📊 Confluent Control Center: http://localhost:9021"
echo "🔌 Backend API: http://localhost:5001"
echo ""
echo "💡 To view logs: docker-compose logs -f [service-name]"
echo "🛑 To stop: docker-compose down"